from __future__ import annotations

import calendar
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Callable

import markdown
import polars as pl

from social_posts_analysis.analysis.cache import AnalysisCacheStore
from social_posts_analysis.analysis.clustering import NarrativeClusterer
from social_posts_analysis.analysis.providers import build_providers
from social_posts_analysis.collectors.telegram_mtproto import TelegramMtprotoCollector
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.normalization.persistence import persist_table, sync_duckdb
from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.pipeline import CollectionService
from social_posts_analysis.utils import make_run_id, read_json, utc_now_iso


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    window_id: str
    start: str
    end: str


CollectionServiceFactory = Callable[[ProjectConfig, ProjectPaths], Any]


def build_monthly_windows(start: str, end: str, *, max_windows: int) -> list[HistoryWindow]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date < start_date:
        raise ValueError("history.end must be greater than or equal to history.start.")
    if max_windows <= 0:
        raise ValueError("history.max_windows must be greater than zero.")

    windows: list[HistoryWindow] = []
    cursor = start_date
    while cursor <= end_date and len(windows) < max_windows:
        month_end_day = calendar.monthrange(cursor.year, cursor.month)[1]
        window_end = min(date(cursor.year, cursor.month, month_end_day), end_date)
        windows.append(
            HistoryWindow(
                window_id=f"{cursor.year:04d}{cursor.month:02d}",
                start=cursor.isoformat(),
                end=window_end.isoformat(),
            )
        )
        cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
    return windows


def discover_history_start(config: ProjectConfig) -> str:
    if config.source.platform == "telegram" and config.collector.mode == "mtproto":
        discovered = TelegramMtprotoCollector(config).oldest_source_datetime()
        if discovered:
            return discovered[:10]
    raise RuntimeError("history.start is required unless Telegram MTProto can discover the oldest source message.")


class HistoricalBackfillService:
    def __init__(
        self,
        config: ProjectConfig,
        paths: ProjectPaths,
        *,
        collection_service_factory: CollectionServiceFactory | None = None,
    ) -> None:
        self.config = config
        self.paths = paths
        self.collection_service_factory = collection_service_factory or (lambda cfg, pths: CollectionService(cfg, pths))

    def run(self, history_run_id: str | None = None) -> dict[str, Any]:
        resolved_history_run_id = history_run_id or f"history-{make_run_id()}"
        start = self.config.history.start or discover_history_start(self.config)
        end = self.config.history.end or datetime.now(tz=UTC).date().isoformat()
        windows = build_monthly_windows(start, end, max_windows=self.config.history.max_windows)
        parent_warnings = self._parent_warnings(start=start, end=end, windows=windows)

        window_rows: list[dict[str, Any]] = []
        for window in windows:
            child_run_id = f"{resolved_history_run_id}__{window.window_id}"
            try:
                if self.config.history.resume and self._child_run_ready(child_run_id):
                    row = self._summarize_window(
                        history_run_id=resolved_history_run_id,
                        window=window,
                        child_run_id=child_run_id,
                        status_override="skipped",
                    )
                else:
                    window_config = self.config.model_copy(deep=True)
                    window_config.date_range.start = window.start
                    window_config.date_range.end = window.end
                    window_config.history.active = True
                    collector = self.collection_service_factory(window_config, self.paths)
                    collector.run(run_id=child_run_id)
                    NormalizationService(window_config, self.paths).run(run_id=child_run_id, source_run_ids=[child_run_id])
                    row = self._summarize_window(
                        history_run_id=resolved_history_run_id,
                        window=window,
                        child_run_id=child_run_id,
                    )
                window_rows.append(row)
            except Exception as exc:
                window_rows.append(
                    self._empty_window_row(
                        history_run_id=resolved_history_run_id,
                        window=window,
                        child_run_id=child_run_id,
                        status="failed",
                        warnings=[str(exc)],
                    )
                )
                if self.config.history.stop_on_error:
                    break

        status = "success"
        if parent_warnings or any(row["status"] in {"failed", "partial"} for row in window_rows):
            status = "partial"
        child_run_ids = [row["child_run_id"] for row in window_rows if row["status"] != "failed"]
        parent_row = {
            "history_run_id": resolved_history_run_id,
            "created_at": utc_now_iso(),
            "platform": self.config.source.platform,
            "source_kind": self.config.source.kind,
            "source_id": self.config.source.source_id or "",
            "source_name": self.config.source.source_name or "",
            "window": self.config.history.window,
            "start": start,
            "end": end,
            "status": status,
            "child_run_ids": child_run_ids,
            "warning_count": len(parent_warnings),
            "warnings": parent_warnings,
        }

        outputs = {
            "history_runs": persist_table(self.paths, "history_runs", [parent_row]),
            "history_windows": persist_table(self.paths, "history_windows", window_rows),
        }
        sync_duckdb(self.paths.database_path, outputs)
        self._write_parent_manifest(parent_row, window_rows)

        return {
            "history_run_id": resolved_history_run_id,
            "status": status,
            "windows": window_rows,
            "warnings": parent_warnings,
        }

    def _child_run_ready(self, child_run_id: str) -> bool:
        if not (self.paths.run_raw_dir(child_run_id) / "manifest.json").exists():
            return False
        collection_runs_path = self.paths.processed_root / "collection_runs.parquet"
        if not collection_runs_path.exists():
            return False
        collection_runs = pl.read_parquet(collection_runs_path)
        return "run_id" in collection_runs.columns and not collection_runs.filter(pl.col("run_id") == child_run_id).is_empty()

    def _summarize_window(
        self,
        *,
        history_run_id: str,
        window: HistoryWindow,
        child_run_id: str,
        status_override: str | None = None,
    ) -> dict[str, Any]:
        collection_run = _first_row(_load_run_table(self.paths, "collection_runs", child_run_id))
        posts = _load_run_table(self.paths, "posts", child_run_id)
        comments = _load_run_table(self.paths, "comments", child_run_id)
        propagations = _load_run_table(self.paths, "propagations", child_run_id)
        match_hits = _load_run_table(self.paths, "match_hits", child_run_id)
        manifest = _load_manifest(self.paths, child_run_id)
        warnings = _list_value(collection_run.get("warning_messages") if collection_run else None)
        if not warnings:
            warnings = _list_value(manifest.get("warnings") if manifest else None)
        status = status_override or str(collection_run.get("status") if collection_run else manifest.get("status", "success"))

        return {
            "history_run_id": history_run_id,
            "window_id": window.window_id,
            "child_run_id": child_run_id,
            "start": window.start,
            "end": window.end,
            "status": status,
            "post_count": _int_value(collection_run.get("post_count") if collection_run else None, posts.height),
            "comment_count": _int_value(collection_run.get("comment_count") if collection_run else None, comments.height),
            "propagation_count": _int_value(
                collection_run.get("propagation_count") if collection_run else None,
                propagations.height,
            ),
            "match_hit_count": match_hits.height,
            "warning_count": len(warnings),
            "coverage_gap_total": _coverage_gap_total(posts, comments, propagations),
            "warnings": [str(item) for item in warnings],
        }

    def _empty_window_row(
        self,
        *,
        history_run_id: str,
        window: HistoryWindow,
        child_run_id: str,
        status: str,
        warnings: list[str],
    ) -> dict[str, Any]:
        return {
            "history_run_id": history_run_id,
            "window_id": window.window_id,
            "child_run_id": child_run_id,
            "start": window.start,
            "end": window.end,
            "status": status,
            "post_count": 0,
            "comment_count": 0,
            "propagation_count": 0,
            "match_hit_count": 0,
            "warning_count": len(warnings),
            "coverage_gap_total": 0,
            "warnings": warnings,
        }

    def _parent_warnings(self, *, start: str, end: str, windows: list[HistoryWindow]) -> list[str]:
        warnings: list[str] = []
        theoretical = len(build_monthly_windows(start, end, max_windows=10_000))
        if theoretical > len(windows):
            warnings.append(
                f"History backfill was truncated by history.max_windows={self.config.history.max_windows}; "
                f"requested_windows={theoretical}, executed_windows={len(windows)}."
            )
        if self.config.source.platform != "telegram" or self.config.collector.mode != "mtproto":
            warnings.append(
                "Historical v1 is production-quality only for telegram_mtproto; this platform uses existing best-effort collector coverage."
            )
        return warnings

    def _write_parent_manifest(self, parent_row: dict[str, Any], window_rows: list[dict[str, Any]]) -> None:
        history_dir = self.paths.raw_root / "_history" / parent_row["history_run_id"]
        history_dir.mkdir(parents=True, exist_ok=True)
        (history_dir / "manifest.json").write_text(
            json.dumps({**parent_row, "windows": window_rows}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


class HistoryAnalysisService:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, history_run_id: str) -> dict[str, Any]:
        history_run = self._history_run(history_run_id)
        windows = _load_history_table(self.paths, "history_windows", history_run_id)
        child_run_ids = [str(item) for item in windows["child_run_id"].to_list()] if "child_run_id" in windows.columns else []
        child_run_ids = [item for item in child_run_ids if item]

        posts = _load_child_runs_table(self.paths, "posts", child_run_ids)
        comments = _load_child_runs_table(self.paths, "comments", child_run_ids)
        propagations = _load_child_runs_table(self.paths, "propagations", child_run_ids)
        match_hits = _load_child_runs_table(self.paths, "match_hits", child_run_ids)
        window_by_run = {row["child_run_id"]: row["window_id"] for row in windows.to_dicts()}

        item_index = self._build_item_index(history_run_id, window_by_run, posts, comments, propagations)
        coverage_gaps = self._build_coverage_gaps(history_run_id, window_by_run, posts, comments, propagations)
        cluster_rows, membership_rows, stance_rows = self._analyze_items(history_run_id, item_index, posts, comments, propagations)
        temporal_metrics = self._build_temporal_metrics(
            history_run_id=history_run_id,
            source_kind=str(history_run.get("source_kind") or "feed"),
            item_index=item_index,
            memberships=pl.DataFrame(membership_rows) if membership_rows else pl.DataFrame(),
            stance_labels=pl.DataFrame(stance_rows) if stance_rows else pl.DataFrame(),
            posts=posts,
            comments=comments,
            propagations=propagations,
            match_hits=match_hits,
            window_by_run=window_by_run,
        )

        outputs = {
            "history_item_index": persist_table(self.paths, "history_item_index", item_index.to_dicts()),
            "history_narrative_clusters": persist_table(self.paths, "history_narrative_clusters", cluster_rows),
            "history_cluster_memberships": persist_table(self.paths, "history_cluster_memberships", membership_rows),
            "history_stance_labels": persist_table(self.paths, "history_stance_labels", stance_rows),
            "history_temporal_metrics": persist_table(self.paths, "history_temporal_metrics", temporal_metrics),
            "history_coverage_gaps": persist_table(self.paths, "history_coverage_gaps", coverage_gaps),
        }
        sync_duckdb(self.paths.database_path, outputs)
        return {
            "history_run_id": history_run_id,
            "item_count": item_index.height,
            "cluster_count": len(cluster_rows),
            "temporal_metric_count": len(temporal_metrics),
        }

    def _history_run(self, history_run_id: str) -> dict[str, Any]:
        history_runs = _load_history_table(self.paths, "history_runs", history_run_id)
        if history_runs.is_empty():
            raise RuntimeError(f"History run '{history_run_id}' was not found.")
        return history_runs.to_dicts()[0]

    def _build_item_index(
        self,
        history_run_id: str,
        window_by_run: dict[str, str],
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        propagations: pl.DataFrame,
    ) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        rows.extend(self._index_rows(history_run_id, window_by_run, posts, "post", "post_id"))
        rows.extend(self._index_rows(history_run_id, window_by_run, comments, "comment", "comment_id"))
        rows.extend(self._index_rows(history_run_id, window_by_run, propagations, "propagation", "propagation_id"))
        return pl.DataFrame(rows) if rows else pl.DataFrame(schema=_history_item_index_schema())

    def _index_rows(
        self,
        history_run_id: str,
        window_by_run: dict[str, str],
        frame: pl.DataFrame,
        item_type: str,
        id_column: str,
    ) -> list[dict[str, Any]]:
        if frame.is_empty() or id_column not in frame.columns:
            return []
        rows = []
        for row in frame.to_dicts():
            child_run_id = str(row.get("run_id") or "")
            rows.append(
                {
                    "history_run_id": history_run_id,
                    "window_id": window_by_run.get(child_run_id, _window_id_from_created_at(row.get("created_at"))),
                    "child_run_id": child_run_id,
                    "item_type": item_type,
                    "item_id": str(row.get(id_column) or ""),
                    "created_at": str(row.get("created_at") or ""),
                    "parent_post_id": str(row.get("parent_post_id") or ""),
                    "parent_entity_type": str(row.get("parent_entity_type") or ""),
                    "parent_entity_id": str(row.get("parent_entity_id") or ""),
                    "origin_post_id": str(row.get("origin_post_id") or ""),
                    "container_source_id": str(row.get("container_source_id") or ""),
                    "permalink": str(row.get("permalink") or ""),
                }
            )
        return rows

    def _analyze_items(
        self,
        history_run_id: str,
        item_index: pl.DataFrame,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        propagations: pl.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        providers = build_providers(self.config.providers.embeddings, self.config.providers.llm)
        cache_store = AnalysisCacheStore(self.config, self.paths)
        clusterer = NarrativeClusterer(
            llm_provider=providers.llm,
            exemplar_count=self.config.analysis.exemplar_count,
            min_cluster_size=self.config.analysis.min_cluster_size,
            min_samples=self.config.analysis.min_samples,
        )
        cluster_rows: list[dict[str, Any]] = []
        membership_rows: list[dict[str, Any]] = []
        stance_rows: list[dict[str, Any]] = []
        index_lookup = {(row["item_type"], row["item_id"]): row for row in item_index.to_dicts()}
        for item_type, frame, id_column in (
            ("post", posts, "post_id"),
            ("comment", comments, "comment_id"),
            ("propagation", propagations, "propagation_id"),
        ):
            items = _items_from_frame(frame, item_type, id_column)
            embeddings = cache_store.embedding_matrix(
                items,
                provider_name=providers.embeddings.name,
                embed_many=providers.embeddings.embed_texts,
                batch_size=self.config.analysis.batch_size,
                dimension=self.config.providers.embeddings.dimension,
            )
            clusters, memberships = clusterer.cluster_items(item_type, items, embeddings, history_run_id)
            for row in clusters:
                row["history_run_id"] = row.pop("run_id")
            for row in memberships:
                row["history_run_id"] = row.pop("run_id")
                index_row = index_lookup.get((row["item_type"], row["item_id"]), {})
                row["window_id"] = index_row.get("window_id", "")
                row["child_run_id"] = index_row.get("child_run_id", "")
            labels = cache_store.stance_predictions(
                items,
                llm_name=providers.llm.name,
                sides=self.config.sides,
                classify_one=providers.llm.classify_stance,
            )
            for row in labels:
                index_row = index_lookup.get((row["item_type"], row["item_id"]), {})
                row["history_run_id"] = history_run_id
                row["window_id"] = index_row.get("window_id", "")
                row["child_run_id"] = index_row.get("child_run_id", "")
            cluster_rows.extend(clusters)
            membership_rows.extend(memberships)
            stance_rows.extend(labels)
        return cluster_rows, membership_rows, stance_rows

    def _build_temporal_metrics(
        self,
        *,
        history_run_id: str,
        source_kind: str,
        item_index: pl.DataFrame,
        memberships: pl.DataFrame,
        stance_labels: pl.DataFrame,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        propagations: pl.DataFrame,
        match_hits: pl.DataFrame,
        window_by_run: dict[str, str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend(_volume_metrics(history_run_id, item_index, posts, comments, propagations))
        rows.extend(_cluster_metrics(history_run_id, memberships, posts, comments, propagations))
        rows.extend(_stance_metrics(history_run_id, stance_labels, posts, comments, propagations))
        if source_kind == "person_monitor":
            rows.extend(_person_monitor_metrics(history_run_id, match_hits, window_by_run))
        return rows

    def _build_coverage_gaps(
        self,
        history_run_id: str,
        window_by_run: dict[str, str],
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        propagations: pl.DataFrame,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend(_coverage_gap_rows(history_run_id, window_by_run, posts, comments, "post", "post_id"))
        rows.extend(_coverage_gap_rows(history_run_id, window_by_run, propagations, comments, "propagation", "propagation_id"))
        return rows


class HistoryReportService:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, history_run_id: str) -> list[Any]:
        output_dir = self.paths.reports_root / "history" / history_run_id
        tables_dir = output_dir / "tables"
        output_dir.mkdir(parents=True, exist_ok=True)
        tables_dir.mkdir(parents=True, exist_ok=True)

        history_runs = _load_history_table(self.paths, "history_runs", history_run_id)
        windows = _load_history_table(self.paths, "history_windows", history_run_id)
        metrics = _load_history_table(self.paths, "history_temporal_metrics", history_run_id)
        gaps = _load_history_table(self.paths, "history_coverage_gaps", history_run_id)
        clusters = _load_history_table(self.paths, "history_narrative_clusters", history_run_id)

        csv_paths = []
        for table_name, frame in {
            "history_windows": windows,
            "history_temporal_metrics": metrics,
            "history_coverage_gaps": gaps,
            "history_narrative_clusters": clusters,
        }.items():
            csv_path = tables_dir / f"{table_name}.csv"
            _csv_safe_frame(frame).write_csv(csv_path)
            csv_paths.append(csv_path)

        markdown_path = output_dir / "history_report.md"
        html_path = output_dir / "history_report.html"
        markdown_text = self._markdown(history_run_id, history_runs, windows, metrics, gaps, clusters)
        markdown_path.write_text(markdown_text, encoding="utf-8")
        html_path.write_text(markdown.markdown(markdown_text, extensions=["tables", "fenced_code"]), encoding="utf-8")
        return [markdown_path, html_path, *csv_paths]

    def _markdown(
        self,
        history_run_id: str,
        history_runs: pl.DataFrame,
        windows: pl.DataFrame,
        metrics: pl.DataFrame,
        gaps: pl.DataFrame,
        clusters: pl.DataFrame,
    ) -> str:
        run_row = history_runs.to_dicts()[0] if not history_runs.is_empty() else {}
        source_kind = str(run_row.get("source_kind") or "feed")
        total_posts = int(windows["post_count"].sum()) if "post_count" in windows.columns and windows.height else 0
        total_comments = int(windows["comment_count"].sum()) if "comment_count" in windows.columns and windows.height else 0
        lines = [
            f"# Historical Profile Intelligence: {history_run_id}",
            "",
            "## Timeline Overview",
            f"- Status: {run_row.get('status', 'unknown')}",
            f"- Window: {run_row.get('window', 'month')}",
            f"- Windows analyzed: {windows.height}",
            f"- Posts: {total_posts}",
            f"- Comments: {total_comments}",
            "",
            "## Narrative Evolution",
            f"- Global narrative clusters: {clusters.height}",
            "",
            "## Stance/Support Trend",
            f"- Temporal metric rows: {metrics.height}",
            "",
            "## Audience Comment Trend",
            f"- Coverage gap rows: {gaps.height}",
            "",
            "## Top Turning-Point Months",
        ]
        if metrics.is_empty() or "net_support" not in metrics.columns:
            lines.append("- No temporal stance metrics available.")
        else:
            for row in metrics.sort("net_support").head(5).to_dicts():
                lines.append(
                    f"- {row.get('window_id')}: {row.get('metric_kind')} {row.get('item_type')} "
                    f"net_support={row.get('net_support')}"
                )
        if source_kind == "person_monitor":
            lines.extend(
                [
                    "",
                    "## Person Monitor Activity",
                    f"- Authored activity items: {_metric_item_total(metrics, 'person_monitor_authored_activity')}",
                    f"- Mention activity items: {_metric_item_total(metrics, 'person_monitor_mention_activity')}",
                ]
            )
        lines.extend(["", "## Coverage Warnings"])
        if gaps.is_empty():
            lines.append("- No coverage gaps detected.")
        else:
            for row in gaps.sort("comment_gap", descending=True).head(10).to_dicts():
                lines.append(
                    f"- {row.get('window_id')} {row.get('item_type')} {row.get('item_id')}: "
                    f"visible={row.get('visible_comment_count')} extracted={row.get('extracted_comment_count')}"
                )
        lines.append("")
        return "\n".join(lines)


def _parse_date(raw_value: str) -> date:
    return date.fromisoformat(raw_value[:10])


def _load_manifest(paths: ProjectPaths, run_id: str) -> dict[str, Any]:
    manifest_path = paths.run_raw_dir(run_id) / "manifest.json"
    return read_json(manifest_path) if manifest_path.exists() else {}


def _first_row(frame: pl.DataFrame) -> dict[str, Any] | None:
    if frame.is_empty():
        return None
    return frame.head(1).to_dicts()[0]


def _load_run_table(paths: ProjectPaths, table_name: str, run_id: str) -> pl.DataFrame:
    path = paths.processed_root / f"{table_name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    frame = pl.read_parquet(path)
    if "run_id" not in frame.columns:
        return pl.DataFrame()
    return frame.filter(pl.col("run_id") == run_id)


def _load_child_runs_table(paths: ProjectPaths, table_name: str, child_run_ids: list[str]) -> pl.DataFrame:
    path = paths.processed_root / f"{table_name}.parquet"
    if not path.exists() or not child_run_ids:
        return pl.DataFrame()
    frame = pl.read_parquet(path)
    if "run_id" not in frame.columns:
        return pl.DataFrame()
    return frame.filter(pl.col("run_id").is_in(child_run_ids))


def _load_history_table(paths: ProjectPaths, table_name: str, history_run_id: str) -> pl.DataFrame:
    path = paths.processed_root / f"{table_name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    frame = pl.read_parquet(path)
    if "history_run_id" not in frame.columns:
        return pl.DataFrame()
    return frame.filter(pl.col("history_run_id") == history_run_id)


def _list_value(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _csv_safe_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    rows: list[dict[str, Any]] = []
    for row in frame.to_dicts():
        rows.append(
            {
                key: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (list, dict))
                else value
                for key, value in row.items()
            }
        )
    return pl.DataFrame(rows)


def _int_value(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _metric_item_total(metrics: pl.DataFrame, metric_kind: str) -> int:
    if metrics.is_empty() or "metric_kind" not in metrics.columns or "item_count" not in metrics.columns:
        return 0
    matching = metrics.filter(pl.col("metric_kind") == metric_kind)
    return int(matching["item_count"].sum()) if matching.height else 0


def _coverage_gap_total(posts: pl.DataFrame, comments: pl.DataFrame, propagations: pl.DataFrame) -> int:
    return sum(row["comment_gap"] for row in _coverage_gap_rows("", {}, posts, comments, "post", "post_id")) + sum(
        row["comment_gap"] for row in _coverage_gap_rows("", {}, propagations, comments, "propagation", "propagation_id")
    )


def _coverage_gap_rows(
    history_run_id: str,
    window_by_run: dict[str, str],
    items: pl.DataFrame,
    comments: pl.DataFrame,
    item_type: str,
    id_column: str,
) -> list[dict[str, Any]]:
    if items.is_empty() or id_column not in items.columns or "comments_count" not in items.columns:
        return []
    parent_column = "parent_post_id" if item_type == "post" else "parent_entity_id"
    extracted = (
        comments.group_by(parent_column).agg(pl.len().alias("extracted_comment_count"))
        if not comments.is_empty() and parent_column in comments.columns
        else pl.DataFrame(
            {
                parent_column: pl.Series([], dtype=pl.String),
                "extracted_comment_count": pl.Series([], dtype=pl.Int64),
            }
        )
    )
    rows = (
        items.select(
            id_column,
            "run_id",
            "comments_count",
            pl.col("permalink").fill_null("").alias("permalink") if "permalink" in items.columns else pl.lit("").alias("permalink"),
        )
        .join(extracted, left_on=id_column, right_on=parent_column, how="left")
        .with_columns(
            pl.col("extracted_comment_count").fill_null(0),
            (pl.col("comments_count") - pl.col("extracted_comment_count").fill_null(0)).alias("comment_gap"),
        )
        .filter(pl.col("comment_gap") > 0)
        .to_dicts()
    )
    return [
        {
            "history_run_id": history_run_id,
            "window_id": window_by_run.get(str(row.get("run_id") or ""), ""),
            "child_run_id": str(row.get("run_id") or ""),
            "item_type": item_type,
            "item_id": str(row.get(id_column) or ""),
            "visible_comment_count": _int_value(row.get("comments_count"), 0),
            "extracted_comment_count": _int_value(row.get("extracted_comment_count"), 0),
            "comment_gap": _int_value(row.get("comment_gap"), 0),
            "permalink": str(row.get("permalink") or ""),
        }
        for row in rows
    ]


def _items_from_frame(frame: pl.DataFrame, item_type: str, id_column: str) -> list[dict[str, Any]]:
    if frame.is_empty() or id_column not in frame.columns or "message" not in frame.columns:
        return []
    rows = frame.select(
        pl.col(id_column).alias("item_id"),
        pl.lit(item_type).alias("item_type"),
        pl.col("message").fill_null("").alias("text"),
    ).filter(pl.col("text").str.len_chars() > 0)
    return rows.to_dicts()


def _history_item_index_schema() -> dict[str, Any]:
    return {
        "history_run_id": pl.String,
        "window_id": pl.String,
        "child_run_id": pl.String,
        "item_type": pl.String,
        "item_id": pl.String,
        "created_at": pl.String,
        "parent_post_id": pl.String,
        "parent_entity_type": pl.String,
        "parent_entity_id": pl.String,
        "origin_post_id": pl.String,
        "container_source_id": pl.String,
        "permalink": pl.String,
    }


def _window_id_from_created_at(value: Any) -> str:
    text = str(value or "")
    return text[:7].replace("-", "") if len(text) >= 7 else ""


def _engagement_lookup(posts: pl.DataFrame, comments: pl.DataFrame, propagations: pl.DataFrame) -> dict[tuple[str, str], int]:
    lookup: dict[tuple[str, str], int] = {}
    for item_type, frame, id_column in (
        ("post", posts, "post_id"),
        ("comment", comments, "comment_id"),
        ("propagation", propagations, "propagation_id"),
    ):
        if frame.is_empty() or id_column not in frame.columns:
            continue
        for row in frame.to_dicts():
            lookup[(item_type, str(row.get(id_column) or ""))] = (
                _int_value(row.get("reactions"), 0)
                + _int_value(row.get("shares"), 0)
                + _int_value(row.get("views"), 0)
                + _int_value(row.get("forwards"), 0)
            )
    return lookup


def _empty_metric(
    *,
    history_run_id: str,
    window_id: str,
    item_type: str,
    cluster_id: str = "",
    side_id: str = "",
    metric_kind: str,
    item_count: int,
    engagement_total: int = 0,
) -> dict[str, Any]:
    return {
        "history_run_id": history_run_id,
        "window_id": window_id,
        "item_type": item_type,
        "cluster_id": cluster_id,
        "side_id": side_id,
        "metric_kind": metric_kind,
        "item_count": item_count,
        "support_count": 0,
        "oppose_count": 0,
        "neutral_count": 0,
        "unclear_count": 0,
        "support_ratio": 0.0,
        "net_support": 0,
        "engagement_total": engagement_total,
    }


def _volume_metrics(
    history_run_id: str,
    item_index: pl.DataFrame,
    posts: pl.DataFrame,
    comments: pl.DataFrame,
    propagations: pl.DataFrame,
) -> list[dict[str, Any]]:
    if item_index.is_empty():
        return []
    engagement = _engagement_lookup(posts, comments, propagations)
    rows: list[dict[str, Any]] = []
    for group in item_index.group_by(["window_id", "item_type"]).agg(pl.len().alias("item_count")).to_dicts():
        window_id = group["window_id"]
        item_type = group["item_type"]
        item_ids = item_index.filter((pl.col("window_id") == window_id) & (pl.col("item_type") == item_type))["item_id"].to_list()
        rows.append(
            _empty_metric(
                history_run_id=history_run_id,
                window_id=window_id,
                item_type=item_type,
                metric_kind="volume",
                item_count=_int_value(group["item_count"], 0),
                engagement_total=sum(engagement.get((item_type, str(item_id)), 0) for item_id in item_ids),
            )
        )
    return rows


def _cluster_metrics(
    history_run_id: str,
    memberships: pl.DataFrame,
    posts: pl.DataFrame,
    comments: pl.DataFrame,
    propagations: pl.DataFrame,
) -> list[dict[str, Any]]:
    if memberships.is_empty():
        return []
    engagement = _engagement_lookup(posts, comments, propagations)
    rows: list[dict[str, Any]] = []
    for group in memberships.group_by(["window_id", "item_type", "cluster_id"]).agg(pl.len().alias("item_count")).to_dicts():
        item_ids = memberships.filter(
            (pl.col("window_id") == group["window_id"])
            & (pl.col("item_type") == group["item_type"])
            & (pl.col("cluster_id") == group["cluster_id"])
        )["item_id"].to_list()
        rows.append(
            _empty_metric(
                history_run_id=history_run_id,
                window_id=group["window_id"],
                item_type=group["item_type"],
                cluster_id=group["cluster_id"],
                metric_kind="narrative_cluster",
                item_count=_int_value(group["item_count"], 0),
                engagement_total=sum(engagement.get((group["item_type"], str(item_id)), 0) for item_id in item_ids),
            )
        )
    return rows


def _stance_metrics(
    history_run_id: str,
    stance_labels: pl.DataFrame,
    posts: pl.DataFrame,
    comments: pl.DataFrame,
    propagations: pl.DataFrame,
) -> list[dict[str, Any]]:
    if stance_labels.is_empty():
        return []
    engagement = _engagement_lookup(posts, comments, propagations)
    rows: list[dict[str, Any]] = []
    for group in (
        stance_labels.group_by(["window_id", "item_type", "side_id"])
        .agg(
            pl.len().alias("item_count"),
            (pl.col("label") == "support").sum().alias("support_count"),
            (pl.col("label") == "oppose").sum().alias("oppose_count"),
            (pl.col("label") == "neutral").sum().alias("neutral_count"),
            (pl.col("label") == "unclear").sum().alias("unclear_count"),
        )
        .to_dicts()
    ):
        item_ids = stance_labels.filter(
            (pl.col("window_id") == group["window_id"])
            & (pl.col("item_type") == group["item_type"])
            & (pl.col("side_id") == group["side_id"])
        )["item_id"].to_list()
        support_count = _int_value(group["support_count"], 0)
        oppose_count = _int_value(group["oppose_count"], 0)
        neutral_count = _int_value(group["neutral_count"], 0)
        denominator = max(support_count + oppose_count + neutral_count, 1)
        rows.append(
            {
                "history_run_id": history_run_id,
                "window_id": group["window_id"],
                "item_type": group["item_type"],
                "cluster_id": "",
                "side_id": group["side_id"],
                "metric_kind": "stance",
                "item_count": _int_value(group["item_count"], 0),
                "support_count": support_count,
                "oppose_count": oppose_count,
                "neutral_count": neutral_count,
                "unclear_count": _int_value(group["unclear_count"], 0),
                "support_ratio": support_count / denominator,
                "net_support": support_count - oppose_count,
                "engagement_total": sum(engagement.get((group["item_type"], str(item_id)), 0) for item_id in item_ids),
            }
        )
    return rows


def _person_monitor_metrics(
    history_run_id: str,
    match_hits: pl.DataFrame,
    window_by_run: dict[str, str],
) -> list[dict[str, Any]]:
    if match_hits.is_empty() or "match_kind" not in match_hits.columns:
        return []
    rows: list[dict[str, Any]] = []
    with_windows = match_hits.with_columns(pl.col("run_id").replace(window_by_run).alias("window_id"))
    for group in with_windows.group_by(["window_id", "item_type", "match_kind"]).agg(pl.len().alias("item_count")).to_dicts():
        metric_kind = (
            "person_monitor_authored_activity"
            if group["match_kind"] == "authored_by_subject"
            else "person_monitor_mention_activity"
        )
        rows.append(
            _empty_metric(
                history_run_id=history_run_id,
                window_id=group["window_id"],
                item_type=group["item_type"],
                metric_kind=metric_kind,
                item_count=_int_value(group["item_count"], 0),
            )
        )
    return rows
