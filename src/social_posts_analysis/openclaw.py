from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .config import ProjectConfig
from .history import history_coverage_summary, history_stance_turning_points, history_window_records
from .paths import ProjectPaths
from .utils import read_json, utc_now_iso

SCHEMA_VERSION = "openclaw.social_posts_analysis.v1"
HISTORY_SCHEMA_VERSION = "openclaw.social_posts_analysis.history.v1"


@dataclass(slots=True)
class OpenClawExportOutputs:
    bundle_path: Path
    brief_path: Path


class OpenClawExportService:
    """Build a stable file-contract bundle from an already completed local run."""

    def __init__(
        self,
        config: ProjectConfig | None,
        project_paths: ProjectPaths | None = None,
        paths: ProjectPaths | None = None,
    ) -> None:
        resolved_paths = project_paths or paths
        if resolved_paths is None:
            raise TypeError("OpenClawExportService requires project paths.")
        self.config = config
        self.paths = resolved_paths

    def run(self, run_id: str | None = None) -> OpenClawExportOutputs:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("OpenClaw export requires an existing run_id with data/raw/<run_id>/manifest.json.")

        manifest_path = self.paths.run_raw_dir(resolved_run_id) / "manifest.json"
        if not manifest_path.exists():
            raise RuntimeError(
                f"OpenClaw export requires an existing run_id with data/raw/{resolved_run_id}/manifest.json."
            )

        manifest = read_json(manifest_path)
        tables = {
            "posts": self._load_table("posts", resolved_run_id),
            "comments": self._load_table("comments", resolved_run_id),
            "propagations": self._load_table("propagations", resolved_run_id),
            "collection_runs": self._load_table("collection_runs", resolved_run_id),
            "observed_sources": self._load_table("observed_sources", resolved_run_id),
            "match_hits": self._load_table("match_hits", resolved_run_id),
        }
        collection_run = self._first_row(tables["collection_runs"])
        output_dir = self.paths.reports_root / "openclaw" / resolved_run_id
        bundle_path = output_dir / "bundle.json"
        brief_path = output_dir / "brief.md"

        source = self._build_source(manifest, collection_run)
        source_kind = source.get("source_kind") or "feed"
        warnings = self._build_warning_rows(resolved_run_id, collection_run, manifest)
        coverage_gaps = self._post_coverage_gaps(tables["posts"], tables["comments"])
        propagation_coverage_gaps = self._propagation_coverage_gaps(tables["propagations"], tables["comments"])
        person_monitor = self._build_person_monitor(
            source_kind=source_kind,
            observed_sources=tables["observed_sources"],
            match_hits=tables["match_hits"],
            posts=tables["posts"],
            comments=tables["comments"],
        )
        counts = self._build_counts(
            manifest=manifest,
            collection_run=collection_run,
            tables=tables,
            warnings=warnings,
        )
        status = self._string_value(collection_run.get("status") if collection_run else None, manifest.get("status", "unknown"))

        bundle = {
            "schema_version": SCHEMA_VERSION,
            "run_id": resolved_run_id,
            "created_at": utc_now_iso(),
            "project_name": self.config.project_name if self.config else "",
            "source": source,
            "platform": source.get("platform"),
            "source_kind": source_kind,
            "collector": self._string_value(
                collection_run.get("collector") if collection_run else None,
                manifest.get("collector", ""),
            ),
            "mode": self._string_value(collection_run.get("mode") if collection_run else None, manifest.get("mode", "")),
            "status": status,
            "counts": counts,
            "artifacts": self._build_artifacts(
                run_id=resolved_run_id,
                manifest_path=manifest_path,
                bundle_path=bundle_path,
                brief_path=brief_path,
            ),
            "warnings": warnings,
            "coverage_gaps": coverage_gaps,
            "propagation_coverage_gaps": propagation_coverage_gaps,
            "person_monitor": person_monitor,
            "next_actions": self._next_actions(
                status=status,
                warnings=warnings,
                coverage_gaps=coverage_gaps,
                propagation_coverage_gaps=propagation_coverage_gaps,
                person_monitor=person_monitor,
            ),
        }

        output_dir.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(
            json.dumps(_json_safe(bundle), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        brief_path.write_text(self._brief_markdown(bundle), encoding="utf-8")
        return OpenClawExportOutputs(bundle_path=bundle_path, brief_path=brief_path)

    def run_history(self, history_run_id: str) -> OpenClawExportOutputs:
        history_runs = self._load_history_table("history_runs", history_run_id)
        if history_runs.is_empty():
            raise RuntimeError(f"OpenClaw history export requires an existing history_run_id '{history_run_id}'.")

        windows = self._load_history_table("history_windows", history_run_id)
        temporal_metrics = self._load_history_table("history_temporal_metrics", history_run_id)
        coverage_gaps = self._load_history_table("history_coverage_gaps", history_run_id)
        clusters = self._load_history_table("history_narrative_clusters", history_run_id)
        run_row = history_runs.to_dicts()[0]
        output_dir = self.paths.reports_root / "openclaw" / history_run_id
        bundle_path = output_dir / "bundle.json"
        brief_path = output_dir / "brief.md"

        counts = {
            "windows": windows.height,
            "posts": int(windows["post_count"].sum()) if "post_count" in windows.columns and windows.height else 0,
            "comments": int(windows["comment_count"].sum()) if "comment_count" in windows.columns and windows.height else 0,
            "propagations": int(windows["propagation_count"].sum()) if "propagation_count" in windows.columns and windows.height else 0,
            "match_hits": int(windows["match_hit_count"].sum()) if "match_hit_count" in windows.columns and windows.height else 0,
            "coverage_gaps": coverage_gaps.height,
            "warnings": int(windows["warning_count"].sum()) if "warning_count" in windows.columns and windows.height else 0,
            "narrative_clusters": clusters.height,
        }
        bundle = {
            "schema_version": HISTORY_SCHEMA_VERSION,
            "history_run_id": history_run_id,
            "created_at": utc_now_iso(),
            "project_name": self.config.project_name if self.config else "",
            "platform": run_row.get("platform"),
            "source_kind": run_row.get("source_kind"),
            "source": {
                "platform": run_row.get("platform"),
                "source_kind": run_row.get("source_kind"),
                "source_id": run_row.get("source_id"),
                "source_name": run_row.get("source_name"),
            },
            "status": run_row.get("status"),
            "counts": counts,
            "artifacts": self._build_history_artifacts(history_run_id, bundle_path, brief_path),
            "history": {
                "start": run_row.get("start"),
                "end": run_row.get("end"),
                "window": run_row.get("window"),
                "windows": history_window_records(windows, limit=120),
                "temporal_metrics": _frame_records(temporal_metrics, limit=200),
                "coverage_summary": history_coverage_summary(windows),
                "coverage_gaps": _frame_records(coverage_gaps, limit=100),
                "top_stance_shifts": self._top_history_stance_shifts(temporal_metrics),
                "top_narratives": _frame_records(clusters, limit=50),
            },
            "next_actions": self._history_next_actions(run_row, windows, coverage_gaps),
        }

        output_dir.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(json.dumps(_json_safe(bundle), ensure_ascii=False, indent=2), encoding="utf-8")
        brief_path.write_text(self._history_brief_markdown(bundle), encoding="utf-8")
        return OpenClawExportOutputs(bundle_path=bundle_path, brief_path=brief_path)

    def _load_table(self, table_name: str, run_id: str) -> pl.DataFrame:
        table_path = self.paths.processed_root / f"{table_name}.parquet"
        if not table_path.exists():
            return pl.DataFrame()
        frame = pl.read_parquet(table_path)
        if "run_id" in frame.columns:
            return frame.filter(pl.col("run_id") == run_id)
        return frame

    def _load_history_table(self, table_name: str, history_run_id: str) -> pl.DataFrame:
        table_path = self.paths.processed_root / f"{table_name}.parquet"
        if not table_path.exists():
            return pl.DataFrame()
        frame = pl.read_parquet(table_path)
        if "history_run_id" in frame.columns:
            return frame.filter(pl.col("history_run_id") == history_run_id)
        return pl.DataFrame()

    def _first_row(self, frame: pl.DataFrame) -> dict[str, Any] | None:
        if frame.is_empty():
            return None
        return frame.head(1).to_dicts()[0]

    def _build_source(self, manifest: dict[str, Any], collection_run: dict[str, Any] | None) -> dict[str, Any]:
        raw_source = manifest.get("source") or {}
        config_source = self.config.source if self.config else None
        source_kind = self._string_value(
            collection_run.get("source_kind") if collection_run else None,
            raw_source.get("source_kind") or (config_source.kind if config_source else "feed"),
        )
        platform = self._string_value(
            collection_run.get("platform") if collection_run else None,
            raw_source.get("platform") or (config_source.platform if config_source else ""),
        )
        return {
            "platform": platform,
            "source_kind": source_kind,
            "source_id": self._string_value(
                collection_run.get("source_id") if collection_run else None,
                raw_source.get("source_id") or (config_source.source_id if config_source else ""),
            ),
            "source_name": self._string_value(
                collection_run.get("source_name") if collection_run else None,
                raw_source.get("source_name") or (config_source.source_name if config_source else ""),
            ),
            "source_url": self._string_value(raw_source.get("source_url") or (config_source.url if config_source else None), ""),
            "source_type": self._string_value(collection_run.get("source_type") if collection_run else None, raw_source.get("source_type", "")),
        }

    def _build_counts(
        self,
        manifest: dict[str, Any],
        collection_run: dict[str, Any] | None,
        tables: dict[str, pl.DataFrame],
        warnings: list[dict[str, Any]],
    ) -> dict[str, int]:
        manifest_posts = manifest.get("posts") or []
        manifest_comments = sum(len(post.get("comments") or []) for post in manifest_posts if isinstance(post, dict))
        return {
            "posts": self._int_value(collection_run.get("post_count") if collection_run else None, tables["posts"].height or len(manifest_posts)),
            "comments": self._int_value(
                collection_run.get("comment_count") if collection_run else None,
                tables["comments"].height or manifest_comments,
            ),
            "propagations": self._int_value(
                collection_run.get("propagation_count") if collection_run else None,
                tables["propagations"].height or len(manifest.get("propagations") or []),
            ),
            "match_hits": tables["match_hits"].height,
            "observed_sources": tables["observed_sources"].height,
            "warnings": len(warnings),
        }

    def _build_artifacts(
        self,
        run_id: str,
        manifest_path: Path,
        bundle_path: Path,
        brief_path: Path,
    ) -> dict[str, Any]:
        report_markdown = self.paths.reports_root / f"report_{run_id}.md"
        report_html = self.paths.reports_root / f"report_{run_id}.html"
        tabular_dir = self.paths.reports_root / f"report_{run_id}_tables"
        csv_exports: dict[str, str] = {}
        if tabular_dir.exists():
            csv_exports = {path.stem: self._path_string(path) for path in sorted(tabular_dir.glob("*.csv"))}

        return {
            "raw_manifest": self._path_string(manifest_path),
            "processed_dir": self._path_string(self.paths.processed_root),
            "duckdb": self._path_string(self.paths.database_path),
            "report_markdown": self._path_string(report_markdown) if report_markdown.exists() else None,
            "report_html": self._path_string(report_html) if report_html.exists() else None,
            "tabular_dir": self._path_string(tabular_dir) if tabular_dir.exists() else None,
            "csv_exports": csv_exports,
            "openclaw_bundle": self._path_string(bundle_path),
            "openclaw_brief": self._path_string(brief_path),
        }

    def _build_warning_rows(
        self,
        run_id: str,
        collection_run: dict[str, Any] | None,
        manifest: dict[str, Any],
    ) -> list[dict[str, Any]]:
        source_run_ids = self._list_value(collection_run.get("source_run_ids") if collection_run else None) or [run_id]
        rows: list[dict[str, Any]] = []
        for source_run_id in source_run_ids:
            source_manifest_path = self.paths.run_raw_dir(str(source_run_id)) / "manifest.json"
            if not source_manifest_path.exists():
                continue
            source_manifest = read_json(source_manifest_path)
            source_warnings = source_manifest.get("warnings") or []
            for index, warning in enumerate(source_warnings, start=1):
                rows.append(
                    {
                        "source_run_id": str(source_run_id),
                        "warning_index": index,
                        "warning": str(warning),
                    }
                )

        if rows:
            return rows

        warning_messages = self._list_value(collection_run.get("warning_messages") if collection_run else None)
        if not warning_messages:
            warning_messages = self._list_value(manifest.get("warnings"))
        return [
            {
                "source_run_id": run_id,
                "warning_index": index,
                "warning": str(warning),
            }
            for index, warning in enumerate(warning_messages, start=1)
        ]

    def _post_coverage_gaps(self, posts: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        required = {"post_id", "comments_count"}
        if posts.is_empty() or not required.issubset(set(posts.columns)):
            return []
        if comments.is_empty() or "parent_post_id" not in comments.columns:
            extracted = pl.DataFrame(
                {
                    "post_id": pl.Series([], dtype=pl.String),
                    "extracted_comment_count": pl.Series([], dtype=pl.Int64),
                }
            )
        else:
            extracted = comments.group_by("parent_post_id").len().rename(
                {"parent_post_id": "post_id", "len": "extracted_comment_count"}
            )
        selected_columns = [
            column
            for column in ["post_id", "comments_count", "permalink", "message", "container_source_id", "container_source_name"]
            if column in posts.columns
        ]
        frame = posts.select(selected_columns).join(extracted, on="post_id", how="left")
        frame = frame.with_columns(pl.col("extracted_comment_count").fill_null(0))
        frame = frame.with_columns((pl.col("comments_count") - pl.col("extracted_comment_count")).alias("comment_gap"))
        frame = frame.filter(pl.col("comment_gap") > 0).sort("comment_gap", descending=True).head(8)
        return [
            {
                "post_id": row.get("post_id"),
                "comment_gap": self._int_value(row.get("comment_gap"), 0),
                "comments_count": self._int_value(row.get("comments_count"), 0),
                "extracted_comment_count": self._int_value(row.get("extracted_comment_count"), 0),
                "permalink": row.get("permalink"),
                "container_source_id": row.get("container_source_id"),
                "container_source_name": row.get("container_source_name"),
                "message_excerpt": _truncate(row.get("message")),
            }
            for row in frame.to_dicts()
        ]

    def _propagation_coverage_gaps(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        required = {"propagation_id", "comments_count"}
        if propagations.is_empty() or not required.issubset(set(propagations.columns)):
            return []
        if comments.is_empty() or "parent_post_id" not in comments.columns:
            extracted = pl.DataFrame(
                {
                    "propagation_id": pl.Series([], dtype=pl.String),
                    "extracted_comment_count": pl.Series([], dtype=pl.Int64),
                }
            )
        else:
            extracted = comments.group_by("parent_post_id").len().rename(
                {"parent_post_id": "propagation_id", "len": "extracted_comment_count"}
            )
        selected_columns = [
            column
            for column in ["propagation_id", "comments_count", "permalink", "message", "container_source_id", "container_source_name"]
            if column in propagations.columns
        ]
        frame = propagations.select(selected_columns).join(extracted, on="propagation_id", how="left")
        frame = frame.with_columns(pl.col("extracted_comment_count").fill_null(0))
        frame = frame.with_columns((pl.col("comments_count") - pl.col("extracted_comment_count")).alias("comment_gap"))
        frame = frame.filter(pl.col("comment_gap") > 0).sort("comment_gap", descending=True).head(8)
        return [
            {
                "propagation_id": row.get("propagation_id"),
                "comment_gap": self._int_value(row.get("comment_gap"), 0),
                "comments_count": self._int_value(row.get("comments_count"), 0),
                "extracted_comment_count": self._int_value(row.get("extracted_comment_count"), 0),
                "permalink": row.get("permalink"),
                "container_source_id": row.get("container_source_id"),
                "container_source_name": row.get("container_source_name"),
                "message_excerpt": _truncate(row.get("message")),
            }
            for row in frame.to_dicts()
        ]

    def _build_person_monitor(
        self,
        source_kind: str,
        observed_sources: pl.DataFrame,
        match_hits: pl.DataFrame,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        if source_kind != "person_monitor":
            return {"enabled": False}
        return {
            "enabled": True,
            "observed_sources": _frame_records(observed_sources, limit=50),
            "match_breakdown": self._match_breakdown(match_hits),
            "top_matched_posts": self._top_matched_items(
                match_hits=match_hits,
                items=posts,
                item_types={"post", "propagation"},
                id_column="post_id",
            ),
            "top_matched_comments": self._top_matched_items(
                match_hits=match_hits,
                items=comments,
                item_types={"comment"},
                id_column="comment_id",
            ),
        }

    def _match_breakdown(self, match_hits: pl.DataFrame) -> dict[str, int]:
        if match_hits.is_empty() or "match_kind" not in match_hits.columns:
            return {}
        rows = match_hits.group_by("match_kind").len().sort("match_kind").to_dicts()
        return {str(row["match_kind"]): self._int_value(row["len"], 0) for row in rows}

    def _top_matched_items(
        self,
        match_hits: pl.DataFrame,
        items: pl.DataFrame,
        item_types: set[str],
        id_column: str,
    ) -> list[dict[str, Any]]:
        if match_hits.is_empty() or items.is_empty() or "item_type" not in match_hits.columns or "item_id" not in match_hits.columns:
            return []
        item_lookup = {str(row.get(id_column)): row for row in items.to_dicts() if row.get(id_column)}
        buckets: dict[str, dict[str, Any]] = {}
        for hit in match_hits.filter(pl.col("item_type").is_in(sorted(item_types))).to_dicts():
            item_id = str(hit.get("item_id") or "")
            if not item_id:
                continue
            bucket = buckets.setdefault(
                item_id,
                {
                    "item_id": item_id,
                    "match_count": 0,
                    "match_kinds": set(),
                    "matched_values": set(),
                    "container_source_id": hit.get("container_source_id"),
                },
            )
            bucket["match_count"] += 1
            if hit.get("match_kind"):
                bucket["match_kinds"].add(str(hit["match_kind"]))
            if hit.get("matched_value"):
                bucket["matched_values"].add(str(hit["matched_value"]))

        rows: list[dict[str, Any]] = []
        for item_id, bucket in buckets.items():
            item = item_lookup.get(item_id, {})
            rows.append(
                {
                    "item_id": item_id,
                    "match_count": bucket["match_count"],
                    "match_kinds": sorted(bucket["match_kinds"]),
                    "matched_values": sorted(bucket["matched_values"]),
                    "container_source_id": item.get("container_source_id") or bucket.get("container_source_id"),
                    "container_source_name": item.get("container_source_name"),
                    "permalink": item.get("permalink"),
                    "message_excerpt": _truncate(item.get("message") or item.get("raw_text")),
                }
            )
        return sorted(rows, key=lambda row: (-row["match_count"], str(row["item_id"])))[:20]

    def _next_actions(
        self,
        status: str,
        warnings: list[dict[str, Any]],
        coverage_gaps: list[dict[str, Any]],
        propagation_coverage_gaps: list[dict[str, Any]],
        person_monitor: dict[str, Any],
    ) -> list[str]:
        actions: list[str] = []
        warning_text = " ".join(str(row.get("warning", "")).lower() for row in warnings)
        if status not in {"success", "completed"}:
            actions.append("Treat this run as partial until the collector status is success.")
        if warnings:
            actions.append("Review collection warnings before using the run as evidence.")
        if "login" in warning_text or "signup" in warning_text:
            actions.append("Run the relevant doctor command with a logged-in browser profile before deeper selector tuning.")
        if "unsupported" in warning_text and person_monitor.get("enabled"):
            actions.append("Use watchlist discovery for unsupported search surfaces.")
        if coverage_gaps:
            actions.append("Inspect posts with comment gaps and rerun with authenticated or deeper detail extraction where available.")
        if propagation_coverage_gaps:
            actions.append("Inspect propagation comment gaps before making completeness claims.")
        if person_monitor.get("enabled") and not person_monitor.get("observed_sources"):
            actions.append("Add explicit watchlist surfaces or working search queries for person_monitor coverage.")
        if person_monitor.get("enabled") and not person_monitor.get("match_breakdown"):
            actions.append("Verify profile URL, handle, source_id and exact aliases before treating zero matches as absence.")
        if not actions:
            actions.append("No immediate OpenClaw action is required; the bundle is ready for review.")
        return actions

    def _brief_markdown(self, bundle: dict[str, Any]) -> str:
        counts = bundle["counts"]
        warnings = bundle["warnings"]
        next_actions = bundle["next_actions"]
        source = bundle["source"]
        lines = [
            f"# OpenClaw Brief: {bundle['run_id']}",
            "",
            f"- Status: {bundle['status']}",
            f"- Source: {source.get('platform')} / {source.get('source_kind')} / {source.get('source_name') or source.get('source_id')}",
            (
                "- Counts: "
                f"posts={counts['posts']}, comments={counts['comments']}, propagations={counts['propagations']}, "
                f"match_hits={counts['match_hits']}, observed_sources={counts['observed_sources']}, warnings={counts['warnings']}"
            ),
            "",
            "## Warnings",
        ]
        if warnings:
            lines.extend(
                f"- [{row.get('source_run_id')} #{row.get('warning_index')}] {row.get('warning')}" for row in warnings[:10]
            )
        else:
            lines.append("- None")
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in next_actions)
        lines.append("")
        return "\n".join(lines)

    def _build_history_artifacts(self, history_run_id: str, bundle_path: Path, brief_path: Path) -> dict[str, Any]:
        history_manifest = self.paths.raw_root / "_history" / history_run_id / "manifest.json"
        report_dir = self.paths.reports_root / "history" / history_run_id
        report_markdown = report_dir / "history_report.md"
        report_html = report_dir / "history_report.html"
        tables_dir = report_dir / "tables"
        csv_exports: dict[str, str] = {}
        if tables_dir.exists():
            csv_exports = {path.stem: self._path_string(path) for path in sorted(tables_dir.glob("*.csv"))}
        return {
            "history_manifest": self._path_string(history_manifest) if history_manifest.exists() else None,
            "processed_dir": self._path_string(self.paths.processed_root),
            "duckdb": self._path_string(self.paths.database_path),
            "history_report_markdown": self._path_string(report_markdown) if report_markdown.exists() else None,
            "history_report_html": self._path_string(report_html) if report_html.exists() else None,
            "history_tabular_dir": self._path_string(tables_dir) if tables_dir.exists() else None,
            "csv_exports": csv_exports,
            "openclaw_bundle": self._path_string(bundle_path),
            "openclaw_brief": self._path_string(brief_path),
        }

    def _top_history_stance_shifts(self, temporal_metrics: pl.DataFrame) -> list[dict[str, Any]]:
        return history_stance_turning_points(temporal_metrics, limit=20)

    def _history_next_actions(
        self,
        run_row: dict[str, Any],
        windows: pl.DataFrame,
        coverage_gaps: pl.DataFrame,
    ) -> list[str]:
        actions: list[str] = []
        if run_row.get("status") != "success":
            actions.append("Treat this historical run as partial until failed or partial windows are reviewed.")
        if not windows.is_empty() and "status" in windows.columns and windows.filter(pl.col("status") == "failed").height:
            actions.append("Rerun failed monthly windows with history.resume=true after fixing collector errors.")
        if not coverage_gaps.is_empty():
            actions.append("Inspect months with visible-vs-extracted comment gaps before making completeness claims.")
        if not actions:
            actions.append("Historical bundle is ready for OpenClaw review.")
        return actions

    def _history_brief_markdown(self, bundle: dict[str, Any]) -> str:
        counts = bundle["counts"]
        lines = [
            f"# OpenClaw History Brief: {bundle['history_run_id']}",
            "",
            f"- Status: {bundle['status']}",
            f"- Source: {bundle['platform']} / {bundle['source_kind']} / {bundle['source'].get('source_name')}",
            (
                "- Counts: "
                f"windows={counts['windows']}, posts={counts['posts']}, comments={counts['comments']}, "
                f"coverage_gaps={counts['coverage_gaps']}, warnings={counts['warnings']}"
            ),
            "",
            "## Next Actions",
        ]
        lines.extend(f"- {action}" for action in bundle["next_actions"])
        lines.append("")
        return "\n".join(lines)

    def _path_string(self, path: Path) -> str:
        return str(path.resolve())

    def _string_value(self, value: Any, default: Any = "") -> str:
        if value is None:
            return str(default or "")
        return str(value)

    def _int_value(self, value: Any, default: int) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _list_value(self, value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        return [value]


def _frame_records(frame: pl.DataFrame, limit: int) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    return _json_safe(frame.head(limit).to_dicts())


def _truncate(value: Any, limit: int = 280) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value
