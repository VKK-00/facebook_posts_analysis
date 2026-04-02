from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import markdown
import polars as pl
from jinja2 import Environment, FileSystemLoader, select_autoescape

from facebook_posts_analysis.config import ProjectConfig
from facebook_posts_analysis.paths import ProjectPaths
from facebook_posts_analysis.utils import read_json

from facebook_posts_analysis.analysis.metrics import compute_support_metrics


class ReviewExportService:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> list[Path]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No analysis run available for review export.")

        clusters = self._load_table("narrative_clusters").filter(pl.col("run_id") == resolved_run_id)
        stance_labels = self._load_table("stance_labels").filter(pl.col("run_id") == resolved_run_id)

        narrative_path = self.paths.review_root / "narrative_overrides.csv"
        stance_path = self.paths.review_root / "stance_overrides.csv"
        snapshot_path = self.paths.review_root / "current_analysis_snapshot.json"

        narrative_export = clusters.select(
            "item_type",
            "cluster_id",
            pl.col("label").alias("current_label"),
            pl.col("description").alias("current_description"),
        ).with_columns(
            pl.lit("").alias("action"),
            pl.lit("").alias("target_cluster_id"),
            pl.lit("").alias("new_label"),
            pl.lit("").alias("new_description"),
        )
        stance_export = stance_labels.select(
            "item_type",
            "item_id",
            "side_id",
            pl.col("label").alias("current_label"),
            "confidence",
        ).with_columns(
            pl.lit("").alias("override_label"),
            pl.lit("").alias("override_confidence"),
            pl.lit("").alias("note"),
        )

        narrative_export = _merge_existing_export(
            narrative_path,
            narrative_export,
            keys=["item_type", "cluster_id"],
            editable_columns=["action", "target_cluster_id", "new_label", "new_description"],
        )
        stance_export = _merge_existing_export(
            stance_path,
            stance_export,
            keys=["item_type", "item_id", "side_id"],
            editable_columns=["override_label", "override_confidence", "note"],
        )

        narrative_export.write_csv(narrative_path)
        stance_export.write_csv(stance_path)
        snapshot_path.write_text(
            json.dumps(
                {
                    "run_id": resolved_run_id,
                    "cluster_count": clusters.height,
                    "stance_labels_count": stance_labels.height,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return [narrative_path, stance_path, snapshot_path]

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()


class ReportService:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths
        template_root = Path(__file__).resolve().parent.parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(template_root.as_posix()),
            autoescape=select_autoescape(["html"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def run(self, run_id: str | None = None) -> list[Path]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No analysis run available for reporting.")

        context = self._build_context(resolved_run_id)
        markdown_template = self.env.get_template("report.md.j2")
        html_template = self.env.get_template("report.html.j2")

        markdown_text = markdown_template.render(**context)
        html_body = markdown.markdown(markdown_text, extensions=["tables", "fenced_code"])
        html_text = html_template.render(title=context["title"], body=html_body)

        markdown_path = self.paths.reports_root / f"report_{resolved_run_id}.md"
        html_path = self.paths.reports_root / f"report_{resolved_run_id}.html"
        markdown_path.write_text(markdown_text, encoding="utf-8")
        html_path.write_text(html_text, encoding="utf-8")
        return [markdown_path, html_path]

    def _build_context(self, run_id: str) -> dict[str, Any]:
        posts = self._load_table("posts").filter(pl.col("run_id") == run_id)
        comments = self._load_table("comments").filter(pl.col("run_id") == run_id)
        languages = self._load_table("detected_languages").filter(pl.col("run_id") == run_id)
        clusters = self._load_table("narrative_clusters").filter(pl.col("run_id") == run_id)
        memberships = self._load_table("cluster_memberships").filter(pl.col("run_id") == run_id)
        stance_labels = self._load_table("stance_labels").filter(pl.col("run_id") == run_id)
        analysis_runs = self._load_table("analysis_runs").filter(pl.col("run_id") == run_id)
        collection_runs = self._load_table("collection_runs").filter(pl.col("run_id") == run_id)

        clusters, memberships = self._apply_narrative_overrides(clusters, memberships)
        stance_labels = self._apply_stance_overrides(stance_labels)
        comment_memberships = memberships.filter(pl.col("item_type") == "comment")
        support_metrics = compute_support_metrics(stance_labels, comment_memberships, run_id)

        if memberships.is_empty():
            cluster_counts = pl.DataFrame(schema={"item_type": pl.String, "cluster_id": pl.String, "member_count": pl.Int64})
        else:
            cluster_counts = memberships.group_by(["item_type", "cluster_id"]).agg(pl.len().alias("member_count"))
        clusters_with_counts = (
            clusters.join(cluster_counts, on=["item_type", "cluster_id"], how="left").with_columns(
                pl.col("member_count").fill_null(0)
            )
            if not clusters.is_empty()
            else pl.DataFrame()
        )

        language_mix = (
            languages.group_by(["item_type", "language"])
            .agg(pl.len().alias("count"))
            .sort(["item_type", "count"], descending=[False, True])
            .to_dicts()
            if not languages.is_empty()
            else []
        )

        global_support = (
            support_metrics.filter(pl.col("scope_type") == "global").sort("support_count", descending=True).to_dicts()
            if not support_metrics.is_empty()
            else []
        )
        post_cluster_rows = (
            clusters_with_counts.filter(pl.col("item_type") == "post").sort("member_count", descending=True).to_dicts()
            if not clusters_with_counts.is_empty()
            else []
        )
        comment_cluster_rows = (
            clusters_with_counts.filter(pl.col("item_type") == "comment").sort("member_count", descending=True).to_dicts()
            if not clusters_with_counts.is_empty()
            else []
        )

        comments_lookup = {row["comment_id"]: row for row in comments.to_dicts()}
        posts_lookup = {row["post_id"]: row for row in posts.to_dicts()}
        exemplar_quotes = []
        for cluster in comment_cluster_rows[:8]:
            for exemplar_id in cluster.get("exemplar_ids", [])[:2]:
                if exemplar_id in comments_lookup:
                    exemplar_quotes.append(
                        {
                            "cluster_id": cluster["cluster_id"],
                            "label": cluster["label"],
                            "text": comments_lookup[exemplar_id].get("message", "")[:280],
                        }
                    )

        conflict_rows = self._high_conflict_threads(stance_labels, comments, posts_lookup)
        coverage_gaps = self._coverage_gaps(posts, comments)
        top_supportive_comments = self._top_comments_by_stance(
            stance_labels=stance_labels,
            comments=comments,
            target_label="support",
        )
        top_critical_comments = self._top_comments_by_stance(
            stance_labels=stance_labels,
            comments=comments,
            target_label="oppose",
        )
        reply_depth_summary = self._reply_depth_summary(comments)
        warnings = self._data_quality_warnings(run_id, posts, comments, analysis_runs, collection_runs)

        page_name = collection_runs["page_name"][0] if collection_runs.height else self.config.page.page_name or "Facebook Page"
        source_run_ids: list[str] = []
        if collection_runs.height and "source_run_ids" in collection_runs.columns:
            raw_source_run_ids = collection_runs["source_run_ids"][0]
            if isinstance(raw_source_run_ids, pl.Series):
                source_run_ids = raw_source_run_ids.to_list()
            else:
                source_run_ids = list(raw_source_run_ids or [])
        source_run_count = int(collection_runs["source_run_count"][0]) if collection_runs.height and "source_run_count" in collection_runs.columns else 1
        return {
            "title": f"Narrative analysis report: {page_name}",
            "run_id": run_id,
            "page_name": page_name,
            "post_count": posts.height,
            "comment_count": comments.height,
            "source_run_ids": source_run_ids or [run_id],
            "source_run_count": source_run_count,
            "providers": analysis_runs.to_dicts()[0] if analysis_runs.height else {},
            "global_support": global_support,
            "post_clusters": post_cluster_rows,
            "comment_clusters": comment_cluster_rows,
            "language_mix": language_mix,
            "exemplar_quotes": exemplar_quotes,
            "high_conflict_threads": conflict_rows,
            "coverage_gaps": coverage_gaps,
            "top_supportive_comments": top_supportive_comments,
            "top_critical_comments": top_critical_comments,
            "reply_depth_summary": reply_depth_summary,
            "warnings": warnings,
        }

    def _apply_narrative_overrides(
        self,
        clusters: pl.DataFrame,
        memberships: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        overrides_path = self.paths.review_root / "narrative_overrides.csv"
        if not overrides_path.exists() or clusters.is_empty():
            return clusters, memberships

        overrides = pl.read_csv(overrides_path)
        mapping: dict[tuple[str, str], str] = {}
        label_updates: dict[tuple[str, str], str] = {}
        description_updates: dict[tuple[str, str], str] = {}
        for row in overrides.to_dicts():
            key = (row.get("item_type", ""), row.get("cluster_id", ""))
            action = (row.get("action") or "").strip().lower()
            target_cluster = (row.get("target_cluster_id") or "").strip()
            if action == "merge" and target_cluster:
                mapping[key] = target_cluster
            new_label = (row.get("new_label") or "").strip()
            new_description = (row.get("new_description") or "").strip()
            if new_label:
                label_updates[(row.get("item_type", ""), target_cluster or row.get("cluster_id", ""))] = new_label
            if new_description:
                description_updates[(row.get("item_type", ""), target_cluster or row.get("cluster_id", ""))] = new_description

        if mapping:
            memberships = memberships.with_columns(
                pl.struct(["item_type", "cluster_id"]).map_elements(
                    lambda row: mapping.get((row["item_type"], row["cluster_id"]), row["cluster_id"]),
                    return_dtype=pl.String,
                ).alias("cluster_id")
            )
            clusters = clusters.with_columns(
                pl.struct(["item_type", "cluster_id"]).map_elements(
                    lambda row: mapping.get((row["item_type"], row["cluster_id"]), row["cluster_id"]),
                    return_dtype=pl.String,
                ).alias("cluster_id")
            )
            clusters = (
                clusters.group_by(["item_type", "cluster_id", "run_id"])
                .agg(
                    pl.col("label").first().alias("label"),
                    pl.col("description").first().alias("description"),
                    pl.col("top_keywords").first().alias("top_keywords"),
                    pl.col("exemplar_ids").first().alias("exemplar_ids"),
                )
            )

        if label_updates:
            clusters = clusters.with_columns(
                pl.struct(["item_type", "cluster_id", "label"]).map_elements(
                    lambda row: label_updates.get((row["item_type"], row["cluster_id"]), row["label"]),
                    return_dtype=pl.String,
                ).alias("label")
            )
        if description_updates:
            clusters = clusters.with_columns(
                pl.struct(["item_type", "cluster_id", "description"]).map_elements(
                    lambda row: description_updates.get((row["item_type"], row["cluster_id"]), row["description"]),
                    return_dtype=pl.String,
                ).alias("description")
            )
        return clusters, memberships

    def _apply_stance_overrides(self, stance_labels: pl.DataFrame) -> pl.DataFrame:
        overrides_path = self.paths.review_root / "stance_overrides.csv"
        if not overrides_path.exists() or stance_labels.is_empty():
            return stance_labels
        overrides = pl.read_csv(overrides_path)
        label_map: dict[tuple[str, str, str], str] = {}
        confidence_map: dict[tuple[str, str, str], float] = {}
        for row in overrides.to_dicts():
            key = (row.get("item_type", ""), row.get("item_id", ""), row.get("side_id", ""))
            override_label = (row.get("override_label") or "").strip()
            override_confidence = (row.get("override_confidence") or "").strip()
            if override_label:
                label_map[key] = override_label
            if override_confidence:
                try:
                    confidence_map[key] = float(override_confidence)
                except ValueError:
                    continue

        if label_map:
            stance_labels = stance_labels.with_columns(
                pl.struct(["item_type", "item_id", "side_id", "label"]).map_elements(
                    lambda row: label_map.get((row["item_type"], row["item_id"], row["side_id"]), row["label"]),
                    return_dtype=pl.String,
                ).alias("label")
            )
        if confidence_map:
            stance_labels = stance_labels.with_columns(
                pl.struct(["item_type", "item_id", "side_id", "confidence"]).map_elements(
                    lambda row: confidence_map.get((row["item_type"], row["item_id"], row["side_id"]), row["confidence"]),
                    return_dtype=pl.Float64,
                ).alias("confidence")
            )
        return stance_labels

    def _high_conflict_threads(
        self,
        stance_labels: pl.DataFrame,
        comments: pl.DataFrame,
        posts_lookup: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if stance_labels.is_empty() or comments.is_empty():
            return []
        joined = stance_labels.filter(pl.col("item_type") == "comment").join(
            comments.select("comment_id", "parent_post_id"),
            left_on="item_id",
            right_on="comment_id",
            how="left",
        )
        grouped = (
            joined.group_by(["parent_post_id", "side_id"])
            .agg(
                (pl.col("label") == "support").sum().alias("support_count"),
                (pl.col("label") == "oppose").sum().alias("oppose_count"),
            )
            .with_columns(pl.min_horizontal("support_count", "oppose_count").alias("conflict_score"))
            .sort("conflict_score", descending=True)
            .head(8)
        )
        rows = []
        for row in grouped.to_dicts():
            post = posts_lookup.get(row["parent_post_id"], {})
            rows.append(
                {
                    "post_id": row["parent_post_id"],
                    "side_id": row["side_id"],
                    "support_count": row["support_count"],
                    "oppose_count": row["oppose_count"],
                    "conflict_score": row["conflict_score"],
                    "post_excerpt": (post.get("message") or "")[:220],
                }
            )
        return rows

    def _coverage_gaps(self, posts: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        if posts.is_empty():
            return []
        extracted_counts = (
            comments.group_by("parent_post_id").agg(pl.len().alias("extracted_comment_count"))
            if not comments.is_empty()
            else pl.DataFrame(schema={"parent_post_id": pl.String, "extracted_comment_count": pl.Int64})
        )
        joined = (
            posts.select("post_id", "message", "comments_count", "permalink")
            .join(extracted_counts, left_on="post_id", right_on="parent_post_id", how="left")
            .with_columns(pl.col("extracted_comment_count").fill_null(0))
            .with_columns((pl.col("comments_count") - pl.col("extracted_comment_count")).alias("comment_gap"))
            .filter(pl.col("comment_gap") > 0)
            .sort("comment_gap", descending=True)
            .head(8)
        )
        rows = joined.to_dicts()
        for row in rows:
            row["post_excerpt"] = (row.get("message") or "")[:220]
        return rows

    def _top_comments_by_stance(
        self,
        *,
        stance_labels: pl.DataFrame,
        comments: pl.DataFrame,
        target_label: str,
    ) -> list[dict[str, Any]]:
        if stance_labels.is_empty() or comments.is_empty():
            return []
        ranked = (
            stance_labels.filter((pl.col("item_type") == "comment") & (pl.col("label") == target_label))
            .join(
                comments.select("comment_id", "message", "parent_post_id", "permalink", "depth", "author_id"),
                left_on="item_id",
                right_on="comment_id",
                how="inner",
            )
            .sort(["confidence", "depth"], descending=[True, False])
            .head(8)
        )
        return [
            {
                "comment_id": row["item_id"],
                "side_id": row["side_id"],
                "confidence": row["confidence"],
                "depth": row["depth"],
                "parent_post_id": row["parent_post_id"],
                "permalink": row["permalink"],
                "text": (row.get("message") or "")[:280],
            }
            for row in ranked.to_dicts()
        ]

    def _reply_depth_summary(self, comments: pl.DataFrame) -> list[dict[str, Any]]:
        if comments.is_empty():
            return []
        return (
            comments.group_by("depth")
            .agg(pl.len().alias("count"))
            .sort("depth")
            .to_dicts()
        )

    def _data_quality_warnings(
        self,
        run_id: str,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        analysis_runs: pl.DataFrame,
        collection_runs: pl.DataFrame,
    ) -> list[str]:
        warnings: list[str] = []
        manifest_path = self.paths.run_raw_dir(run_id) / "manifest.json"
        if manifest_path.exists():
            manifest = read_json(manifest_path)
            warnings.extend(manifest.get("warnings", []))
        if collection_runs.height and bool(collection_runs["fallback_used"][0]):
            warnings.append("Collector fallback was used for this run.")
        if collection_runs.height and "source_run_count" in collection_runs.columns and int(collection_runs["source_run_count"][0]) > 1:
            warnings.append(
                f"Normalized snapshot merged {int(collection_runs['source_run_count'][0])} recent collection runs."
            )
        if comments.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some comments were collected without message text.")
        if posts.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some posts were collected without message text.")
        if analysis_runs.height:
            llm_provider = analysis_runs["llm_provider"][0]
            if llm_provider == "heuristic_llm":
                warnings.append("Stance and cluster descriptions were generated with heuristic fallbacks.")
        return list(dict.fromkeys(warnings))

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()


def _merge_existing_export(path: Path, current: pl.DataFrame, keys: list[str], editable_columns: list[str]) -> pl.DataFrame:
    if not path.exists():
        return current
    existing = pl.read_csv(path)
    editable_columns = [column for column in editable_columns if column in existing.columns and column in current.columns]
    if not editable_columns:
        return current
    merged = current.join(existing.select([*keys, *editable_columns]), on=keys, how="left", suffix="_existing")
    for column in editable_columns:
        if f"{column}_existing" in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(f"{column}_existing").is_not_null() & (pl.col(f"{column}_existing") != ""))
                .then(pl.col(f"{column}_existing"))
                .otherwise(pl.col(column))
                .alias(column)
            ).drop(f"{column}_existing")
    return merged
