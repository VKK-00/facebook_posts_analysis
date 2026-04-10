from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import markdown
import polars as pl
from jinja2 import Environment, FileSystemLoader, select_autoescape
from openpyxl import Workbook

from social_posts_analysis.analysis.metrics import compute_support_metrics
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.utils import read_json


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
        tabular_paths = self._write_tabular_exports(resolved_run_id, context["export_tables"])
        return [markdown_path, html_path, *tabular_paths]

    def run_tabular(self, run_id: str | None = None) -> list[Path]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No analysis run available for tabular export.")
        context = self._build_context(resolved_run_id)
        return self._write_tabular_exports(resolved_run_id, context["export_tables"])

    def _build_context(self, run_id: str) -> dict[str, Any]:
        posts = self._load_table("posts").filter(pl.col("run_id") == run_id)
        propagations = self._load_table("propagations").filter(pl.col("run_id") == run_id)
        comments = self._load_table("comments").filter(pl.col("run_id") == run_id)
        origin_posts = (
            posts.filter(~pl.col("is_propagation").fill_null(False))
            if not posts.is_empty() and "is_propagation" in posts.columns
            else posts
        )
        languages = self._load_table("detected_languages").filter(pl.col("run_id") == run_id)
        clusters = self._load_table("narrative_clusters").filter(pl.col("run_id") == run_id)
        memberships = self._load_table("cluster_memberships").filter(pl.col("run_id") == run_id)
        stance_labels = self._load_table("stance_labels").filter(pl.col("run_id") == run_id)
        analysis_runs = self._load_table("analysis_runs").filter(pl.col("run_id") == run_id)
        collection_runs = self._load_table("collection_runs").filter(pl.col("run_id") == run_id)

        clusters, memberships = self._apply_narrative_overrides(clusters, memberships)
        stance_labels = self._apply_stance_overrides(stance_labels)
        comment_memberships = memberships.filter(pl.col("item_type") == "comment")
        support_metrics = compute_support_metrics(stance_labels, comment_memberships, comments, run_id)

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
        propagation_cluster_rows = (
            clusters_with_counts.filter(pl.col("item_type") == "propagation").sort("member_count", descending=True).to_dicts()
            if not clusters_with_counts.is_empty()
            else []
        )
        comment_cluster_rows = (
            clusters_with_counts.filter(pl.col("item_type") == "comment").sort("member_count", descending=True).to_dicts()
            if not clusters_with_counts.is_empty()
            else []
        )

        comments_lookup = {row["comment_id"]: row for row in comments.to_dicts()}
        posts_lookup = {row["post_id"]: row for row in origin_posts.to_dicts()}
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
        coverage_gaps = self._coverage_gaps(origin_posts, comments)
        propagation_coverage_gaps = self._propagation_coverage_gaps(propagations, comments)
        post_overview = self._post_overview(origin_posts, comments)
        propagation_overview = self._propagation_overview(propagations, comments)
        top_propagated_items = self._top_propagated_items(origin_posts, propagations)
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

        platform = collection_runs["platform"][0] if collection_runs.height and "platform" in collection_runs.columns else self.config.source.platform
        source_name = (
            collection_runs["source_name"][0]
            if collection_runs.height and "source_name" in collection_runs.columns
            else self.config.source.source_name or self.config.source.source_id or "Source"
        )
        source_id = (
            collection_runs["source_id"][0]
            if collection_runs.height and "source_id" in collection_runs.columns
            else self.config.source.source_id or ""
        )
        source_type = (
            collection_runs["source_type"][0]
            if collection_runs.height and "source_type" in collection_runs.columns
            else ("channel" if platform == "telegram" else "account" if platform in {"x", "threads", "instagram"} else "page")
        )
        source_run_ids: list[str] = []
        if collection_runs.height and "source_run_ids" in collection_runs.columns:
            raw_source_run_ids = collection_runs["source_run_ids"][0]
            if isinstance(raw_source_run_ids, pl.Series):
                source_run_ids = raw_source_run_ids.to_list()
            else:
                source_run_ids = list(raw_source_run_ids or [])
        source_run_count = int(collection_runs["source_run_count"][0]) if collection_runs.height and "source_run_count" in collection_runs.columns else 1
        origin_support = (
            support_metrics.filter(pl.col("scope_type") == "origin_post").sort("support_count", descending=True).to_dicts()
            if not support_metrics.is_empty()
            else []
        )
        origin_plus_support = (
            support_metrics.filter(pl.col("scope_type") == "origin_plus_propagations").sort("support_count", descending=True).to_dicts()
            if not support_metrics.is_empty()
            else []
        )
        propagation_support = (
            support_metrics.filter(pl.col("scope_type") == "propagation").sort("support_count", descending=True).to_dicts()
            if not support_metrics.is_empty()
            else []
        )
        export_tables = {
            "per_side_support": self._rows_to_frame(global_support),
            "per_origin_support": self._rows_to_frame(origin_support),
            "per_origin_plus_propagations": self._rows_to_frame(origin_plus_support),
            "per_propagation_support": self._rows_to_frame(propagation_support),
            "per_post_overview": post_overview,
            "per_propagation_overview": propagation_overview,
            "per_thread_conflict": self._rows_to_frame(conflict_rows),
            "post_narratives": self._sanitize_export_frame(self._rows_to_frame(post_cluster_rows)),
            "propagation_narratives": self._sanitize_export_frame(self._rows_to_frame(propagation_cluster_rows)),
            "comment_narratives": self._sanitize_export_frame(self._rows_to_frame(comment_cluster_rows)),
            "language_mix": self._rows_to_frame(language_mix),
            "top_supportive_comments": self._rows_to_frame(top_supportive_comments),
            "top_critical_comments": self._rows_to_frame(top_critical_comments),
            "coverage_gaps": self._rows_to_frame(coverage_gaps),
            "propagation_coverage_gaps": self._rows_to_frame(propagation_coverage_gaps),
            "top_propagated_items": self._rows_to_frame(top_propagated_items),
        }
        telegram_summary = self._telegram_summary(origin_posts, comments, collection_runs) if platform == "telegram" else None
        x_summary = self._x_summary(origin_posts, comments) if platform == "x" else None
        threads_summary = self._threads_summary(origin_posts, comments) if platform == "threads" else None
        instagram_summary = self._instagram_summary(origin_posts, comments) if platform == "instagram" else None
        propagation_summary = self._propagation_summary(propagations, comments)
        return {
            "title": f"Narrative analysis report: {source_name}",
            "run_id": run_id,
            "platform": platform,
            "source_name": source_name,
            "source_id": source_id,
            "source_type": source_type,
            "post_count": origin_posts.height,
            "propagation_count": propagations.height,
            "comment_count": comments.height,
            "source_run_ids": source_run_ids or [run_id],
            "source_run_count": source_run_count,
            "providers": analysis_runs.to_dicts()[0] if analysis_runs.height else {},
            "global_support": global_support,
            "origin_support": origin_support,
            "origin_plus_support": origin_plus_support,
            "propagation_support": propagation_support,
            "post_clusters": post_cluster_rows,
            "propagation_clusters": propagation_cluster_rows,
            "comment_clusters": comment_cluster_rows,
            "language_mix": language_mix,
            "exemplar_quotes": exemplar_quotes,
            "high_conflict_threads": conflict_rows,
            "coverage_gaps": coverage_gaps,
            "propagation_coverage_gaps": propagation_coverage_gaps,
            "top_propagated_items": top_propagated_items,
            "top_supportive_comments": top_supportive_comments,
            "top_critical_comments": top_critical_comments,
            "reply_depth_summary": reply_depth_summary,
            "warnings": warnings,
            "telegram_summary": telegram_summary,
            "x_summary": x_summary,
            "threads_summary": threads_summary,
            "instagram_summary": instagram_summary,
            "propagation_summary": propagation_summary,
            "export_tables": export_tables,
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

    def _propagation_coverage_gaps(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        if propagations.is_empty():
            return []
        extracted_counts = (
            comments.filter(pl.col("parent_entity_type") == "propagation")
            .group_by("parent_entity_id")
            .agg(pl.len().alias("extracted_comment_count"))
            if not comments.is_empty()
            else pl.DataFrame(schema={"parent_entity_id": pl.String, "extracted_comment_count": pl.Int64})
        )
        joined = (
            propagations.select("propagation_id", "message", "comments_count", "permalink", "propagation_kind", "origin_post_id")
            .join(extracted_counts, left_on="propagation_id", right_on="parent_entity_id", how="left")
            .with_columns(pl.col("extracted_comment_count").fill_null(0))
            .with_columns((pl.col("comments_count") - pl.col("extracted_comment_count")).alias("comment_gap"))
            .filter(pl.col("comment_gap") > 0)
            .sort("comment_gap", descending=True)
            .head(8)
        )
        rows = joined.to_dicts()
        for row in rows:
            row["propagation_excerpt"] = (row.get("message") or "")[:220]
        return rows

    def _post_overview(self, posts: pl.DataFrame, comments: pl.DataFrame) -> pl.DataFrame:
        if posts.is_empty():
            return pl.DataFrame()
        extracted_counts = (
            comments.group_by("parent_post_id").agg(pl.len().alias("extracted_comment_count"))
            if not comments.is_empty()
            else pl.DataFrame(schema={"parent_post_id": pl.String, "extracted_comment_count": pl.Int64})
        )
        return (
            posts.select(
                "post_id",
                "created_at",
                "permalink",
                "reactions",
                "shares",
                "comments_count",
                "views",
                "forwards",
                "reply_count",
                "has_media",
                "media_type",
                pl.col("message").fill_null("").str.slice(0, 220).alias("post_excerpt"),
            )
            .join(extracted_counts, left_on="post_id", right_on="parent_post_id", how="left")
            .with_columns(
                pl.col("extracted_comment_count").fill_null(0),
                (pl.col("comments_count") - pl.col("extracted_comment_count").fill_null(0)).alias("comment_gap"),
            )
            .sort("created_at", descending=True)
        )

    def _propagation_overview(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> pl.DataFrame:
        if propagations.is_empty():
            return pl.DataFrame()
        extracted_counts = (
            comments.filter(pl.col("parent_entity_type") == "propagation")
            .group_by("parent_entity_id")
            .agg(pl.len().alias("extracted_comment_count"))
            if not comments.is_empty()
            else pl.DataFrame(schema={"parent_entity_id": pl.String, "extracted_comment_count": pl.Int64})
        )
        return (
            propagations.select(
                "propagation_id",
                "origin_post_id",
                "origin_external_id",
                "propagation_kind",
                "created_at",
                "permalink",
                "reactions",
                "shares",
                "comments_count",
                "views",
                "forwards",
                "reply_count",
                "has_media",
                "media_type",
                pl.col("message").fill_null("").str.slice(0, 220).alias("propagation_excerpt"),
            )
            .join(extracted_counts, left_on="propagation_id", right_on="parent_entity_id", how="left")
            .with_columns(
                pl.col("extracted_comment_count").fill_null(0),
                (pl.col("comments_count") - pl.col("extracted_comment_count").fill_null(0)).alias("comment_gap"),
            )
            .sort("created_at", descending=True)
        )

    def _telegram_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        collection_runs: pl.DataFrame,
    ) -> dict[str, Any]:
        discussion_linked = (
            bool(collection_runs["discussion_linked"][0])
            if collection_runs.height and "discussion_linked" in collection_runs.columns
            else False
        )
        filtered_service_message_count = (
            int(collection_runs["filtered_service_message_count"][0])
            if collection_runs.height and "filtered_service_message_count" in collection_runs.columns
            else 0
        )
        return {
            "discussion_linked": discussion_linked,
            "filtered_service_message_count": filtered_service_message_count,
            "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
            "total_forwards": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
            "total_reply_count": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
            "reaction_breakdown": self._reaction_breakdown_summary(posts, comments),
        }

    def _x_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return {
            "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
            "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
            "total_reposts": int(posts["shares"].fill_null(0).sum()) if "shares" in posts.columns and posts.height else 0,
            "total_quotes": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
            "total_replies": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
            "reaction_breakdown": self._reaction_breakdown_summary(posts, comments),
        }

    def _threads_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return {
            "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
            "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
            "total_reposts": int(posts["shares"].fill_null(0).sum()) if "shares" in posts.columns and posts.height else 0,
            "total_quotes": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
            "total_replies": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
            "reaction_breakdown": self._reaction_breakdown_summary(posts, comments),
        }

    def _instagram_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return {
            "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
            "total_comments_visible": int(posts["comments_count"].fill_null(0).sum())
            if "comments_count" in posts.columns and posts.height
            else 0,
            "total_comments_extracted": comments.height,
            "reels_count": int(posts.filter(pl.col("media_type").fill_null("").str.to_lowercase() == "reel").height)
            if "media_type" in posts.columns
            else 0,
            "reaction_breakdown": self._reaction_breakdown_summary(posts, comments),
        }

    def _propagation_summary(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any] | None:
        if propagations.is_empty():
            return None
        comment_counts = (
            comments.filter(pl.col("parent_entity_type") == "propagation").group_by("parent_entity_id").agg(pl.len().alias("count"))
            if not comments.is_empty()
            else pl.DataFrame(schema={"parent_entity_id": pl.String, "count": pl.Int64})
        )
        kind_counts = (
            propagations.group_by("propagation_kind").agg(pl.len().alias("count")).sort("count", descending=True).to_dicts()
            if "propagation_kind" in propagations.columns
            else []
        )
        return {
            "total_instances": propagations.height,
            "with_comments": int(comment_counts.height),
            "kinds": kind_counts,
        }

    def _top_propagated_items(self, posts: pl.DataFrame, propagations: pl.DataFrame) -> list[dict[str, Any]]:
        if propagations.is_empty():
            return []
        counts = (
            propagations.filter(pl.col("origin_post_id").is_not_null() & (pl.col("origin_post_id") != ""))
            .group_by("origin_post_id")
            .agg(
                pl.len().alias("propagation_count"),
                pl.col("comments_count").fill_null(0).sum().alias("propagation_comment_count"),
            )
            .sort("propagation_count", descending=True)
            .head(10)
        )
        post_lookup = {row["post_id"]: row for row in posts.to_dicts()}
        rows: list[dict[str, Any]] = []
        for row in counts.to_dicts():
            origin_post = post_lookup.get(row["origin_post_id"], {})
            rows.append(
                {
                    "origin_post_id": row["origin_post_id"],
                    "propagation_count": row["propagation_count"],
                    "propagation_comment_count": row["propagation_comment_count"],
                    "origin_excerpt": (origin_post.get("message") or "")[:220],
                    "origin_permalink": origin_post.get("permalink"),
                }
            )
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
        if (
            collection_runs.height
            and "platform" in collection_runs.columns
            and collection_runs["platform"][0] == "telegram"
            and "discussion_linked" in collection_runs.columns
            and not bool(collection_runs["discussion_linked"][0])
        ):
            warnings.append("Telegram source has no linked discussion chat; stance/support metrics are based on posts only where comments are absent.")
        if comments.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some comments were collected without message text.")
        if posts.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some posts were collected without message text.")
        if analysis_runs.height:
            llm_provider = analysis_runs["llm_provider"][0]
            if llm_provider == "heuristic_llm":
                warnings.append("Stance and cluster descriptions were generated with heuristic fallbacks.")
        return list(dict.fromkeys(warnings))

    def _reaction_breakdown_summary(self, posts: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        totals: dict[str, int] = {}
        for frame in (posts, comments):
            if frame.is_empty() or "reaction_breakdown_json" not in frame.columns:
                continue
            for raw_value in frame["reaction_breakdown_json"].fill_null("").to_list():
                if not raw_value:
                    continue
                try:
                    payload = json.loads(raw_value)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue
                for key, value in payload.items():
                    totals[str(key)] = totals.get(str(key), 0) + int(value or 0)
        return [
            {"reaction": reaction, "count": count}
            for reaction, count in sorted(totals.items(), key=lambda item: item[1], reverse=True)
        ]

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()

    def _write_tabular_exports(self, run_id: str, export_tables: dict[str, pl.DataFrame]) -> list[Path]:
        export_root = self.paths.reports_root / f"report_{run_id}_tables"
        export_root.mkdir(parents=True, exist_ok=True)

        csv_paths: list[Path] = []
        for table_name, frame in export_tables.items():
            csv_path = export_root / f"{table_name}.csv"
            self._sanitize_export_frame(frame).write_csv(csv_path)
            csv_paths.append(csv_path)

        workbook_path = self.paths.reports_root / f"report_{run_id}.xlsx"
        workbook = Workbook()
        default_sheet = workbook.active
        workbook.remove(default_sheet)
        for table_name, frame in export_tables.items():
            worksheet = workbook.create_sheet(title=self._sheet_name(table_name))
            sanitized = self._sanitize_export_frame(frame)
            worksheet.append(list(sanitized.columns))
            for row in sanitized.iter_rows():
                worksheet.append([self._excel_cell_value(value) for value in row])
        workbook.save(workbook_path)
        return [*csv_paths, workbook_path]

    @staticmethod
    def _rows_to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    @staticmethod
    def _sanitize_export_frame(frame: pl.DataFrame) -> pl.DataFrame:
        if frame.is_empty():
            return frame
        expressions: list[pl.Expr] = []
        for column_name, dtype in frame.schema.items():
            base_type = dtype.base_type() if hasattr(dtype, "base_type") else None
            if base_type is pl.List:
                expressions.append(
                    pl.col(column_name).map_elements(
                        ReportService._json_list_cell,
                        return_dtype=pl.String,
                    ).alias(column_name)
                )
            elif base_type is pl.Struct:
                expressions.append(
                    pl.col(column_name).map_elements(
                        ReportService._json_object_cell,
                        return_dtype=pl.String,
                    ).alias(column_name)
                )
            else:
                expressions.append(pl.col(column_name))
        return frame.select(expressions)

    @staticmethod
    def _sheet_name(name: str) -> str:
        trimmed = name[:31]
        return trimmed or "sheet"

    @staticmethod
    def _excel_cell_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        return value

    @staticmethod
    def _json_list_cell(value: Any) -> str:
        if value is None:
            return "[]"
        if isinstance(value, pl.Series):
            return json.dumps(value.to_list(), ensure_ascii=False)
        if isinstance(value, tuple):
            return json.dumps(list(value), ensure_ascii=False)
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _json_object_cell(value: Any) -> str:
        if value is None:
            return "{}"
        if isinstance(value, pl.Series):
            return json.dumps(value.to_list(), ensure_ascii=False)
        return json.dumps(value, ensure_ascii=False)


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
