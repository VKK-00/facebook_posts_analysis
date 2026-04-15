from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import markdown
import polars as pl
from jinja2 import Environment, FileSystemLoader, select_autoescape

from social_posts_analysis.analysis.metrics import compute_support_metrics
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.propagation import filter_origin_posts_frame
from social_posts_analysis.utils import read_json

from .exports import merge_existing_export, rows_to_frame, sanitize_export_frame, write_tabular_exports
from .summaries import (
    instagram_summary,
    person_monitor_breakdown,
    post_overview,
    propagation_comment_overview,
    propagation_overview,
    propagation_summary,
    reaction_breakdown_summary,
    reply_depth_summary,
    telegram_summary,
    threads_summary,
    top_external_sources_by_hit_count,
    top_propagated_items,
    x_summary,
)


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

        narrative_export = merge_existing_export(
            narrative_path,
            narrative_export,
            keys=["item_type", "cluster_id"],
            editable_columns=["action", "target_cluster_id", "new_label", "new_description"],
        )
        stance_export = merge_existing_export(
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
        posts = self._load_run_table("posts", run_id)
        propagations = self._load_run_table("propagations", run_id)
        comments = self._load_run_table("comments", run_id)
        origin_posts = filter_origin_posts_frame(posts)
        languages = self._load_run_table("detected_languages", run_id)
        clusters = self._load_run_table("narrative_clusters", run_id)
        memberships = self._load_run_table("cluster_memberships", run_id)
        stance_labels = self._load_run_table("stance_labels", run_id)
        analysis_runs = self._load_run_table("analysis_runs", run_id)
        collection_runs = self._load_run_table("collection_runs", run_id)
        observed_sources = self._load_run_table("observed_sources", run_id)
        match_hits = self._load_run_table("match_hits", run_id)

        clusters, memberships = self._apply_narrative_overrides(clusters, memberships)
        stance_labels = self._apply_stance_overrides(stance_labels)
        comment_memberships = (
            memberships.filter(pl.col("item_type") == "comment")
            if not memberships.is_empty() and "item_type" in memberships.columns
            else pl.DataFrame()
        )
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
        propagation_comments = self._propagation_comment_overview(comments, propagations)
        top_propagated_items = self._top_propagated_items(origin_posts, propagations)
        source_run_trace = self._source_run_trace_rows(run_id, collection_runs)
        source_warnings = self._source_warning_rows(run_id, collection_runs)
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
        warnings = self._data_quality_warnings(run_id, posts, propagations, comments, analysis_runs, collection_runs)

        platform = collection_runs["platform"][0] if collection_runs.height and "platform" in collection_runs.columns else self.config.source.platform
        source_kind = collection_runs["source_kind"][0] if collection_runs.height and "source_kind" in collection_runs.columns else "feed"
        is_person_monitor = source_kind == "person_monitor"
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
        person_monitor_stats = (
            person_monitor_breakdown(match_hits)
            if is_person_monitor
            else {
                "authored_posts": 0,
                "authored_comments": 0,
                "mentioned_posts": 0,
                "mentioned_comments": 0,
            }
        )
        top_external_sources = top_external_sources_by_hit_count(match_hits, observed_sources) if is_person_monitor else []
        matched_posts = self._matched_posts_frame(posts, match_hits) if is_person_monitor else pl.DataFrame()
        matched_comments = self._matched_comments_frame(comments, match_hits) if is_person_monitor else pl.DataFrame()
        export_tables = {
            "per_side_support": self._rows_to_frame(global_support),
            "per_origin_support": self._rows_to_frame(origin_support),
            "per_origin_plus_propagations": self._rows_to_frame(origin_plus_support),
            "per_propagation_support": self._rows_to_frame(propagation_support),
            "per_post_overview": post_overview,
            "per_propagation_overview": propagation_overview,
            "per_propagation_comments": propagation_comments,
            "per_thread_conflict": self._rows_to_frame(conflict_rows),
            "post_narratives": self._sanitize_export_frame(self._rows_to_frame(post_cluster_rows)),
            "propagation_narratives": self._sanitize_export_frame(self._rows_to_frame(propagation_cluster_rows)),
            "comment_narratives": self._sanitize_export_frame(self._rows_to_frame(comment_cluster_rows)),
            "language_mix": self._rows_to_frame(language_mix),
            "top_supportive_comments": self._rows_to_frame(top_supportive_comments),
            "top_critical_comments": self._rows_to_frame(top_critical_comments),
            "coverage_gaps": self._rows_to_frame(coverage_gaps),
            "propagation_coverage_gaps": self._rows_to_frame(propagation_coverage_gaps),
            "reply_depth_summary": self._rows_to_frame(reply_depth_summary),
            "top_propagated_items": self._rows_to_frame(top_propagated_items),
            "source_run_trace": self._rows_to_frame(source_run_trace),
            "source_warnings": self._rows_to_frame(source_warnings),
            "observed_sources": observed_sources,
            "match_hits": match_hits,
            "matched_posts": matched_posts,
            "matched_comments": matched_comments,
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
            "source_kind": source_kind,
            "is_person_monitor": is_person_monitor,
            "source_name": source_name,
            "source_id": source_id,
            "source_type": source_type,
            "post_count": origin_posts.height,
            "propagation_count": propagations.height,
            "comment_count": comments.height,
            "observed_source_count": observed_sources.height,
            "match_hit_count": match_hits.height,
            "person_monitor_stats": person_monitor_stats,
            "top_external_sources": top_external_sources,
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
            "propagation_comments": propagation_comments.head(20).to_dicts() if not propagation_comments.is_empty() else [],
            "top_supportive_comments": top_supportive_comments,
            "top_critical_comments": top_critical_comments,
            "reply_depth_summary": reply_depth_summary,
            "source_run_trace": source_run_trace,
            "source_warnings": source_warnings,
            "warnings": warnings,
            "observed_sources": observed_sources.to_dicts() if not observed_sources.is_empty() else [],
            "match_hits": match_hits.to_dicts() if not match_hits.is_empty() else [],
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
        return post_overview(posts, comments)

    def _propagation_overview(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> pl.DataFrame:
        return propagation_overview(propagations, comments)

    def _propagation_comment_overview(self, comments: pl.DataFrame, propagations: pl.DataFrame) -> pl.DataFrame:
        return propagation_comment_overview(comments, propagations)

    def _telegram_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
        collection_runs: pl.DataFrame,
    ) -> dict[str, Any]:
        return telegram_summary(posts, comments, collection_runs)

    def _x_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return x_summary(posts, comments)

    def _threads_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return threads_summary(posts, comments)

    def _instagram_summary(
        self,
        posts: pl.DataFrame,
        comments: pl.DataFrame,
    ) -> dict[str, Any]:
        return instagram_summary(posts, comments)

    def _propagation_summary(self, propagations: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any] | None:
        return propagation_summary(propagations, comments)

    def _top_propagated_items(self, posts: pl.DataFrame, propagations: pl.DataFrame) -> list[dict[str, Any]]:
        return top_propagated_items(posts, propagations)

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
        return reply_depth_summary(comments)

    def _source_warning_rows(self, run_id: str, collection_runs: pl.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for source_run_id, manifest in self._source_run_manifests(run_id, collection_runs):
            for warning_index, warning in enumerate(manifest.get("warnings", []), start=1):
                if not warning:
                    continue
                rows.append(
                    {
                        "source_run_id": source_run_id,
                        "warning_index": warning_index,
                        "warning": warning,
                    }
                )
        if rows:
            return rows
        if collection_runs.is_empty() or "warning_messages" not in collection_runs.columns:
            return []
        raw_warning_messages = collection_runs["warning_messages"][0]
        if isinstance(raw_warning_messages, pl.Series):
            warning_messages = raw_warning_messages.to_list()
        else:
            warning_messages = list(raw_warning_messages or [])
        return [
            {
                "source_run_id": run_id,
                "warning_index": index + 1,
                "warning": warning,
            }
            for index, warning in enumerate(warning_messages)
            if warning
        ]

    def _source_run_trace_rows(self, run_id: str, collection_runs: pl.DataFrame) -> list[dict[str, Any]]:
        rows = [
            {
                "source_run_id": source_run_id,
                "collector": manifest.get("collector"),
                "mode": manifest.get("mode"),
                "status": manifest.get("status"),
                "fallback_used": bool(manifest.get("fallback_used", False)),
                "warning_count": len(manifest.get("warnings", [])),
            }
            for source_run_id, manifest in self._source_run_manifests(run_id, collection_runs)
        ]
        if rows:
            return rows
        if collection_runs.is_empty():
            return []
        return [
            {
                "source_run_id": run_id,
                "collector": collection_runs["collector"][0] if "collector" in collection_runs.columns else None,
                "mode": collection_runs["mode"][0] if "mode" in collection_runs.columns else None,
                "status": collection_runs["status"][0] if "status" in collection_runs.columns else None,
                "fallback_used": bool(collection_runs["fallback_used"][0]) if "fallback_used" in collection_runs.columns else False,
                "warning_count": int(collection_runs["warning_count"][0]) if "warning_count" in collection_runs.columns else 0,
            }
        ]

    def _source_run_ids(self, run_id: str, collection_runs: pl.DataFrame) -> list[str]:
        if collection_runs.height and "source_run_ids" in collection_runs.columns:
            raw_source_run_ids = collection_runs["source_run_ids"][0]
            if isinstance(raw_source_run_ids, pl.Series):
                source_run_ids = raw_source_run_ids.to_list()
            else:
                source_run_ids = list(raw_source_run_ids or [])
            if source_run_ids:
                return source_run_ids
        return [run_id]

    def _source_run_manifests(self, run_id: str, collection_runs: pl.DataFrame) -> list[tuple[str, dict[str, Any]]]:
        rows: list[tuple[str, dict[str, Any]]] = []
        for source_run_id in self._source_run_ids(run_id, collection_runs):
            manifest_path = self.paths.run_raw_dir(source_run_id) / "manifest.json"
            if not manifest_path.exists():
                continue
            rows.append((source_run_id, read_json(manifest_path)))
        return rows

    def _data_quality_warnings(
        self,
        run_id: str,
        posts: pl.DataFrame,
        propagations: pl.DataFrame,
        comments: pl.DataFrame,
        analysis_runs: pl.DataFrame,
        collection_runs: pl.DataFrame,
    ) -> list[str]:
        warnings: list[str] = []
        source_warning_rows = self._source_warning_rows(run_id, collection_runs)
        if source_warning_rows:
            warnings.extend(row["warning"] for row in source_warning_rows)
        else:
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
        if not propagations.is_empty() and comments.filter(pl.col("parent_entity_type") == "propagation").is_empty():
            warnings.append("Propagation instances were detected, but no comments were extracted from propagated copies in this run.")
        if comments.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some comments were collected without message text.")
        if posts.filter(pl.col("message").fill_null("").str.len_chars() == 0).height > 0:
            warnings.append("Some posts were collected without message text.")
        if analysis_runs.height:
            llm_provider = analysis_runs["llm_provider"][0]
            if llm_provider == "heuristic_llm":
                warnings.append("Stance and cluster descriptions were generated with heuristic fallbacks.")
        return list(dict.fromkeys(warnings))

    def _matched_posts_frame(self, posts: pl.DataFrame, match_hits: pl.DataFrame) -> pl.DataFrame:
        if posts.is_empty() or match_hits.is_empty():
            return pl.DataFrame()
        post_like_hits = (
            match_hits.filter(pl.col("item_type").is_in(["post", "propagation"]))
            .group_by("item_id")
            .agg(
                pl.len().alias("match_count"),
                pl.col("match_kind").alias("match_kinds"),
                pl.col("matched_value").alias("matched_values"),
            )
        )
        if post_like_hits.is_empty():
            return pl.DataFrame()
        return (
            posts.join(post_like_hits, left_on="post_id", right_on="item_id", how="inner")
            .with_columns(
                pl.when(pl.col("is_propagation"))
                .then(pl.lit("propagation"))
                .otherwise(pl.lit("post"))
                .alias("item_type")
            )
            .select(
                "item_type",
                "post_id",
                "container_source_id",
                "container_source_name",
                "container_source_url",
                "discovery_kind",
                "created_at",
                "permalink",
                "match_count",
                "match_kinds",
                "matched_values",
                "message",
            )
            .sort("created_at", descending=True)
        )

    def _matched_comments_frame(self, comments: pl.DataFrame, match_hits: pl.DataFrame) -> pl.DataFrame:
        if comments.is_empty() or match_hits.is_empty():
            return pl.DataFrame()
        comment_hits = (
            match_hits.filter(pl.col("item_type") == "comment")
            .group_by("item_id")
            .agg(
                pl.len().alias("match_count"),
                pl.col("match_kind").alias("match_kinds"),
                pl.col("matched_value").alias("matched_values"),
            )
        )
        if comment_hits.is_empty():
            return pl.DataFrame()
        return (
            comments.join(comment_hits, left_on="comment_id", right_on="item_id", how="inner")
            .select(
                "comment_id",
                "parent_post_id",
                "container_source_id",
                "container_source_name",
                "container_source_url",
                "discovery_kind",
                "created_at",
                "permalink",
                "match_count",
                "match_kinds",
                "matched_values",
                "message",
                "depth",
            )
            .sort("created_at", descending=True)
        )

    def _reaction_breakdown_summary(self, posts: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
        return reaction_breakdown_summary(posts, comments)

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()

    def _load_run_table(self, table_name: str, run_id: str) -> pl.DataFrame:
        frame = self._load_table(table_name)
        if frame.is_empty() or "run_id" not in frame.columns:
            return frame
        return frame.filter(pl.col("run_id") == run_id)

    def _write_tabular_exports(self, run_id: str, export_tables: dict[str, pl.DataFrame]) -> list[Path]:
        return write_tabular_exports(self.paths, run_id, export_tables)

    @staticmethod
    def _rows_to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
        return rows_to_frame(rows)

    @staticmethod
    def _sanitize_export_frame(frame: pl.DataFrame) -> pl.DataFrame:
        return sanitize_export_frame(frame)

    @staticmethod
    def _sheet_name(name: str) -> str:
        from .exports import sheet_name

        return sheet_name(name)

    @staticmethod
    def _excel_cell_value(value: Any) -> Any:
        from .exports import excel_cell_value

        return excel_cell_value(value)

    @staticmethod
    def _json_list_cell(value: Any) -> str:
        from .exports import json_list_cell

        return json_list_cell(value)

    @staticmethod
    def _json_object_cell(value: Any) -> str:
        from .exports import json_object_cell

        return json_object_cell(value)
