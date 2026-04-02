from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from facebook_posts_analysis.config import ProjectConfig
from facebook_posts_analysis.paths import ProjectPaths

from .clustering import NarrativeClusterer
from .language import LanguageDetector
from .metrics import compute_support_metrics
from .providers import build_providers
from .stance import StanceAnalyzer


class AnalysisService:
    ANALYSIS_KEYS = {
        "analysis_runs": ["run_id"],
        "detected_languages": ["run_id", "item_type", "item_id"],
        "cluster_memberships": ["run_id", "item_type", "item_id"],
        "narrative_clusters": ["run_id", "item_type", "cluster_id"],
        "stance_labels": ["run_id", "item_type", "item_id", "side_id"],
        "support_metrics": ["run_id", "scope_type", "scope_id", "side_id"],
    }
    ANALYSIS_SCHEMAS = {
        "analysis_runs": {
            "run_id": pl.String,
            "embedding_provider": pl.String,
            "llm_provider": pl.String,
            "post_items": pl.Int64,
            "comment_items": pl.Int64,
        },
        "detected_languages": {
            "item_type": pl.String,
            "item_id": pl.String,
            "language": pl.String,
            "confidence": pl.Float64,
            "method": pl.String,
            "run_id": pl.String,
        },
        "cluster_memberships": {
            "item_type": pl.String,
            "item_id": pl.String,
            "cluster_id": pl.String,
            "run_id": pl.String,
        },
        "narrative_clusters": {
            "item_type": pl.String,
            "cluster_id": pl.String,
            "label": pl.String,
            "description": pl.String,
            "top_keywords": pl.List(pl.String),
            "exemplar_ids": pl.List(pl.String),
            "run_id": pl.String,
        },
        "stance_labels": {
            "item_type": pl.String,
            "item_id": pl.String,
            "side_id": pl.String,
            "label": pl.String,
            "confidence": pl.Float64,
            "model_name": pl.String,
            "run_id": pl.String,
        },
        "support_metrics": {
            "scope_type": pl.String,
            "scope_id": pl.String,
            "side_id": pl.String,
            "support_count": pl.Int64,
            "oppose_count": pl.Int64,
            "neutral_count": pl.Int64,
            "unclear_count": pl.Int64,
            "support_ratio": pl.Float64,
            "net_support": pl.Int64,
            "run_id": pl.String,
        },
    }

    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> dict[str, Any]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No normalized run found to analyze.")

        posts = self._load_table("posts").filter(pl.col("run_id") == resolved_run_id)
        comments = self._load_table("comments").filter(pl.col("run_id") == resolved_run_id)

        detector = LanguageDetector(self.config.analysis.languages)
        providers = build_providers(self.config.providers.embeddings, self.config.providers.llm)
        clusterer = NarrativeClusterer(
            llm_provider=providers.llm,
            exemplar_count=self.config.analysis.exemplar_count,
            min_cluster_size=self.config.analysis.min_cluster_size,
            min_samples=self.config.analysis.min_samples,
        )
        stance_analyzer = StanceAnalyzer(providers.llm, self.config.sides)

        post_items = self._items_from_frame(posts, "post")
        comment_items = self._items_from_frame(comments, "comment")

        language_rows = self._detect_languages(post_items, detector, resolved_run_id)
        language_rows.extend(self._detect_languages(comment_items, detector, resolved_run_id))

        post_embeddings = providers.embeddings.embed_texts([item["text"] for item in post_items])
        comment_embeddings = providers.embeddings.embed_texts([item["text"] for item in comment_items])

        post_clusters, post_memberships = clusterer.cluster_items("post", post_items, post_embeddings, resolved_run_id)
        comment_clusters, comment_memberships = clusterer.cluster_items(
            "comment",
            comment_items,
            comment_embeddings,
            resolved_run_id,
        )

        stance_rows = stance_analyzer.label_items("post", post_items, resolved_run_id)
        stance_rows.extend(stance_analyzer.label_items("comment", comment_items, resolved_run_id))

        support_metrics = compute_support_metrics(
            pl.DataFrame(stance_rows) if stance_rows else pl.DataFrame(),
            pl.DataFrame(comment_memberships) if comment_memberships else pl.DataFrame(),
            resolved_run_id,
        )

        analysis_run = [
            {
                "run_id": resolved_run_id,
                "embedding_provider": providers.summary["embeddings"],
                "llm_provider": providers.summary["llm"],
                "post_items": len(post_items),
                "comment_items": len(comment_items),
            }
        ]

        outputs = {
            "analysis_runs": self._persist_table("analysis_runs", analysis_run),
            "detected_languages": self._persist_table("detected_languages", language_rows),
            "cluster_memberships": self._persist_table("cluster_memberships", [*post_memberships, *comment_memberships]),
            "narrative_clusters": self._persist_table("narrative_clusters", [*post_clusters, *comment_clusters]),
            "stance_labels": self._persist_table("stance_labels", stance_rows),
            "support_metrics": self._persist_table(
                "support_metrics",
                support_metrics.to_dicts() if not support_metrics.is_empty() else [],
            ),
        }
        self._sync_duckdb(outputs)
        return {"run_id": resolved_run_id, "providers": providers.summary}

    def _items_from_frame(self, frame: pl.DataFrame, item_type: str) -> list[dict[str, Any]]:
        if frame.is_empty():
            return []
        limit = self.config.analysis.max_items_per_item_type
        rows = frame.select(
            (
                pl.col("post_id")
                if item_type == "post"
                else pl.col("comment_id")
            ).alias("item_id"),
            pl.col("message").fill_null("").alias("text"),
            pl.col("parent_post_id").fill_null("").alias("parent_post_id") if item_type == "comment" else pl.lit("").alias("parent_post_id"),
        )
        rows = rows.filter(pl.col("text").str.len_chars() > 0)
        if limit:
            rows = rows.head(limit)
        return rows.to_dicts()

    @staticmethod
    def _detect_languages(
        items: list[dict[str, Any]],
        detector: LanguageDetector,
        run_id: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in items:
            prediction = detector.detect(item["text"])
            rows.append(
                {
                    "item_type": "comment" if item.get("parent_post_id") else "post",
                    "item_id": item["item_id"],
                    "language": prediction.language,
                    "confidence": prediction.confidence,
                    "method": prediction.method,
                    "run_id": run_id,
                }
            )
        return rows

    def _persist_table(self, table_name: str, records: list[dict[str, Any]]) -> Path:
        path = self.paths.processed_root / f"{table_name}.parquet"
        new_df = pl.DataFrame(records, schema=self.ANALYSIS_SCHEMAS[table_name]) if records else pl.DataFrame(
            schema=self.ANALYSIS_SCHEMAS[table_name]
        )
        if path.exists():
            existing_df = pl.read_parquet(path)
            if new_df.is_empty():
                combined = existing_df
            elif existing_df.is_empty():
                combined = new_df
            else:
                combined = pl.concat([existing_df, new_df], how="diagonal_relaxed")
        else:
            combined = new_df

        key_columns = [column for column in self.ANALYSIS_KEYS[table_name] if column in combined.columns]
        if key_columns and not combined.is_empty():
            combined = combined.unique(subset=key_columns, keep="last")
        combined.write_parquet(path)
        return path

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()

    def _sync_duckdb(self, table_paths: dict[str, Path]) -> None:
        connection = duckdb.connect(str(self.paths.database_path))
        try:
            for table_name, path in table_paths.items():
                path_str = path.as_posix().replace("'", "''")
                connection.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')"
                )
        finally:
            connection.close()
