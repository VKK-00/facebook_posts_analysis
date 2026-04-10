from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from social_posts_analysis.analysis.service import AnalysisService
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.reporting.service import ReportService


def test_normalize_and_report_include_propagation_scopes(tmp_path: Path) -> None:
    run_id = "20260410T120000Z"
    root = tmp_path / "project"
    config = ProjectConfig.model_validate(
        {
            "project_name": "social_posts_analysis",
            "source": {
                "platform": "facebook",
                "url": "https://www.facebook.com/example-page/",
            },
            "collector": {
                "mode": "web",
                "public_web": {"enabled": True},
                "meta_api": {"enabled": False},
            },
            "sides": [
                {
                    "side_id": "actor_a",
                    "name": "Actor A",
                    "aliases": ["actor a"],
                    "support_keywords": ["support actor a"],
                    "oppose_keywords": ["oppose actor a"],
                }
            ],
            "analysis": {
                "languages": ["en"],
                "min_cluster_size": 2,
                "min_samples": 1,
            },
            "providers": {
                "embeddings": {"kind": "hash"},
                "llm": {"kind": "heuristic"},
            },
        }
    )
    paths = ProjectPaths.from_config(root, config)
    paths.ensure()

    manifest = CollectionManifest(
        run_id=run_id,
        collected_at="2026-04-10T12:00:00+00:00",
        collector="public_web",
        mode="web",
        source=SourceSnapshot(
            platform="facebook",
            source_id="page_1",
            source_name="Example Page",
            source_url="https://www.facebook.com/example-page/",
            source_type="page",
            source_collector="public_web",
            raw_path="source.json",
        ),
        posts=[
            PostSnapshot(
                post_id="facebook:page_1:post_1",
                platform="facebook",
                source_id="page_1",
                created_at="2026-04-08T10:00:00+00:00",
                message="Actor A update from the main page.",
                permalink="https://www.facebook.com/example-page/posts/1",
                comments_count=1,
                source_collector="public_web",
                raw_path="post_1.json",
                author=AuthorSnapshot(author_id="page_1", name="Example Page"),
                comments=[
                    CommentSnapshot(
                        comment_id="comment_origin_1",
                        platform="facebook",
                        parent_post_id="facebook:page_1:post_1",
                        created_at="2026-04-08T10:05:00+00:00",
                        message="I support Actor A.",
                        permalink="https://www.facebook.com/example-page/posts/1?comment_id=1",
                        source_collector="public_web",
                        raw_path="comment_origin_1.json",
                        author=AuthorSnapshot(author_id="user_1", name="User 1"),
                    )
                ],
            ),
            PostSnapshot(
                post_id="facebook:page_1:post_2",
                platform="facebook",
                source_id="page_1",
                origin_post_id="facebook:page_1:post_1",
                origin_external_id="1",
                origin_permalink="https://www.facebook.com/example-page/posts/1",
                propagation_kind="share",
                is_propagation=True,
                created_at="2026-04-08T11:00:00+00:00",
                message="Shared Actor A update to another audience.",
                permalink="https://www.facebook.com/example-page/posts/2",
                shares=1,
                comments_count=1,
                source_collector="public_web",
                raw_path="post_2.json",
                author=AuthorSnapshot(author_id="page_1", name="Example Page"),
                comments=[
                    CommentSnapshot(
                        comment_id="comment_prop_1",
                        platform="facebook",
                        parent_post_id="facebook:page_1:post_2",
                        created_at="2026-04-08T11:05:00+00:00",
                        message="I oppose Actor A.",
                        permalink="https://www.facebook.com/example-page/posts/2?comment_id=1",
                        source_collector="public_web",
                        raw_path="comment_prop_1.json",
                        author=AuthorSnapshot(author_id="user_2", name="User 2"),
                    )
                ],
            ),
        ],
    )

    run_dir = paths.run_raw_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")

    NormalizationService(config, paths).run(run_id=run_id)

    propagations = pl.read_parquet(paths.processed_root / "propagations.parquet").filter(pl.col("run_id") == run_id)
    comments = pl.read_parquet(paths.processed_root / "comments.parquet").filter(pl.col("run_id") == run_id)

    assert propagations.height == 1
    propagation_comment = comments.filter(pl.col("comment_id") == "comment_prop_1").to_dicts()[0]
    assert propagation_comment["parent_entity_type"] == "propagation"
    assert propagation_comment["parent_entity_id"] == "facebook:page_1:post_2"
    assert propagation_comment["origin_post_id"] == "facebook:page_1:post_1"

    AnalysisService(config, paths).run(run_id=run_id)
    context = ReportService(config, paths)._build_context(run_id)

    assert context["post_count"] == 1
    assert context["propagation_count"] == 1
    assert context["top_propagated_items"][0]["origin_post_id"] == "facebook:page_1:post_1"

    origin_plus_row = next(
        row for row in context["origin_plus_support"] if row["scope_id"] == "facebook:page_1:post_1" and row["side_id"] == "actor_a"
    )
    propagation_row = next(
        row for row in context["propagation_support"] if row["scope_id"] == "facebook:page_1:post_2" and row["side_id"] == "actor_a"
    )

    assert origin_plus_row["support_count"] == 1
    assert origin_plus_row["oppose_count"] == 1
    assert propagation_row["oppose_count"] == 1
