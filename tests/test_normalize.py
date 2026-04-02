from __future__ import annotations

import json
import shutil
from pathlib import Path

from facebook_posts_analysis.normalize import NormalizationService


def test_normalization_materializes_nested_comments(project_config, project_paths) -> None:
    service = NormalizationService(project_config, project_paths)
    summary = service.run(run_id="20260402T120000Z")

    assert summary["run_id"] == "20260402T120000Z"

    import polars as pl

    comments = pl.read_parquet(project_paths.processed_root / "comments.parquet")
    edges = pl.read_parquet(project_paths.processed_root / "comment_edges.parquet")
    posts = pl.read_parquet(project_paths.processed_root / "posts.parquet")

    assert posts.height == 2
    assert comments.height == 2
    assert edges.filter(pl.col("parent_comment_id").is_not_null()).height == 1
    assert comments.sort("depth")["depth"].to_list() == [0, 1]


def test_normalization_merges_recent_runs_into_snapshot(project_root: Path, project_config, project_paths) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"
    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)

    manifest_path = target_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["run_id"] = "20260402T121500Z"
    manifest["posts"][0]["comments"].append(
        {
            "comment_id": "comment_2",
            "parent_post_id": "page_1_post_1",
            "parent_comment_id": None,
            "created_at": "2026-03-30T10:00:00+00:00",
            "message": "A newly discovered supporting comment.",
            "permalink": "https://facebook.com/example/posts/1?comment_id=3",
            "reactions": 1,
            "source_collector": "meta_api",
            "depth": 0,
            "raw_path": "data/raw/20260402T121500Z/api_comment_items/comment_2.json",
            "author": {
                "author_id": "user_3",
                "name": "Late User",
                "profile_url": None,
            },
        }
    )
    manifest["posts"][0]["comments_count"] = 3
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 2
    service = NormalizationService(project_config, project_paths)
    summary = service.run(run_id="20260402T121500Z")

    assert summary["source_run_ids"] == ["20260402T120000Z", "20260402T121500Z"]

    import polars as pl

    comments = pl.read_parquet(project_paths.processed_root / "comments.parquet").filter(
        pl.col("run_id") == "20260402T121500Z"
    )
    collection_runs = pl.read_parquet(project_paths.processed_root / "collection_runs.parquet").filter(
        pl.col("run_id") == "20260402T121500Z"
    )

    assert comments.height == 3
    assert collection_runs["source_run_count"][0] == 2
