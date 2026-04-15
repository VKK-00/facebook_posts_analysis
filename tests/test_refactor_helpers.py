from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl

from social_posts_analysis.analysis.service import AnalysisService
from social_posts_analysis.collectors.range_utils import RangeFilter
from social_posts_analysis.collectors.web_runtime import prepare_temp_profile_directory
from social_posts_analysis.contracts import CommentSnapshot, PostSnapshot
from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.propagation import filter_origin_posts_frame, resolve_comment_scope
from social_posts_analysis.reporting.service import ReportService


def test_range_filter_handles_date_and_datetime_boundaries() -> None:
    range_filter = RangeFilter.from_strings("2026-04-01", "2026-04-02")

    assert range_filter.contains("2026-04-01T00:00:00+00:00", allow_missing=False) is True
    assert range_filter.contains("2026-04-02T23:59:59+00:00", allow_missing=False) is True
    assert range_filter.contains("2026-03-31T23:59:59+00:00", allow_missing=False) is False
    assert range_filter.contains("2026-04-03T00:00:00+00:00", allow_missing=False) is False


def test_propagation_helpers_classify_origin_posts_and_comment_scope() -> None:
    origin_post = PostSnapshot(
        post_id="facebook:page_1:post_1",
        platform="facebook",
        source_id="page_1",
        message="Origin",
        source_collector="test",
    )
    propagation_post = PostSnapshot(
        post_id="facebook:page_1:post_2",
        platform="facebook",
        source_id="page_1",
        origin_post_id="facebook:page_1:post_1",
        propagation_kind="share",
        is_propagation=True,
        message="Shared",
        source_collector="test",
    )
    comment = CommentSnapshot(
        comment_id="comment_1",
        platform="facebook",
        parent_post_id=propagation_post.post_id,
        message="Reply",
        source_collector="test",
    )

    origin_scope = resolve_comment_scope(origin_post, comment.model_copy(update={"parent_post_id": origin_post.post_id}))
    propagation_scope = resolve_comment_scope(propagation_post, comment)
    filtered = filter_origin_posts_frame(
        pl.DataFrame(
            [
                {"post_id": origin_post.post_id, "is_propagation": False},
                {"post_id": propagation_post.post_id, "is_propagation": True},
            ]
        )
    )

    assert origin_scope.parent_entity_type == "post"
    assert origin_scope.parent_entity_id == origin_post.post_id
    assert origin_scope.origin_post_id == origin_post.post_id
    assert propagation_scope.parent_entity_type == "propagation"
    assert propagation_scope.parent_entity_id == propagation_post.post_id
    assert propagation_scope.origin_post_id == origin_post.post_id
    assert filtered["post_id"].to_list() == [origin_post.post_id]


def test_prepare_temp_profile_directory_copies_profile_snapshot_without_cache_dirs(tmp_path: Path) -> None:
    source_user_data_dir = tmp_path / "source"
    profile_dir = source_user_data_dir / "Default"
    cache_dir = profile_dir / "Cache"
    cache_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text("prefs", encoding="utf-8")
    (cache_dir / "cache.bin").write_text("skip", encoding="utf-8")
    (source_user_data_dir / "Local State").write_text("state", encoding="utf-8")

    snapshot_dir = prepare_temp_profile_directory(
        source_user_data_dir=source_user_data_dir,
        profile_directory="Default",
        temp_root_dir=str(tmp_path / "snapshots"),
        prefix="runtime-test-",
        best_effort=True,
    )
    try:
        assert (snapshot_dir / "Local State").exists()
        assert (snapshot_dir / "Default" / "Preferences").exists()
        assert not (snapshot_dir / "Default" / "Cache" / "cache.bin").exists()
    finally:
        import shutil

        shutil.rmtree(snapshot_dir, ignore_errors=True)


def test_report_export_table_schema_stays_stable(project_config, project_paths) -> None:
    run_id = "20260402T120000Z"
    NormalizationService(project_config, project_paths).run(run_id=run_id)
    AnalysisService(project_config, project_paths).run(run_id=run_id)

    context = ReportService(project_config, project_paths)._build_context(run_id)

    assert set(context["export_tables"]) >= {
        "per_side_support",
        "per_origin_support",
        "per_origin_plus_propagations",
        "per_propagation_support",
        "per_post_overview",
        "per_propagation_overview",
        "per_propagation_comments",
        "per_thread_conflict",
        "post_narratives",
        "propagation_narratives",
        "comment_narratives",
        "language_mix",
        "top_supportive_comments",
        "top_critical_comments",
        "coverage_gaps",
        "propagation_coverage_gaps",
        "reply_depth_summary",
        "top_propagated_items",
        "source_run_trace",
        "source_warnings",
        "observed_sources",
        "match_hits",
        "matched_posts",
        "matched_comments",
    }


def test_report_service_uses_collection_run_warning_messages_for_merged_runs(
    project_root: Path,
    project_config,
    project_paths,
) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"
    source_manifest_path = source_dir / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    source_manifest["collector"] = "meta_api"
    source_manifest["mode"] = "hybrid"
    source_manifest["status"] = "success"
    source_manifest["fallback_used"] = False
    source_manifest["warnings"] = ["Older source-run warning."]
    source_manifest_path.write_text(json.dumps(source_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)
    target_manifest_path = target_dir / "manifest.json"
    target_manifest = json.loads(target_manifest_path.read_text(encoding="utf-8"))
    target_manifest["run_id"] = "20260402T121500Z"
    target_manifest["collector"] = "public_web"
    target_manifest["mode"] = "hybrid"
    target_manifest["status"] = "partial"
    target_manifest["fallback_used"] = True
    target_manifest["warnings"] = ["Newer source-run warning."]
    target_manifest_path.write_text(json.dumps(target_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 2
    NormalizationService(project_config, project_paths).run(run_id="20260402T121500Z")
    AnalysisService(project_config, project_paths).run(run_id="20260402T121500Z")

    context = ReportService(project_config, project_paths)._build_context("20260402T121500Z")

    assert "Older source-run warning." in context["warnings"]
    assert "Newer source-run warning." in context["warnings"]
    assert "source_run_trace" in context["export_tables"]
    assert "source_warnings" in context["export_tables"]
    source_run_trace = context["export_tables"]["source_run_trace"].sort("source_run_id")
    assert source_run_trace["source_run_id"].to_list() == ["20260402T120000Z", "20260402T121500Z"]
    assert source_run_trace["collector"].to_list() == ["meta_api", "public_web"]
    assert source_run_trace["mode"].to_list() == ["hybrid", "hybrid"]
    assert source_run_trace["status"].to_list() == ["success", "partial"]
    assert source_run_trace["fallback_used"].to_list() == [False, True]
    source_warnings = context["export_tables"]["source_warnings"].sort(["source_run_id", "warning_index"])
    assert source_warnings["source_run_id"].to_list() == ["20260402T120000Z", "20260402T121500Z"]
    assert source_warnings["warning"].to_list() == ["Older source-run warning.", "Newer source-run warning."]
