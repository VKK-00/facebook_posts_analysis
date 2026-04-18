from __future__ import annotations

import json
from typing import Any

import polars as pl
import pytest

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.history import (
    HistoricalBackfillService,
    HistoryAnalysisService,
    HistoryReportService,
    build_monthly_windows,
)


def test_history_config_accepts_defaults(project_config) -> None:
    assert project_config.history.window == "month"
    assert project_config.history.max_windows == 240
    assert project_config.history.max_items_per_window == 5000
    assert project_config.history.max_comments_per_post == 5000
    assert project_config.history.resume is True
    assert project_config.history.stop_on_error is False


def test_build_monthly_windows_splits_inclusive_months() -> None:
    windows = build_monthly_windows("2026-01-15", "2026-03-02", max_windows=12)

    assert [(item.window_id, item.start, item.end) for item in windows] == [
        ("202601", "2026-01-15", "2026-01-31"),
        ("202602", "2026-02-01", "2026-02-28"),
        ("202603", "2026-03-01", "2026-03-02"),
    ]


def test_history_backfill_null_start_for_telegram_uses_oldest_message_discovery(project_config, project_paths, monkeypatch) -> None:
    project_config.source.platform = "telegram"
    project_config.source.source_name = "example_channel"
    project_config.collector.mode = "mtproto"
    project_config.collector.telegram_mtproto.enabled = True
    project_config.collector.telegram_mtproto.session_file = ".sessions/example"
    project_config.collector.telegram_mtproto.api_id = 12345
    project_config.collector.telegram_mtproto.api_hash = "hash"
    project_config.history.start = None
    project_config.history.end = "2026-02-15"
    project_config.history.max_windows = 3

    called: list[bool] = []

    monkeypatch.setattr(
        "social_posts_analysis.history.discover_history_start",
        lambda config: called.append(True) or "2026-01-10",
    )

    service = HistoricalBackfillService(
        project_config,
        project_paths,
        collection_service_factory=lambda config, paths: FakeCollectionService(config, paths),
    )
    summary = service.run(history_run_id="hist-telegram")

    assert called == [True]
    assert summary["history_run_id"] == "hist-telegram"
    assert [item["window_id"] for item in summary["windows"]] == ["202601", "202602"]
    assert (project_paths.raw_root / "_history/hist-telegram/manifest.json").exists()


def test_history_backfill_requires_start_for_non_telegram_history(project_config, project_paths) -> None:
    project_config.history.start = None
    project_config.history.end = "2026-01-31"

    service = HistoricalBackfillService(
        project_config,
        project_paths,
        collection_service_factory=lambda config, paths: FakeCollectionService(config, paths),
    )

    with pytest.raises(RuntimeError, match="history.start is required"):
        service.run(history_run_id="hist-missing-start")


def test_history_backfill_max_windows_adds_warning(project_config, project_paths) -> None:
    project_config.history.start = "2026-01-01"
    project_config.history.end = "2026-03-31"
    project_config.history.max_windows = 1

    service = HistoricalBackfillService(
        project_config,
        project_paths,
        collection_service_factory=lambda config, paths: FakeCollectionService(config, paths),
    )
    summary = service.run(history_run_id="hist-window-limit")

    assert summary["status"] == "partial"
    assert [item["window_id"] for item in summary["windows"]] == ["202601"]
    assert any("history.max_windows=1" in warning for warning in summary["warnings"])


def test_history_backfill_resume_skips_existing_child_run(project_config, project_paths) -> None:
    project_config.history.start = "2026-01-01"
    project_config.history.end = "2026-01-31"
    project_config.history.resume = True

    service = HistoricalBackfillService(
        project_config,
        project_paths,
        collection_service_factory=lambda config, paths: FakeCollectionService(config, paths),
    )
    first = service.run(history_run_id="hist-resume")
    second = service.run(history_run_id="hist-resume")

    assert first["windows"][0]["status"] == "success"
    assert second["windows"][0]["status"] == "skipped"
    history_windows = pl.read_parquet(project_paths.processed_root / "history_windows.parquet").filter(
        pl.col("history_run_id") == "hist-resume"
    )
    assert history_windows.height == 1
    assert history_windows["child_run_id"][0] == "hist-resume__202601"


def test_history_backfill_failed_window_is_recorded_as_partial(project_config, project_paths) -> None:
    project_config.history.start = "2026-01-01"
    project_config.history.end = "2026-02-28"

    service = HistoricalBackfillService(
        project_config,
        project_paths,
        collection_service_factory=lambda config, paths: FailingSecondWindowCollectionService(config, paths),
    )
    summary = service.run(history_run_id="hist-partial")

    assert summary["status"] == "partial"
    assert [item["status"] for item in summary["windows"]] == ["success", "failed"]
    history_manifest = json.loads(
        (project_paths.raw_root / "_history/hist-partial/manifest.json").read_text(encoding="utf-8")
    )
    assert history_manifest["status"] == "partial"
    assert "Synthetic February failure" in history_manifest["windows"][1]["warnings"][0]


def test_history_analysis_builds_temporal_tables_for_feed(project_config, project_paths) -> None:
    _write_history_processed_fixture(project_paths, history_run_id="hist-analysis", source_kind="feed")

    summary = HistoryAnalysisService(project_config, project_paths).run(history_run_id="hist-analysis")

    assert summary["history_run_id"] == "hist-analysis"
    temporal = pl.read_parquet(project_paths.processed_root / "history_temporal_metrics.parquet").filter(
        pl.col("history_run_id") == "hist-analysis"
    )
    item_index = pl.read_parquet(project_paths.processed_root / "history_item_index.parquet").filter(
        pl.col("history_run_id") == "hist-analysis"
    )
    clusters = pl.read_parquet(project_paths.processed_root / "history_narrative_clusters.parquet").filter(
        pl.col("history_run_id") == "hist-analysis"
    )

    assert set(temporal["window_id"].to_list()) == {"202601", "202602"}
    side_id = project_config.sides[0].side_id
    assert temporal.filter((pl.col("item_type") == "comment") & (pl.col("side_id") == side_id)).height == 2
    assert item_index.filter(pl.col("item_type") == "comment").height == 2
    assert clusters.height > 0


def test_history_analysis_person_monitor_separates_authored_and_mentions(project_config, project_paths) -> None:
    _write_history_processed_fixture(project_paths, history_run_id="hist-pm", source_kind="person_monitor")
    pl.DataFrame(
        {
            "match_id": ["m1", "m2"],
            "run_id": ["hist-pm__202601", "hist-pm__202602"],
            "item_type": ["comment", "post"],
            "item_id": ["c1", "p2"],
            "match_kind": ["authored_by_subject", "alias_text_mention"],
            "matched_value": ["subject", "Subject"],
            "platform": ["facebook", "facebook"],
            "container_source_id": ["external", "external"],
        }
    ).write_parquet(project_paths.processed_root / "match_hits.parquet")

    HistoryAnalysisService(project_config, project_paths).run(history_run_id="hist-pm")

    temporal = pl.read_parquet(project_paths.processed_root / "history_temporal_metrics.parquet").filter(
        pl.col("history_run_id") == "hist-pm"
    )
    assert temporal.filter(pl.col("metric_kind") == "person_monitor_authored_activity").height == 1
    assert temporal.filter(pl.col("metric_kind") == "person_monitor_mention_activity").height == 1


def test_history_report_writes_markdown_html_and_csv(project_config, project_paths) -> None:
    _write_history_processed_fixture(project_paths, history_run_id="hist-report", source_kind="feed")
    HistoryAnalysisService(project_config, project_paths).run(history_run_id="hist-report")

    outputs = HistoryReportService(project_config, project_paths).run(history_run_id="hist-report")

    assert project_paths.reports_root / "history/hist-report/history_report.md" in outputs
    assert project_paths.reports_root / "history/hist-report/history_report.html" in outputs
    assert (project_paths.reports_root / "history/hist-report/tables/history_temporal_metrics.csv").exists()


def test_history_report_person_monitor_includes_authored_and_mention_sections(project_config, project_paths) -> None:
    _write_history_processed_fixture(project_paths, history_run_id="hist-report-pm", source_kind="person_monitor")
    pl.DataFrame(
        {
            "match_id": ["m1", "m2"],
            "run_id": ["hist-report-pm__202601", "hist-report-pm__202602"],
            "item_type": ["comment", "post"],
            "item_id": ["c1", "p2"],
            "match_kind": ["authored_by_subject", "alias_text_mention"],
            "matched_value": ["subject", "Subject"],
            "platform": ["facebook", "facebook"],
            "container_source_id": ["external", "external"],
        }
    ).write_parquet(project_paths.processed_root / "match_hits.parquet")
    HistoryAnalysisService(project_config, project_paths).run(history_run_id="hist-report-pm")

    HistoryReportService(project_config, project_paths).run(history_run_id="hist-report-pm")

    markdown_text = (project_paths.reports_root / "history/hist-report-pm/history_report.md").read_text(encoding="utf-8")
    assert "## Person Monitor Activity" in markdown_text
    assert "Authored activity items: 1" in markdown_text
    assert "Mention activity items: 1" in markdown_text


class FakeCollectionService:
    def __init__(self, config: ProjectConfig, paths: Any) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> Any:
        assert run_id is not None
        start = self.config.date_range.start or "2026-01-01"
        window = start[:7].replace("-", "")
        raw_dir = self.paths.raw_root / run_id
        raw_dir.mkdir(parents=True, exist_ok=True)
        payload = _raw_manifest(run_id=run_id, window=window, created_at=f"{start}T10:00:00+00:00")
        (raw_dir / "manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload


class FailingSecondWindowCollectionService(FakeCollectionService):
    def run(self, run_id: str | None = None) -> Any:
        if run_id and run_id.endswith("__202602"):
            raise RuntimeError("Synthetic February failure")
        return super().run(run_id=run_id)


def _raw_manifest(*, run_id: str, window: str, created_at: str) -> dict[str, Any]:
    post_id = f"post-{window}"
    return {
        "run_id": run_id,
        "collected_at": created_at,
        "collector": "meta_api",
        "mode": "hybrid",
        "status": "success",
        "warnings": [],
        "source": {
            "platform": "facebook",
            "source_id": "page_1",
            "source_name": "Example Page",
            "source_type": "page",
            "source_kind": "feed",
            "source_collector": "meta_api",
        },
        "posts": [
            {
                "post_id": post_id,
                "platform": "facebook",
                "source_id": "page_1",
                "created_at": created_at,
                "message": f"Example Actor update {window}",
                "permalink": f"https://example.test/{post_id}",
                "comments_count": 1,
                "reactions": 2,
                "shares": 0,
                "source_collector": "meta_api",
                "author": {"author_id": "page_1", "name": "Example Page"},
                "comments": [
                    {
                        "comment_id": f"comment-{window}",
                        "platform": "facebook",
                        "parent_post_id": post_id,
                        "created_at": created_at,
                        "message": "I support Example Actor." if window == "202601" else "I oppose Example Actor.",
                        "permalink": f"https://example.test/{post_id}?comment=1",
                        "reactions": 1,
                        "source_collector": "meta_api",
                        "depth": 0,
                        "author": {"author_id": "user_1", "name": "User 1"},
                    }
                ],
            }
        ],
    }


def _write_history_processed_fixture(project_paths, *, history_run_id: str, source_kind: str) -> None:
    project_paths.processed_root.mkdir(parents=True, exist_ok=True)
    child_runs = [f"{history_run_id}__202601", f"{history_run_id}__202602"]
    pl.DataFrame(
        {
            "history_run_id": [history_run_id],
            "created_at": ["2026-04-18T00:00:00+00:00"],
            "platform": ["facebook"],
            "source_kind": [source_kind],
            "source_id": ["page_1"],
            "source_name": ["Example Page"],
            "window": ["month"],
            "start": ["2026-01-01"],
            "end": ["2026-02-28"],
            "status": ["success"],
            "child_run_ids": [child_runs],
            "warning_count": [0],
            "warnings": [[]],
        }
    ).write_parquet(project_paths.processed_root / "history_runs.parquet")
    pl.DataFrame(
        {
            "history_run_id": [history_run_id, history_run_id],
            "window_id": ["202601", "202602"],
            "child_run_id": child_runs,
            "start": ["2026-01-01", "2026-02-01"],
            "end": ["2026-01-31", "2026-02-28"],
            "status": ["success", "success"],
            "post_count": [1, 1],
            "comment_count": [1, 1],
            "propagation_count": [0, 0],
            "match_hit_count": [0, 0],
            "warning_count": [0, 0],
            "coverage_gap_total": [0, 0],
            "warnings": [[], []],
        }
    ).write_parquet(project_paths.processed_root / "history_windows.parquet")
    pl.DataFrame(
        {
            "post_id": ["p1", "p2"],
            "run_id": child_runs,
            "platform": ["facebook", "facebook"],
            "source_kind": [source_kind, source_kind],
            "created_at": ["2026-01-12T10:00:00+00:00", "2026-02-14T10:00:00+00:00"],
            "message": ["Example Actor policy update", "Example Actor crisis response"],
            "comments_count": [1, 1],
            "reactions": [10, 20],
            "shares": [1, 2],
            "views": [0, 0],
            "forwards": [0, 0],
            "reply_count": [1, 1],
        }
    ).write_parquet(project_paths.processed_root / "posts.parquet")
    pl.DataFrame(
        {
            "comment_id": ["c1", "c2"],
            "run_id": child_runs,
            "platform": ["facebook", "facebook"],
            "source_kind": [source_kind, source_kind],
            "parent_post_id": ["p1", "p2"],
            "parent_entity_type": ["post", "post"],
            "parent_entity_id": ["p1", "p2"],
            "created_at": ["2026-01-12T11:00:00+00:00", "2026-02-14T11:00:00+00:00"],
            "message": ["I support Example Actor.", "I oppose Example Actor."],
            "reactions": [1, 2],
            "depth": [0, 0],
        }
    ).write_parquet(project_paths.processed_root / "comments.parquet")
    pl.DataFrame(
        {
            "propagation_id": [],
            "run_id": [],
        }
    ).write_parquet(project_paths.processed_root / "propagations.parquet")
