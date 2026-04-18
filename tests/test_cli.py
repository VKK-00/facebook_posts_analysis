from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

import social_posts_analysis.cli as cli


def test_doctor_instagram_web_writes_diagnostic_json(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.yaml"
    config_path.write_text(
        """
project_name: cli-test
source:
  platform: instagram
  source_name: example_account
sides:
  - side_id: side_a
    name: Actor A
collector:
  mode: web
  instagram_web:
    enabled: true
paths:
  raw_dir: data/raw
  processed_dir: data/processed
  review_dir: review
  reports_dir: reports
  database_path: data/processed/social_posts_analysis.duckdb
""".strip(),
        encoding="utf-8",
    )

    class FakeInstagramWebCollector:
        def __init__(self, config: Any) -> None:
            self.config = config

        def diagnose_browser_session(self, target_url: str | None) -> dict[str, Any]:
            return {
                "collector": "instagram_web",
                "target_url": target_url,
                "final_url": target_url,
                "target_status_id": "",
                "target_author_username": "",
                "authenticated_browser_enabled": True,
                "browser": "chrome",
                "profile_directory": "Default",
                "copy_profile": True,
                "status": "login_wall",
                "page_state": {
                    "login_wall_detected": True,
                    "profile_unavailable_detected": False,
                    "serialized_data_detected": False,
                    "body_text_length": 14,
                },
                "extraction_sources": {
                    "post_links": 0,
                    "json_script_blocks": 0,
                    "media_candidates": 0,
                    "comment_candidates": 0,
                    "target_media_candidates": 0,
                    "other_media_candidates": 0,
                },
                "serialized_candidates": {"media": [], "target_media": [], "other_media": [], "comments": []},
                "serialized_structure": {
                    "scripts_analyzed": 0,
                    "parse_errors": 0,
                    "top_level_types": [],
                    "top_level_keys": [],
                    "key_paths": [],
                    "marker_keys": [],
                    "shape_samples": [],
                },
                "warnings": ["Instagram returned login/signup UI."],
                "body_sample": "Log In Sign Up",
            }

    monkeypatch.setattr(cli, "InstagramWebCollector", FakeInstagramWebCollector)

    result = CliRunner().invoke(
        cli.app,
        [
            "doctor-instagram-web",
            "--config",
            str(config_path),
            "--target-url",
            "https://www.instagram.com/nasa/",
            "--run-id",
            "doctor-run-1",
        ],
    )

    assert result.exit_code == 0
    diagnostic_path = project_root / "data/raw/_diagnostics/doctor-run-1/instagram_web_session.json"
    payload = json.loads(diagnostic_path.read_text(encoding="utf-8"))
    assert payload["status"] == "login_wall"
    assert payload["target_url"] == "https://www.instagram.com/nasa/"
    assert payload["target_status_id"] == ""
    assert payload["page_state"]["login_wall_detected"] is True
    assert payload["serialized_candidates"] == {"media": [], "target_media": [], "other_media": [], "comments": []}
    assert payload["serialized_structure"]["scripts_analyzed"] == 0
    assert "Instagram web diagnostic written" in result.output


def test_openclaw_export_writes_bundle_json(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    raw_dir = project_root / "data/raw/run-1"
    config_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    config_path = config_dir / "project.yaml"
    config_path.write_text(
        """
project_name: openclaw-cli-test
source:
  platform: facebook
  source_id: page_1
  source_name: Example Page
sides:
  - side_id: side_a
    name: Actor A
collector:
  mode: hybrid
paths:
  raw_dir: data/raw
  processed_dir: data/processed
  review_dir: review
  reports_dir: reports
  database_path: data/processed/social_posts_analysis.duckdb
""".strip(),
        encoding="utf-8",
    )
    (raw_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "collector": "facebook_web",
                "mode": "web",
                "status": "success",
                "warnings": [],
                "source": {
                    "platform": "facebook",
                    "source_kind": "feed",
                    "source_id": "page_1",
                    "source_name": "Example Page",
                    "source_type": "page",
                },
                "posts": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "openclaw-export",
            "--config",
            str(config_path),
            "--run-id",
            "run-1",
        ],
    )

    assert result.exit_code == 0
    bundle_path = project_root / "reports/openclaw/run-1/bundle.json"
    assert bundle_path.exists()
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "openclaw.social_posts_analysis.v1"
    assert payload["run_id"] == "run-1"
    assert "OpenClaw bundle written" in result.output


def test_openclaw_export_reports_missing_run(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.yaml"
    config_path.write_text(
        """
project_name: openclaw-cli-test
source:
  platform: facebook
  source_id: page_1
  source_name: Example Page
sides:
  - side_id: side_a
    name: Actor A
collector:
  mode: hybrid
paths:
  raw_dir: data/raw
  processed_dir: data/processed
  review_dir: review
  reports_dir: reports
  database_path: data/processed/social_posts_analysis.duckdb
""".strip(),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "openclaw-export",
            "--config",
            str(config_path),
            "--run-id",
            "missing-run",
        ],
    )

    assert result.exit_code != 0
    assert "OpenClaw export requires an existing run_id" in result.output


def test_history_run_cli_writes_parent_manifest(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    config_path = _write_history_cli_config(project_root)

    class FakeHistoryBackfillService:
        def __init__(self, config: Any, paths: Any) -> None:
            self.paths = paths

        def run(self, history_run_id: str | None = None) -> dict[str, Any]:
            resolved = history_run_id or "hist-generated"
            history_dir = self.paths.raw_root / "_history" / resolved
            history_dir.mkdir(parents=True, exist_ok=True)
            (history_dir / "manifest.json").write_text(
                json.dumps({"history_run_id": resolved, "status": "success"}, ensure_ascii=False),
                encoding="utf-8",
            )
            return {"history_run_id": resolved, "status": "success", "windows": []}

    monkeypatch.setattr(cli, "HistoricalBackfillService", FakeHistoryBackfillService)

    result = CliRunner().invoke(
        cli.app,
        ["history-run", "--config", str(config_path), "--history-run-id", "hist-cli"],
    )

    assert result.exit_code == 0
    assert (project_root / "data/raw/_history/hist-cli/manifest.json").exists()
    assert "Historical run completed: hist-cli" in result.output


def test_history_report_cli_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    config_path = _write_history_cli_config(project_root)

    class FakeHistoryAnalysisService:
        def __init__(self, config: Any, paths: Any) -> None:
            pass

        def run(self, history_run_id: str) -> dict[str, Any]:
            return {"history_run_id": history_run_id, "item_count": 0}

    class FakeHistoryReportService:
        def __init__(self, config: Any, paths: Any) -> None:
            self.paths = paths

        def run(self, history_run_id: str) -> list[Path]:
            output_dir = self.paths.reports_root / "history" / history_run_id
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "history_report.md"
            output_path.write_text("# report", encoding="utf-8")
            return [output_path]

    monkeypatch.setattr(cli, "HistoryAnalysisService", FakeHistoryAnalysisService)
    monkeypatch.setattr(cli, "HistoryReportService", FakeHistoryReportService)

    result = CliRunner().invoke(
        cli.app,
        ["history-report", "--config", str(config_path), "--history-run-id", "hist-cli"],
    )

    assert result.exit_code == 0
    assert "History report files written" in result.output


def test_openclaw_export_rejects_run_id_and_history_run_id_together(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_path = _write_history_cli_config(project_root)

    result = CliRunner().invoke(
        cli.app,
        [
            "openclaw-export",
            "--config",
            str(config_path),
            "--run-id",
            "run-1",
            "--history-run-id",
            "hist-1",
        ],
    )

    assert result.exit_code != 0
    assert "--run-id and --history-run-id are mutually exclusive" in result.output


def test_openclaw_export_history_run_writes_bundle(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    config_path = _write_history_cli_config(project_root)

    class FakeOpenClawExportService:
        def __init__(self, config: Any, paths: Any) -> None:
            self.paths = paths

        def run_history(self, history_run_id: str) -> Any:
            output_dir = self.paths.reports_root / "openclaw" / history_run_id
            output_dir.mkdir(parents=True, exist_ok=True)
            bundle_path = output_dir / "bundle.json"
            brief_path = output_dir / "brief.md"
            bundle_path.write_text("{}", encoding="utf-8")
            brief_path.write_text("# brief", encoding="utf-8")
            return type("Outputs", (), {"bundle_path": bundle_path, "brief_path": brief_path})()

    monkeypatch.setattr(cli, "OpenClawExportService", FakeOpenClawExportService)

    result = CliRunner().invoke(
        cli.app,
        ["openclaw-export", "--config", str(config_path), "--history-run-id", "hist-cli"],
    )

    assert result.exit_code == 0
    assert (project_root / "reports/openclaw/hist-cli/bundle.json").exists()
    assert "OpenClaw bundle written" in result.output


def _write_history_cli_config(project_root: Path) -> Path:
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.yaml"
    config_path.write_text(
        """
project_name: history-cli-test
source:
  platform: facebook
  source_id: page_1
  source_name: Example Page
sides:
  - side_id: side_a
    name: Actor A
collector:
  mode: hybrid
history:
  start: "2026-01-01"
  end: "2026-01-31"
paths:
  raw_dir: data/raw
  processed_dir: data/processed
  review_dir: review
  reports_dir: reports
  database_path: data/processed/social_posts_analysis.duckdb
""".strip(),
        encoding="utf-8",
    )
    return config_path
