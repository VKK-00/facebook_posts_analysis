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
