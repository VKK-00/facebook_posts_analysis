from __future__ import annotations

import pytest

from facebook_posts_analysis.config import ProjectConfig


def test_project_config_requires_page_reference_and_sides() -> None:
    with pytest.raises(Exception):
        ProjectConfig.model_validate(
            {
                "page": {},
                "sides": [],
            }
        )


def test_project_config_accepts_authenticated_browser_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "page": {"url": "https://www.facebook.com/example"},
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "web",
                "multi_pass_runs": 2,
                "wait_between_passes_seconds": 0.5,
                "public_web": {
                    "enabled": True,
                    "authenticated_browser": {
                        "enabled": True,
                        "browser": "chrome",
                        "profile_directory": "Default",
                        "copy_profile": True,
                    },
                },
            },
            "normalization": {"merge_recent_runs": 3},
        }
    )

    assert config.collector.public_web.authenticated_browser.enabled is True
    assert config.collector.public_web.authenticated_browser.profile_directory == "Default"
    assert config.collector.multi_pass_runs == 2
    assert config.collector.wait_between_passes_seconds == 0.5
    assert config.normalization.merge_recent_runs == 3
