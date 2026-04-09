from __future__ import annotations

import pytest

from social_posts_analysis.config import ProjectConfig


def test_project_config_requires_source_reference_and_sides() -> None:
    with pytest.raises(ValueError):
        ProjectConfig.model_validate(
            {
                "source": {"platform": "facebook"},
                "sides": [],
            }
        )


def test_project_config_accepts_authenticated_browser_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "facebook",
                "url": "https://www.facebook.com/example",
            },
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
                "meta_api": {"enabled": False},
            },
            "normalization": {"merge_recent_runs": 3},
        }
    )

    assert config.collector.public_web.authenticated_browser.enabled is True
    assert config.collector.public_web.authenticated_browser.profile_directory == "Default"
    assert config.collector.multi_pass_runs == 2
    assert config.collector.wait_between_passes_seconds == 0.5
    assert config.normalization.merge_recent_runs == 3


def test_project_config_requires_telegram_credentials_for_mtproto() -> None:
    with pytest.raises(ValueError):
        ProjectConfig.model_validate(
            {
                "source": {"platform": "telegram", "source_name": "example_channel"},
                "sides": [{"side_id": "a", "name": "A"}],
                "collector": {
                    "mode": "mtproto",
                    "telegram_mtproto": {
                        "enabled": True,
                        "session_file": None,
                        "api_id": None,
                        "api_hash": None,
                    },
                    "meta_api": {"enabled": False},
                    "public_web": {"enabled": False},
                },
            }
        )


def test_project_config_accepts_telegram_mtproto_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "telegram",
                "source_name": "example_channel",
                "telegram": {"discussion_chat_id": "-100123"},
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "mtproto",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": True,
                    "session_file": ".sessions/example",
                    "api_id": 12345,
                    "api_hash": "hash",
                },
            },
        }
    )

    assert config.source.platform == "telegram"
    assert config.collector.telegram_mtproto.session_file == ".sessions/example"
    assert config.source.telegram.discussion_chat_id == "-100123"
