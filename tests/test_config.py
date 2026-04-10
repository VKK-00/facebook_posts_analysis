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


def test_project_config_accepts_telegram_web_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "telegram",
                "source_name": "example_channel",
                "telegram": {"discussion_chat_id": "example_discussion"},
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_web": {
                    "enabled": True,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {"enabled": False},
            },
        }
    )

    assert config.source.platform == "telegram"
    assert config.collector.mode == "web"
    assert config.collector.telegram_web.enabled is True


def test_project_config_accepts_telegram_bot_api_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "telegram",
                "source_name": "example_channel",
                "telegram": {"discussion_chat_id": "-100123"},
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "bot_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_bot_api": {
                    "enabled": True,
                    "bot_token": "123:token",
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {"enabled": False},
            },
        }
    )

    assert config.source.platform == "telegram"
    assert config.collector.mode == "bot_api"
    assert config.collector.telegram_bot_api.bot_token == "123:token"


def test_project_config_requires_x_bearer_token() -> None:
    with pytest.raises(ValueError):
        ProjectConfig.model_validate(
            {
                "source": {"platform": "x", "source_name": "example_account"},
                "sides": [{"side_id": "a", "name": "A"}],
                "collector": {
                    "mode": "x_api",
                    "meta_api": {"enabled": False},
                    "public_web": {"enabled": False},
                    "telegram_mtproto": {
                        "enabled": False,
                        "session_file": None,
                        "api_id": None,
                        "api_hash": None,
                    },
                    "x_api": {
                        "enabled": True,
                        "bearer_token": None,
                    },
                },
            }
        )


def test_project_config_accepts_x_api_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "x",
                "source_name": "example_account",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "x_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {
                    "enabled": True,
                    "bearer_token": "token",
                    "search_scope": "recent",
                },
            },
        }
    )

    assert config.source.platform == "x"
    assert config.collector.mode == "x_api"
    assert config.collector.x_api.bearer_token == "token"


def test_project_config_accepts_x_web_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "x",
                "source_name": "example_account",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_web": {"enabled": False},
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {
                    "enabled": True,
                    "authenticated_browser": {"enabled": False},
                },
            },
        }
    )

    assert config.source.platform == "x"
    assert config.collector.mode == "web"
    assert config.collector.x_web.enabled is True


def test_project_config_accepts_threads_api_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "threads",
                "source_name": "example_account",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "threads_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "threads_api": {"enabled": True, "access_token": "token"},
                "threads_web": {"enabled": False},
            },
        }
    )

    assert config.source.platform == "threads"
    assert config.collector.mode == "threads_api"
    assert config.collector.threads_api.access_token == "token"


def test_project_config_accepts_threads_web_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "threads",
                "source_name": "example_account",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "threads_api": {"enabled": False, "access_token": None},
                "threads_web": {"enabled": True},
            },
        }
    )

    assert config.source.platform == "threads"
    assert config.collector.mode == "web"
    assert config.collector.threads_web.enabled is True


def test_project_config_accepts_instagram_graph_api_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "instagram",
                "source_id": "17841400000000000",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "instagram_graph_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "instagram_graph_api": {"enabled": True, "access_token": "token"},
                "instagram_web": {"enabled": False},
            },
        }
    )

    assert config.source.platform == "instagram"
    assert config.collector.mode == "instagram_graph_api"
    assert config.collector.instagram_graph_api.access_token == "token"


def test_project_config_accepts_instagram_web_settings() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {
                "platform": "instagram",
                "source_name": "example_account",
            },
            "sides": [{"side_id": "a", "name": "A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "instagram_graph_api": {"enabled": False, "access_token": None},
                "instagram_web": {"enabled": True},
            },
        }
    )

    assert config.source.platform == "instagram"
    assert config.collector.mode == "web"
    assert config.collector.instagram_web.enabled is True
