from __future__ import annotations

import os

import pytest
from playwright.sync_api import sync_playwright

from social_posts_analysis.collectors.web_runtime import open_web_runtime
from social_posts_analysis.config import AuthenticatedBrowserConfig


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.skipif(os.getenv("SPA_RUN_AUTH_RUNTIME_LIVE") != "1", reason="manual authenticated browser live test")
def test_open_web_runtime_authenticated_live() -> None:
    config = AuthenticatedBrowserConfig(
        enabled=True,
        browser=os.getenv("SPA_AUTH_BROWSER", "chrome"),
        user_data_dir=os.getenv("SPA_AUTH_USER_DATA_DIR"),
        profile_directory=os.getenv("SPA_AUTH_PROFILE_DIRECTORY", "Default"),
        copy_profile=_env_flag("SPA_AUTH_COPY_PROFILE", True),
        temp_root_dir=os.getenv("SPA_AUTH_TEMP_ROOT_DIR"),
    )
    with sync_playwright() as playwright:
        runtime = open_web_runtime(
            playwright,
            headless=_env_flag("SPA_AUTH_HEADLESS", True),
            browser_channel=None,
            viewport={"width": 1400, "height": 1600},
            authenticated_browser=config,
            custom_user_data_error="custom dir required",
            missing_user_data_error_prefix="missing user data dir",
            best_effort_profile_copy=True,
        )
        try:
            page = runtime.context.new_page()
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
            assert "facebook.com" in page.url
        finally:
            runtime.close()
