from __future__ import annotations

from pathlib import Path

import pytest

from social_posts_analysis.collectors.base import CollectorUnavailableError
from social_posts_analysis.collectors.web_runtime import open_authenticated_web_runtime
from social_posts_analysis.config import AuthenticatedBrowserConfig


class FakeContext:
    def close(self) -> None:
        return None


class FakeChromium:
    def __init__(self, failures_before_success: int) -> None:
        self.failures_before_success = failures_before_success
        self.calls: list[dict[str, object]] = []

    def launch_persistent_context(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(kwargs)
        if len(self.calls) <= self.failures_before_success:
            raise RuntimeError(f"launch failed #{len(self.calls)}")
        return FakeContext()


class FakePlaywright:
    def __init__(self, failures_before_success: int) -> None:
        self.chromium = FakeChromium(failures_before_success=failures_before_success)


def test_open_authenticated_web_runtime_retries_without_profile_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    created_dirs: list[Path] = []

    def fake_prepare_temp_profile_directory(**_kwargs) -> Path:  # noqa: ANN003
        target = tmp_path / f"profile-{len(created_dirs) + 1}"
        target.mkdir()
        created_dirs.append(target)
        return target

    monkeypatch.setattr(
        "social_posts_analysis.collectors.web_runtime.prepare_temp_profile_directory",
        fake_prepare_temp_profile_directory,
    )

    playwright = FakePlaywright(failures_before_success=1)
    runtime = open_authenticated_web_runtime(
        playwright,
        headless=False,
        browser_channel=None,
        viewport={"width": 1400, "height": 1600},
        authenticated_browser=AuthenticatedBrowserConfig(
            enabled=True,
            browser="chrome",
            profile_directory="Default",
            copy_profile=True,
        ),
        source_user_data_dir=tmp_path / "source-profile",
        locale="en-US",
        profile_copy_prefix="test-auth-",
        best_effort_profile_copy=True,
    )

    assert len(playwright.chromium.calls) == 2
    assert playwright.chromium.calls[0]["args"] == ["--profile-directory=Default"]
    assert playwright.chromium.calls[1]["args"] == []
    assert created_dirs[0].exists() is False
    assert runtime.temp_profile_dir == created_dirs[1]
    assert "Using authenticated browser profile snapshot" in runtime.warnings[0]
    assert "Authenticated browser launch fallback used: without --profile-directory." in runtime.warnings[1]


def test_open_authenticated_web_runtime_retries_headful_and_raises_if_all_attempts_fail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    created_dirs: list[Path] = []

    def fake_prepare_temp_profile_directory(**_kwargs) -> Path:  # noqa: ANN003
        target = tmp_path / f"profile-{len(created_dirs) + 1}"
        target.mkdir()
        created_dirs.append(target)
        return target

    monkeypatch.setattr(
        "social_posts_analysis.collectors.web_runtime.prepare_temp_profile_directory",
        fake_prepare_temp_profile_directory,
    )

    playwright = FakePlaywright(failures_before_success=10)
    with pytest.raises(CollectorUnavailableError) as exc_info:
        open_authenticated_web_runtime(
            playwright,
            headless=True,
            browser_channel=None,
            viewport={"width": 1400, "height": 1600},
            authenticated_browser=AuthenticatedBrowserConfig(
                enabled=True,
                browser="chrome",
                profile_directory="Default",
                copy_profile=True,
            ),
            source_user_data_dir=tmp_path / "source-profile",
            locale="en-US",
            profile_copy_prefix="test-auth-",
            best_effort_profile_copy=True,
        )

    assert len(playwright.chromium.calls) == 4
    assert "requested" in str(exc_info.value)
    assert "without --profile-directory" in str(exc_info.value)
    assert "headful retry" in str(exc_info.value)
    assert "headful retry without --profile-directory" in str(exc_info.value)
    assert all(directory.exists() is False for directory in created_dirs)
