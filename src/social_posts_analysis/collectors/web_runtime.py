from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from social_posts_analysis.config import AuthenticatedBrowserConfig

from .base import CollectorUnavailableError

_IGNORED_DIRECTORY_NAMES = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "ShaderCache",
    "DawnCache",
    "Media Cache",
    "blob_storage",
}


@dataclass(slots=True)
class WebCollectorRuntime:
    browser: Any | None
    context: Any
    temp_profile_dir: Path | None
    warnings: list[str]

    def close(self) -> None:
        self.context.close()
        if self.browser is not None:
            self.browser.close()
        if self.temp_profile_dir is not None:
            shutil.rmtree(self.temp_profile_dir, ignore_errors=True)


@dataclass(frozen=True, slots=True)
class AuthenticatedLaunchStrategy:
    label: str
    channel: str | None
    headless: bool
    use_profile_arg: bool


def ensure_playwright_available(error_message: str) -> None:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError as exc:
        raise CollectorUnavailableError(error_message) from exc


def open_web_runtime(
    playwright: Any,
    *,
    headless: bool,
    browser_channel: str | None,
    viewport: dict[str, int],
    authenticated_browser: AuthenticatedBrowserConfig | None = None,
    locale: str = "en-US",
    profile_copy_prefix: str = "web-profile-",
    custom_user_data_error: str,
    missing_user_data_error_prefix: str = "Authenticated browser user data dir does not exist",
    best_effort_profile_copy: bool = False,
) -> WebCollectorRuntime:
    if authenticated_browser is None or not authenticated_browser.enabled:
        browser = playwright.chromium.launch(headless=headless, channel=browser_channel)
        context = browser.new_context(locale=locale, viewport=viewport)
        return WebCollectorRuntime(browser=browser, context=context, temp_profile_dir=None, warnings=[])

    source_user_data_dir = resolve_authenticated_user_data_dir(
        authenticated_browser,
        custom_user_data_error=custom_user_data_error,
        missing_user_data_error_prefix=missing_user_data_error_prefix,
    )
    return open_authenticated_web_runtime(
        playwright,
        headless=headless,
        browser_channel=browser_channel,
        viewport=viewport,
        authenticated_browser=authenticated_browser,
        source_user_data_dir=source_user_data_dir,
        locale=locale,
        profile_copy_prefix=profile_copy_prefix,
        best_effort_profile_copy=best_effort_profile_copy,
    )


def open_authenticated_web_runtime(
    playwright: Any,
    *,
    headless: bool,
    browser_channel: str | None,
    viewport: dict[str, int],
    authenticated_browser: AuthenticatedBrowserConfig,
    source_user_data_dir: Path,
    locale: str,
    profile_copy_prefix: str,
    best_effort_profile_copy: bool,
) -> WebCollectorRuntime:
    profile_directory = authenticated_browser.profile_directory
    strategies = authenticated_launch_strategies(
        headless=headless,
        channel=resolve_authenticated_browser_channel(authenticated_browser.browser, browser_channel),
        profile_directory=profile_directory,
    )
    failure_messages: list[str] = []
    snapshot_warning = (
        f"Using authenticated browser profile snapshot from {source_user_data_dir} ({profile_directory})."
        if authenticated_browser.copy_profile
        else None
    )

    for strategy in strategies:
        temp_profile_dir: Path | None = None
        launch_user_data_dir = source_user_data_dir
        try:
            if authenticated_browser.copy_profile:
                temp_profile_dir = prepare_temp_profile_directory(
                    source_user_data_dir=source_user_data_dir,
                    profile_directory=profile_directory,
                    temp_root_dir=authenticated_browser.temp_root_dir,
                    prefix=profile_copy_prefix,
                    best_effort=best_effort_profile_copy,
                )
                launch_user_data_dir = temp_profile_dir

            args = [f"--profile-directory={profile_directory}"] if profile_directory and strategy.use_profile_arg else []
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(launch_user_data_dir),
                channel=strategy.channel,
                headless=strategy.headless,
                locale=locale,
                viewport=viewport,
                args=args,
            )
            warnings: list[str] = []
            if snapshot_warning:
                warnings.append(snapshot_warning)
            if strategy.label != "requested":
                warnings.append(f"Authenticated browser launch fallback used: {strategy.label}.")
            return WebCollectorRuntime(browser=None, context=context, temp_profile_dir=temp_profile_dir, warnings=warnings)
        except Exception as exc:
            if temp_profile_dir is not None:
                shutil.rmtree(temp_profile_dir, ignore_errors=True)
            failure_messages.append(f"{strategy.label}: {summarize_launch_exception(exc)}")

    attempts = "; ".join(failure_messages)
    raise CollectorUnavailableError(
        f"Failed to launch authenticated browser context after {len(strategies)} attempts. {attempts}"
    )


def authenticated_launch_strategies(
    *,
    headless: bool,
    channel: str | None,
    profile_directory: str,
) -> list[AuthenticatedLaunchStrategy]:
    strategies = [
        AuthenticatedLaunchStrategy(
            label="requested",
            channel=channel,
            headless=headless,
            use_profile_arg=bool(profile_directory),
        )
    ]
    if profile_directory:
        strategies.append(
            AuthenticatedLaunchStrategy(
                label="without --profile-directory",
                channel=channel,
                headless=headless,
                use_profile_arg=False,
            )
        )
    if headless:
        strategies.append(
            AuthenticatedLaunchStrategy(
                label="headful retry",
                channel=channel,
                headless=False,
                use_profile_arg=bool(profile_directory),
            )
        )
        if profile_directory:
            strategies.append(
                AuthenticatedLaunchStrategy(
                    label="headful retry without --profile-directory",
                    channel=channel,
                    headless=False,
                    use_profile_arg=False,
                )
            )
    return strategies


def summarize_launch_exception(exc: Exception) -> str:
    message = str(exc).strip().replace("\n", " ")
    if not message:
        return exc.__class__.__name__
    return f"{exc.__class__.__name__}: {message}"


def resolve_authenticated_user_data_dir(
    authenticated_browser: AuthenticatedBrowserConfig,
    *,
    custom_user_data_error: str,
    missing_user_data_error_prefix: str,
) -> Path:
    if authenticated_browser.user_data_dir:
        resolved_path = Path(os.path.expandvars(authenticated_browser.user_data_dir)).expanduser()
    elif authenticated_browser.browser == "chrome":
        resolved_path = Path(os.getenv("LOCALAPPDATA", "")) / "Google/Chrome/User Data"
    elif authenticated_browser.browser == "edge":
        resolved_path = Path(os.getenv("LOCALAPPDATA", "")) / "Microsoft/Edge/User Data"
    else:
        raise CollectorUnavailableError(custom_user_data_error)

    if not resolved_path.exists():
        raise CollectorUnavailableError(f"{missing_user_data_error_prefix}: {resolved_path}")
    profile_directory = authenticated_browser.profile_directory
    if profile_directory and not (resolved_path / profile_directory).exists():
        raise CollectorUnavailableError(f"Browser profile directory does not exist: {resolved_path / profile_directory}")
    return resolved_path


def resolve_authenticated_browser_channel(browser_name: str, browser_channel: str | None) -> str | None:
    if browser_name == "custom":
        return browser_channel
    if browser_channel:
        return browser_channel
    if browser_name == "edge":
        return "msedge"
    return browser_name


def prepare_temp_profile_directory(
    *,
    source_user_data_dir: Path,
    profile_directory: str,
    temp_root_dir: str | None,
    prefix: str,
    best_effort: bool,
) -> Path:
    target_parent = (
        Path(os.path.expandvars(temp_root_dir)).expanduser()
        if temp_root_dir
        else Path(tempfile.gettempdir())
    )
    target_parent.mkdir(parents=True, exist_ok=True)
    temp_profile_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=str(target_parent)))

    for root_file_name in ("Local State", "First Run"):
        source_file = source_user_data_dir / root_file_name
        if source_file.exists():
            try:
                shutil.copy2(source_file, temp_profile_dir / root_file_name)
            except OSError:
                if not best_effort:
                    raise

    source_profile_dir = source_user_data_dir / profile_directory
    target_profile_dir = temp_profile_dir / profile_directory
    if best_effort:
        copy_directory_best_effort(source_profile_dir, target_profile_dir)
    else:
        shutil.copytree(source_profile_dir, target_profile_dir, dirs_exist_ok=True)
    return temp_profile_dir


def copy_directory_best_effort(source_directory: Path, target_directory: Path) -> None:
    for root, directory_names, file_names in os.walk(source_directory):
        directory_names[:] = [name for name in directory_names if name not in _IGNORED_DIRECTORY_NAMES]
        root_path = Path(root)
        relative_path = root_path.relative_to(source_directory)
        destination_root = target_directory / relative_path
        destination_root.mkdir(parents=True, exist_ok=True)
        for file_name in file_names:
            source_file = root_path / file_name
            target_file = destination_root / file_name
            try:
                shutil.copy2(source_file, target_file)
            except OSError:
                continue


def scroll_page(page: Any, *, max_scrolls: int, wait_after_scroll_ms: int, passes: int | None = None, wheel_y: int = 2600) -> None:
    for _ in range(passes or max_scrolls):
        page.mouse.wheel(0, wheel_y)
        page.wait_for_timeout(wait_after_scroll_ms)
