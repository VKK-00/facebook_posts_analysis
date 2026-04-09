from __future__ import annotations

import os
import re
import shutil
import tempfile
import unicodedata
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import slugify, stable_id, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError


class PublicWebCollector(BaseCollector):
    name = "public_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.public_web
        if not self.settings.enabled:
            raise CollectorUnavailableError("Public web collector is disabled in config.collector.public_web.enabled.")
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
        except ImportError as exc:
            raise CollectorUnavailableError(
                "Public web collector requires the playwright package and browser install."
            ) from exc

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings: list[str] = [
            "Public web extraction is best-effort and may require selector tuning for the target page."
        ]
        page_url = self.config.source.url or ""
        if not page_url:
            raise CollectorUnavailableError("Public web collector requires source.url.")

        with sync_playwright() as playwright:
            browser, context, temp_profile_dir, context_warnings = self._open_collection_context(playwright)
            warnings.extend(context_warnings)
            discovery_browser = browser
            discovery_context = context
            try:
                page_name = self.config.source.source_name
                page_payload: dict[str, Any] | None = None
                direct_feed_candidates: list[dict[str, Any]] = []
                plugin_candidates: list[dict[str, Any]] = []

                if self._uses_authenticated_browser():
                    page_payload, direct_feed_candidates = self._collect_direct_feed_candidates(
                        context=context,
                        page_url=page_url,
                        raw_store=raw_store,
                        snapshot_name="authenticated_feed_candidates",
                    )
                    if not page_name:
                        page_name = (page_payload.get("title") or "").split("|", 1)[0].strip() or None
                    discovery_browser, discovery_context = self._open_public_context(playwright)

                if discovery_context is not None:
                    plugin_page = discovery_context.new_page()
                    plugin_page.goto(
                        self._page_plugin_url(page_url),
                        wait_until="domcontentloaded",
                        timeout=int(self.settings.timeout_seconds * 1000),
                    )
                    plugin_page.wait_for_timeout(4000)
                    plugin_candidates = self._postprocess_candidates(self._extract_plugin_feed_candidates(plugin_page))
                    plugin_payload: dict[str, Any] = {
                        "title": plugin_page.title(),
                        "url": plugin_page.url,
                        "candidates": plugin_candidates,
                    }
                    raw_store.write_json("web_extract", "page_plugin_candidates", plugin_payload)

                    if not page_name:
                        inferred_name = plugin_page.locator("body").inner_text().splitlines()
                        page_name = next((line.strip() for line in inferred_name if line.strip()), None)
                    plugin_page.close()
                    if page_payload is None:
                        page_payload = {
                            "title": plugin_payload["title"],
                            "url": plugin_payload["url"],
                        }

                video_candidates = self._discover_media_candidates(
                    context=discovery_context,
                    page_url=page_url,
                    tab_name="videos",
                    raw_store=raw_store,
                )
                photo_candidates = self._discover_media_candidates(
                    context=discovery_context,
                    page_url=page_url,
                    tab_name="photos",
                    raw_store=raw_store,
                )
                reel_candidates = self._discover_media_candidates(
                    context=discovery_context,
                    page_url=page_url,
                    tab_name="reels",
                    raw_store=raw_store,
                )
                candidates = direct_feed_candidates + plugin_candidates + video_candidates + photo_candidates + reel_candidates
                if not candidates:
                    page_payload, fallback_candidates = self._collect_direct_feed_candidates(
                        context=discovery_context,
                        page_url=page_url,
                        raw_store=raw_store,
                        snapshot_name="feed_candidates",
                    )
                    candidates = fallback_candidates
                    if not page_name:
                        page_name = (page_payload.get("title") or "").split("|", 1)[0].strip() or None

                page_path = raw_store.write_json("web_page", "page_metadata", page_payload)

                page_id = self.config.source.source_id or stable_id(page_url or "page")
                page_name = page_name or "Facebook Page"
                posts_by_id: dict[str, PostSnapshot] = {}
                seen_permalinks: set[str] = set()

                for candidate in candidates:
                    permalink = candidate.get("permalink") or ""
                    if not permalink or permalink in seen_permalinks:
                        continue
                    seen_permalinks.add(permalink)
                    published_at = candidate.get("published_at") or self._parse_post_timestamp(candidate.get("published_hint") or "")
                    if self.config.date_range.start or self.config.date_range.end:
                        if published_at and not self._within_configured_range(published_at):
                            continue
                    post = self._collect_post_detail(
                        context=context,
                        page_id=page_id,
                        page_name=page_name,
                        candidate=candidate,
                        published_at=published_at,
                        raw_store=raw_store,
                    )
                    if self.config.date_range.start or self.config.date_range.end:
                        if post.created_at is None:
                            warnings.append(f"Skipped post with unparsed timestamp: {permalink}")
                            continue
                        if not self._within_configured_range(post.created_at):
                            continue
                    existing = posts_by_id.get(post.post_id)
                    posts_by_id[post.post_id] = self._merge_post_snapshots(existing, post)

                if discovery_browser is not None:
                    try:
                        mobile_posts = self._collect_mobile_timeline_posts(
                            browser=discovery_browser,
                            page_url=page_url,
                            page_id=page_id,
                            page_name=page_name,
                            raw_store=raw_store,
                        )
                    except Exception as exc:
                        warnings.append(f"Mobile timeline fallback failed: {exc}")
                        mobile_posts = []
                    for post in mobile_posts:
                        self._upsert_post_snapshot(posts_by_id, post)

                posts = list(posts_by_id.values())
            finally:
                if discovery_context is not context:
                    discovery_context.close()
                context.close()
                if discovery_browser is not browser and discovery_browser is not None:
                    discovery_browser.close()
                if browser is not None:
                    browser.close()
                if temp_profile_dir is not None:
                    shutil.rmtree(temp_profile_dir, ignore_errors=True)

        if not posts:
            warnings.append("No post pages were collected from the public web feed for the configured range.")

        page_snapshot = SourceSnapshot(
            platform="facebook",
            source_id=page_id,
            source_name=page_name,
            source_url=self.config.source.url,
            source_type="page",
            source_collector=self.name,
            raw_path=str(page_path),
        )

        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=warnings,
            source=page_snapshot,
            posts=posts,
        )

    def _open_collection_context(self, playwright: Any) -> tuple[Any | None, Any, Path | None, list[str]]:
        if self._uses_authenticated_browser():
            context, temp_profile_dir, warnings = self._open_authenticated_context(playwright)
            return None, context, temp_profile_dir, warnings

        return (*self._open_public_context(playwright), None, [])

    def _open_public_context(self, playwright: Any) -> tuple[Any, Any]:
        browser = playwright.chromium.launch(
            headless=self.settings.headless,
            channel=self.settings.browser_channel,
        )
        context = browser.new_context(locale="en-US", viewport={"width": 1400, "height": 1600})
        return browser, context

    def _open_authenticated_context(self, playwright: Any) -> tuple[Any, Path | None, list[str]]:
        source_user_data_dir = self._resolve_authenticated_user_data_dir()
        profile_directory = self.settings.authenticated_browser.profile_directory
        launch_user_data_dir = source_user_data_dir
        temp_profile_dir: Path | None = None
        warnings: list[str] = []
        if self.settings.authenticated_browser.copy_profile:
            temp_profile_dir = self._prepare_temp_profile_directory(source_user_data_dir, profile_directory)
            launch_user_data_dir = temp_profile_dir
            warnings.append(
                f"Using authenticated browser profile snapshot from {source_user_data_dir} ({profile_directory})."
            )

        args = [f"--profile-directory={profile_directory}"] if profile_directory else []
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(launch_user_data_dir),
            channel=self._resolve_authenticated_browser_channel(),
            headless=self.settings.headless,
            locale="en-US",
            viewport={"width": 1400, "height": 1600},
            args=args,
        )
        return context, temp_profile_dir, warnings

    def _resolve_authenticated_user_data_dir(self) -> Path:
        auth_settings = self.settings.authenticated_browser
        if auth_settings.user_data_dir:
            resolved_path = Path(os.path.expandvars(auth_settings.user_data_dir)).expanduser()
        elif auth_settings.browser == "chrome":
            resolved_path = Path(os.getenv("LOCALAPPDATA", "")) / "Google/Chrome/User Data"
        elif auth_settings.browser == "edge":
            resolved_path = Path(os.getenv("LOCALAPPDATA", "")) / "Microsoft/Edge/User Data"
        else:
            raise CollectorUnavailableError(
                "Authenticated browser mode requires public_web.authenticated_browser.user_data_dir for browser='custom'."
            )

        if not resolved_path.exists():
            raise CollectorUnavailableError(f"Browser user data directory does not exist: {resolved_path}")
        profile_directory = auth_settings.profile_directory
        if profile_directory and not (resolved_path / profile_directory).exists():
            raise CollectorUnavailableError(
                f"Browser profile directory does not exist: {resolved_path / profile_directory}"
            )
        return resolved_path

    def _resolve_authenticated_browser_channel(self) -> str | None:
        if self.settings.browser_channel:
            return self.settings.browser_channel
        browser_name = self.settings.authenticated_browser.browser
        if browser_name == "chrome":
            return "chrome"
        if browser_name == "edge":
            return "msedge"
        return None

    def _prepare_temp_profile_directory(self, source_user_data_dir: Path, profile_directory: str) -> Path:
        temp_root_dir = self.settings.authenticated_browser.temp_root_dir
        target_parent = (
            Path(os.path.expandvars(temp_root_dir)).expanduser()
            if temp_root_dir
            else Path(tempfile.gettempdir())
        )
        target_parent.mkdir(parents=True, exist_ok=True)
        temp_profile_dir = Path(tempfile.mkdtemp(prefix="facebook-posts-auth-", dir=str(target_parent)))

        for root_file_name in ("Local State", "First Run"):
            source_file = source_user_data_dir / root_file_name
            if source_file.exists():
                try:
                    shutil.copy2(source_file, temp_profile_dir / root_file_name)
                except OSError:
                    continue

        self._copy_directory_best_effort(
            source_directory=source_user_data_dir / profile_directory,
            target_directory=temp_profile_dir / profile_directory,
        )
        return temp_profile_dir

    @staticmethod
    def _copy_directory_best_effort(source_directory: Path, target_directory: Path) -> None:
        ignored_directory_names = {
            "Cache",
            "Code Cache",
            "GPUCache",
            "GrShaderCache",
            "ShaderCache",
            "DawnCache",
            "Media Cache",
            "blob_storage",
        }
        for root, directory_names, file_names in os.walk(source_directory):
            directory_names[:] = [name for name in directory_names if name not in ignored_directory_names]
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

    def _collect_direct_feed_candidates(
        self,
        context: Any,
        page_url: str,
        raw_store: RawSnapshotStore,
        snapshot_name: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        feed_page = context.new_page()
        feed_page.goto(
            self._with_locale(page_url),
            wait_until="domcontentloaded",
            timeout=int(self.settings.timeout_seconds * 1000),
        )
        feed_page.wait_for_timeout(4000)
        for _ in range(self.settings.max_scrolls):
            feed_page.mouse.wheel(0, 3500)
            feed_page.wait_for_timeout(self.settings.wait_after_scroll_ms)

        candidates = self._postprocess_candidates(self._extract_feed_candidates(feed_page))
        payload = {
            "title": feed_page.title(),
            "url": feed_page.url,
            "candidates": candidates,
        }
        raw_store.write_json("web_extract", snapshot_name, payload)
        feed_page.close()
        return payload, candidates

    def _uses_authenticated_browser(self) -> bool:
        return self.settings.authenticated_browser.enabled

    def _collect_post_detail(
        self,
        context: Any,
        page_id: str,
        page_name: str,
        candidate: dict[str, Any],
        published_at: str | None,
        raw_store: RawSnapshotStore,
    ) -> PostSnapshot:
        detail_url = self._with_locale(candidate.get("detail_url") or candidate["permalink"])
        target_comment_count = int(candidate.get("comments_count") or 0)
        attempts = [
            {"aggressive": False, "comment_limit": self._comment_article_limit(target_comment_count, aggressive=False)},
            {"aggressive": True, "comment_limit": self._comment_article_limit(target_comment_count, aggressive=True)},
        ]
        if target_comment_count >= 40:
            attempts.append(
                {"aggressive": True, "comment_limit": self._comment_article_limit(target_comment_count + 80, aggressive=True)}
            )

        best_payload: dict[str, Any] | None = None
        best_detail_html = ""
        best_extracted_count = -1
        for attempt in attempts:
            current_payload, current_html = self._collect_post_payload(
                context=context,
                detail_url=detail_url,
                target_comment_count=target_comment_count,
                aggressive=bool(attempt["aggressive"]),
                comment_limit=int(attempt["comment_limit"]),
            )
            current_extracted_count = self._count_meaningful_payload_comments(current_payload)
            if current_extracted_count > best_extracted_count:
                best_payload = current_payload
                best_detail_html = current_html
                best_extracted_count = current_extracted_count
            if not self._should_retry_post_detail(current_payload, target_comment_count):
                break

        payload = best_payload or {}
        detail_html = best_detail_html

        published_hint = (
            payload.get("published_hint")
            or candidate.get("published_hint")
            or self._derive_published_hint_from_body(payload.get("body_text") or "", page_name)
        )
        resolved_published_at = (
            published_at
            or payload.get("published_at")
            or self._extract_embedded_published_at(
                detail_html,
                detail_url=detail_url,
                post_permalink=payload.get("post_permalink"),
            )
            or self._parse_post_timestamp(published_hint)
        )
        post_permalink = self._select_post_permalink(
            payload_post_permalink=payload.get("post_permalink"),
            candidate_permalink=candidate.get("permalink"),
            detail_url=detail_url,
        )
        post_id = stable_id(page_id, post_permalink)
        raw_path = raw_store.write_json("web_post_pages", slugify(post_id), payload)

        post_text = self._clean_post_text(
            payload.get("post_text") or candidate.get("text") or "",
            published_hint,
        )
        if not self._is_meaningful_post_text(post_text):
            post_text = self._extract_generic_post_text(
                payload.get("body_text") or "",
                payload.get("meta_description") or "",
                page_name,
            )
        comments = self._build_comment_snapshots(
            post_id=post_id,
            payload_comments=payload.get("comments", []),
            raw_path=str(raw_path),
        )

        return PostSnapshot(
            post_id=post_id,
            platform="facebook",
            source_id=page_id,
            created_at=resolved_published_at,
            message=post_text,
            permalink=post_permalink,
            reactions=max(
                int(candidate.get("reactions") or 0),
                self._extract_reaction_count(payload),
            ),
            shares=int(candidate.get("shares") or 0),
            comments_count=max(
                len(comments),
                int(candidate.get("comments_count") or 0),
                self._extract_comment_count(payload),
            ),
            has_media=False,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=AuthorSnapshot(author_id=page_id, name=page_name),
            comments=comments,
        )

    def _collect_post_payload(
        self,
        *,
        context: Any,
        detail_url: str,
        target_comment_count: int,
        aggressive: bool,
        comment_limit: int,
    ) -> tuple[dict[str, Any], str]:
        detail_page = context.new_page()
        try:
            detail_page.goto(
                detail_url,
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            detail_page.wait_for_timeout(4000)
            self._prepare_post_detail_page(
                detail_page,
                target_comment_count=target_comment_count,
                aggressive=aggressive,
            )
            payload = self._extract_post_page(detail_page, comment_limit=comment_limit)
            detail_html = detail_page.content()
            return payload, detail_html
        finally:
            detail_page.close()

    def _build_comment_snapshots(
        self,
        *,
        post_id: str,
        payload_comments: list[dict[str, Any]],
        raw_path: str,
    ) -> list[CommentSnapshot]:
        comments: list[CommentSnapshot] = []
        nesting_stack: list[dict[str, Any]] = []
        for payload_comment in payload_comments:
            raw_comment_text = payload_comment.get("text") or ""
            author_name = self._select_comment_author(payload_comment.get("author_name"), raw_comment_text)
            published_hint = payload_comment.get("published_hint") or self._derive_comment_published_hint(raw_comment_text)
            comment_text = self._clean_comment_text(
                raw_comment_text,
                author_name or "",
                published_hint,
            )
            if len(comment_text) < 3:
                continue

            nesting_x = int(payload_comment.get("nesting_x") or 0)
            while nesting_stack and nesting_x <= int(nesting_stack[-1]["nesting_x"]) + 5:
                nesting_stack.pop()

            parent_comment_id = nesting_stack[-1]["comment_id"] if nesting_stack else None
            depth = len(nesting_stack)
            comment_id = stable_id(post_id, payload_comment.get("permalink") or comment_text[:160])
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="facebook",
                parent_post_id=post_id,
                parent_comment_id=parent_comment_id,
                thread_root_post_id=post_id,
                created_at=self._parse_post_timestamp(published_hint),
                message=comment_text,
                permalink=payload_comment.get("permalink"),
                reactions=0,
                source_collector=self.name,
                depth=depth,
                raw_path=raw_path,
                author=AuthorSnapshot(
                    author_id=None,
                    name=author_name,
                ),
            )
            comments.append(snapshot)
            nesting_stack.append({"nesting_x": nesting_x, "comment_id": comment_id})
        return comments

    def _prepare_post_detail_page(
        self,
        page: Any,
        *,
        target_comment_count: int = 0,
        aggressive: bool = False,
    ) -> None:
        self._accept_desktop_cookies(page)
        self._click_buttonish_text(
            page,
            patterns=[r"\b\d+(?:\.\d+)?\s*[KM]?\s+comments?\b"],
            max_clicks=2 if aggressive else 1,
            wait_ms=1500,
        )
        if aggressive and target_comment_count > 0:
            self._click_buttonish_text(
                page,
                patterns=[r"\bComment\b"],
                max_clicks=1,
                wait_ms=1000,
            )
        self._click_buttonish_text(
            page,
            patterns=self._comment_sort_menu_patterns(),
            max_clicks=1,
            wait_ms=1000,
        )
        self._click_buttonish_text(
            page,
            patterns=self._comment_sort_option_patterns(aggressive=aggressive),
            max_clicks=2 if aggressive else 1,
            wait_ms=2500,
        )
        self._expand_comment_threads(page, target_comment_count=target_comment_count, aggressive=aggressive)

    def _expand_comment_threads(self, page: Any, *, target_comment_count: int = 0, aggressive: bool = False) -> None:
        last_article_count = self._count_article_nodes(page)
        stable_rounds = 0
        max_rounds = 16 + min(target_comment_count // 20, 10)
        if aggressive:
            max_rounds += 12
        for _ in range(max_rounds):
            page.mouse.wheel(0, 1800)
            scrolled = self._scroll_primary_comment_container(page)
            page.wait_for_timeout(1200)
            more_clicked = self._click_buttonish_text(
                page,
                patterns=self._comment_expansion_patterns(),
                max_clicks=6 if aggressive else 3,
                wait_ms=1200,
            )
            reply_clicked = self._click_buttonish_text(
                page,
                patterns=self._reply_expansion_patterns(),
                max_clicks=18 if aggressive else 10,
                wait_ms=900,
            )
            if reply_clicked:
                self._scroll_primary_comment_container(page)
                page.wait_for_timeout(1000)

            if aggressive and last_article_count <= 1 and target_comment_count > 0:
                self._click_buttonish_text(
                    page,
                    patterns=[r"\b\d+(?:\.\d+)?\s*[KM]?\s+comments?\b", r"\bComment\b"],
                    max_clicks=2,
                    wait_ms=1000,
                )

            article_count = self._count_article_nodes(page)
            if article_count <= last_article_count and not scrolled and more_clicked == 0 and reply_clicked == 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_article_count = max(last_article_count, article_count)
            enough_comments = (
                article_count >= min(max(target_comment_count, 0) + 1, self._comment_article_limit(target_comment_count, aggressive))
                if target_comment_count
                else False
            )
            if enough_comments or stable_rounds >= (3 if aggressive else 2):
                break

    def _should_retry_post_detail(self, payload: dict[str, Any], target_comment_count: int) -> bool:
        if target_comment_count <= 0:
            return False
        extracted_count = self._count_meaningful_payload_comments(payload)
        if extracted_count == 0:
            return True
        if target_comment_count >= 20 and extracted_count < max(3, target_comment_count // 6):
            return True
        return False

    def _count_meaningful_payload_comments(self, payload: dict[str, Any]) -> int:
        count = 0
        for comment in payload.get("comments", []):
            raw_comment_text = comment.get("text") or ""
            author_name = self._select_comment_author(comment.get("author_name"), raw_comment_text)
            published_hint = comment.get("published_hint") or self._derive_comment_published_hint(raw_comment_text)
            comment_text = self._clean_comment_text(raw_comment_text, author_name or "", published_hint)
            if len(comment_text) >= 3:
                count += 1
        return count

    @staticmethod
    def _comment_article_limit(target_comment_count: int, aggressive: bool) -> int:
        base_limit = 220
        if aggressive:
            base_limit = 320
        if target_comment_count >= 80:
            return max(base_limit, 420 if aggressive else 280)
        if target_comment_count >= 30:
            return max(base_limit, 280 if aggressive else 240)
        return base_limit

    @staticmethod
    def _comment_sort_menu_patterns() -> list[str]:
        return [
            r"\bMost relevant\b",
            r"\bTop comments\b",
            r"\bNewest\b",
            r"\bMost recent\b",
            r"\u041d\u0430\u0439\u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u0456\u0448\u0456\b",
            r"\u041d\u0430\u0439\u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u044b\u0435\b",
        ]

    @staticmethod
    def _comment_sort_option_patterns(*, aggressive: bool) -> list[str]:
        patterns = [
            r"\bAll comments\b",
            r"\bNewest\b",
            r"\bMost recent\b",
            r"\u0423\u0441\u0456 \u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\b",
            r"\u0412\u0441\u0435 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0438\b",
        ]
        if aggressive:
            patterns.append(r"\bMost relevant\b")
        return patterns

    @staticmethod
    def _comment_expansion_patterns() -> list[str]:
        return [
            r"\bView more comments\b",
            r"\bSee more comments\b",
            r"\bMore comments\b",
            r"\bView previous comments\b",
            r"\bSee previous comments\b",
            r"\bShow more comments\b",
            r"\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u0438 \u0431\u0456\u043b\u044c\u0448\u0435 \u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\u0432\b",
            r"\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0435\u0432\b",
        ]

    @staticmethod
    def _reply_expansion_patterns() -> list[str]:
        return [
            r"\bView replies\b",
            r"\bView more replies\b",
            r"\bSee more replies\b",
            r"\bView previous replies\b",
            r"\bSee previous replies\b",
            r"\bShow replies\b",
            r"\b\d+\s+Replies?\b",
            r"\b\d+\s+Reply\b",
            r"\u0412\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u0456\b",
            r"\u041e\u0442\u0432\u0435\u0442\u044b\b",
        ]

    @staticmethod
    def _accept_desktop_cookies(page: Any) -> None:
        for label in ("Decline optional cookies", "Only allow essential cookies", "Allow all cookies"):
            try:
                button = page.get_by_text(label, exact=False)
                if button.count():
                    button.first.click(timeout=5000, force=True)
                    page.wait_for_timeout(2000)
                    return
            except Exception:
                continue

    @staticmethod
    def _click_buttonish_text(
        page: Any,
        *,
        patterns: list[str],
        max_clicks: int,
        wait_ms: int,
    ) -> int:
        clicked = 0
        for _ in range(max_clicks):
            clicked_text = page.evaluate(
                """
                (patterns) => {
                  const isVisible = (element) => {
                    const rect = element.getBoundingClientRect();
                    const style = window.getComputedStyle(element);
                    return (
                      rect.width > 0 &&
                      rect.height > 0 &&
                      style.display !== 'none' &&
                      style.visibility !== 'hidden'
                    );
                  };
                  const candidates = Array.from(
                    document.querySelectorAll('div[role="button"], a[role="button"], span[role="button"], button')
                  );
                  for (const pattern of patterns) {
                    const regex = new RegExp(pattern, 'i');
                    const target = candidates.find((element) => {
                      if (!isVisible(element)) {
                        return false;
                      }
                      const value = `${element.innerText || ''} ${element.getAttribute('aria-label') || ''}`.trim();
                      return regex.test(value);
                    });
                    if (target) {
                      target.scrollIntoView({ block: 'center', inline: 'nearest' });
                      target.click();
                      return `${target.innerText || ''} ${target.getAttribute('aria-label') || ''}`.trim();
                    }
                  }
                  return null;
                }
                """,
                patterns,
            )
            if not clicked_text:
                break
            clicked += 1
            page.wait_for_timeout(wait_ms)
        return clicked

    @staticmethod
    def _scroll_primary_comment_container(page: Any) -> bool:
        return bool(
            page.evaluate(
                """
                () => {
                  const root = document.querySelector('[role="dialog"]') || document;
                  const candidates = Array.from(root.querySelectorAll('div'))
                    .map((element) => {
                      const style = window.getComputedStyle(element);
                      const text = (element.innerText || '').trim();
                      const overflowY = style.overflowY;
                      if (!['auto', 'scroll'].includes(overflowY)) {
                        return null;
                      }
                      if (element.scrollHeight <= element.clientHeight + 40 || element.clientHeight < 300) {
                        return null;
                      }
                      const score = (element.scrollHeight - element.clientHeight) + (text.includes('comments') ? 100000 : 0);
                      return { element, score };
                    })
                    .filter(Boolean)
                    .sort((left, right) => right.score - left.score);
                  const target = candidates[0]?.element;
                  if (!target) {
                    return false;
                  }
                  const before = target.scrollTop;
                  target.scrollTop = target.scrollHeight;
                  return target.scrollTop > before + 20;
                }
                """
            )
        )

    @staticmethod
    def _count_article_nodes(page: Any) -> int:
        return page.locator('div[role="article"], article').count()

    @staticmethod
    def _extract_feed_candidates(page: Any) -> list[dict[str, Any]]:
        script = """
        () => {
          const articles = Array.from(document.querySelectorAll('div[role="article"], article'));
          return articles.map((article) => {
            const permalinkNode = article.querySelector('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="]');
            return {
              permalink: permalinkNode?.href || null,
              published_hint: (permalinkNode?.innerText || '').trim(),
              text: (article.innerText || '').trim(),
              reactions_text: '',
              comments_text: '',
              shares_text: ''
            };
          }).filter(item => item.permalink && item.text);
        }
        """
        return page.evaluate(script)

    @staticmethod
    def _extract_plugin_feed_candidates(page: Any) -> list[dict[str, Any]]:
        script = """
        () => {
          const wrappers = Array.from(document.querySelectorAll('div._5pcr.userContentWrapper'));
          const metricValue = (wrapper, title) => {
            const metricNodes = Array.from(wrapper.querySelectorAll('.embeddedLikeButton, a._29bd div, [title]'));
            const match = metricNodes.find((node) => {
              const value = (node.getAttribute('title') || node.innerText || '').trim().toLowerCase();
              return value.includes(title);
            });
            return (match?.innerText || '').trim();
          };
          return wrappers.map((wrapper) => {
            const timestampNode = wrapper.querySelector('abbr[data-utime]');
            const timestampLink =
              timestampNode?.closest('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="]') ||
              wrapper.querySelector('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="]');
            const messageNodes = Array.from(wrapper.querySelectorAll('[data-testid="post_message"]'));
            const primaryMessage = (messageNodes[0]?.innerText || '').trim();
            const allMessages = messageNodes
              .map((node) => (node.innerText || '').trim())
              .filter(Boolean)
              .join('\\n\\n');
            return {
              permalink: timestampLink?.href || null,
              published_hint: (timestampNode?.innerText || '').trim(),
              published_at: timestampNode?.dataset?.utime ? new Date(Number(timestampNode.dataset.utime) * 1000).toISOString() : null,
              text: primaryMessage || allMessages,
              author_name: (wrapper.querySelector('._50f7, .fwb a, .fwb span')?.innerText || '').trim() || null,
              reactions_text: metricValue(wrapper, 'like'),
              comments_text: metricValue(wrapper, 'comment'),
              shares_text: metricValue(wrapper, 'share'),
            };
          }).filter(item => item.permalink && item.text);
        }
        """
        return page.evaluate(script)

    def _discover_media_candidates(
        self,
        context: Any,
        page_url: str,
        tab_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[dict[str, Any]]:
        tab_page = context.new_page()
        tab_url = self._page_tab_url(page_url, tab_name)
        tab_page.goto(
            tab_url,
            wait_until="domcontentloaded",
            timeout=int(self.settings.timeout_seconds * 1000),
        )
        tab_page.wait_for_timeout(4000)
        if tab_name == "videos":
            raw_candidates = self._extract_video_candidates(tab_page)
        elif tab_name == "photos":
            raw_candidates = self._extract_photo_candidates(tab_page)
        elif tab_name == "reels":
            raw_candidates = self._extract_reel_candidates(tab_page)
        else:
            raw_candidates = []
        payload = {
            "title": tab_page.title(),
            "url": tab_page.url,
            "candidates": raw_candidates,
        }
        raw_store.write_json("web_extract", f"{tab_name}_candidates", payload)
        tab_page.close()
        return self._postprocess_candidates(raw_candidates)

    @staticmethod
    def _extract_video_candidates(page: Any) -> list[dict[str, Any]]:
        script = """
        () => {
          const links = Array.from(document.querySelectorAll('a[href*="/videos/"]'));
          return links.map((link) => ({
            permalink: link.href,
            detail_url: link.href,
            published_hint: '',
            published_at: null,
            text: (link.innerText || '').trim(),
            author_name: null,
            reactions_text: '',
            comments_text: '',
            shares_text: '',
          }));
        }
        """
        return page.evaluate(script)

    @staticmethod
    def _extract_photo_candidates(page: Any) -> list[dict[str, Any]]:
        script = """
        () => {
          const links = Array.from(document.querySelectorAll('a[href*="photo.php?fbid="]'));
          return links.map((link) => ({
            permalink: link.href,
            detail_url: link.href,
            published_hint: '',
            published_at: null,
            text: '',
            author_name: null,
            reactions_text: '',
            comments_text: '',
            shares_text: '',
          }));
        }
        """
        return page.evaluate(script)

    @staticmethod
    def _extract_reel_candidates(page: Any) -> list[dict[str, Any]]:
        script = """
        () => {
          const links = Array.from(document.querySelectorAll('a[href*="/reel/"]'));
          return links.map((link) => ({
            permalink: link.href,
            detail_url: link.href,
            published_hint: '',
            published_at: null,
            text: (link.innerText || '').trim(),
            author_name: null,
            reactions_text: '',
            comments_text: '',
            shares_text: '',
          }));
        }
        """
        return page.evaluate(script)

    @staticmethod
    def _extract_post_page(page: Any, *, comment_limit: int = 200) -> dict[str, Any]:
        script = """
        (commentLimit) => {
          const articles = Array.from(document.querySelectorAll('div[role="article"], article'));
          const firstArticle = articles[0] || null;
          const timestampNode = firstArticle?.querySelector('abbr[data-utime], span.timestampContent');
          const permalinkNode =
            firstArticle?.querySelector('a[href*="/posts/"]:not([href*="comment_id="]), a[href*="/videos/"], a[href*="/reel/"], a[href*="story_fbid="]') ||
            document.querySelector('a[href*="/posts/"]:not([href*="comment_id="]), a[href*="/videos/"], a[href*="/reel/"], a[href*="story_fbid="]');
          const getMeta = (property) => document.querySelector(`meta[property="${property}"]`)?.content || null;
          const getComment = (article) => {
            const links = Array.from(article.querySelectorAll('a[href], a[role="link"]'));
            const authorLink = links.find((link) => {
              const value = (link.innerText || '').trim();
              return (
                value &&
                value.length <= 80 &&
                !/\\b(?:comment|reply|replies|like)\\b/i.test(value) &&
                !/\\b\\d+\\s*(?:m(?:in)?s?|h|d|w)\\b/i.test(value)
              );
            }) || null;
            const commentPermalink = links.find((link) => (link.href || '').includes('comment_id=')) || null;
            const rect = article.getBoundingClientRect();
            return {
              text: (article.innerText || '').trim(),
              author_name: authorLink?.innerText?.trim() || null,
              permalink: commentPermalink?.href || null,
              published_hint: (commentPermalink?.innerText || '').trim(),
              nesting_x: Math.round(rect.x),
            };
          };
          const comments = articles
            .slice(1)
            .map(getComment)
            .filter((comment) => {
              const text = (comment.text || '').trim();
              return text && (comment.author_name || comment.permalink || comment.published_hint);
            })
            .slice(0, commentLimit);
          return {
            post_text: firstArticle?.innerText || '',
            post_permalink: permalinkNode?.href || getMeta('og:url') || window.location.href,
            published_hint: (timestampNode?.innerText || permalinkNode?.innerText || '').trim(),
            published_at: timestampNode?.dataset?.utime ? new Date(Number(timestampNode.dataset.utime) * 1000).toISOString() : null,
            body_text: (document.body?.innerText || '').trim(),
            meta_title: getMeta('og:title'),
            meta_description: getMeta('og:description'),
            comments,
          };
        }
        """
        return page.evaluate(script, comment_limit)

    def _collect_mobile_timeline_posts(
        self,
        browser: Any,
        page_url: str,
        page_id: str,
        page_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        mobile_context = browser.new_context(
            locale="en-US",
            viewport={"width": 430, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
            ),
            is_mobile=True,
            has_touch=True,
        )
        timeline_page = mobile_context.new_page()
        try:
            mobile_url = self._mobile_page_url(page_url)
            timeline_page.goto(
                mobile_url,
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            timeline_page.wait_for_timeout(3000)
            self._accept_mobile_cookies(timeline_page)
            timeline_page.goto(
                mobile_url,
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            timeline_page.wait_for_timeout(5000)
            payload = self._extract_mobile_timeline_payload(timeline_page)
            raw_path = raw_store.write_json("web_extract", "mobile_timeline", payload)
        finally:
            mobile_context.close()

        posts: list[PostSnapshot] = []
        for candidate in self._parse_mobile_timeline_candidates(payload.get("action_items", []), page_name):
            published_at = self._parse_post_timestamp(candidate.get("published_hint") or "")
            if published_at and not self._within_configured_range(published_at):
                continue
            if not published_at and (self.config.date_range.start or self.config.date_range.end):
                continue
            post = PostSnapshot(
                post_id=stable_id(
                    page_id,
                    candidate.get("published_hint") or "",
                    candidate.get("message") or "",
                ),
                platform="facebook",
                source_id=page_id,
                created_at=published_at,
                message=candidate.get("message"),
                permalink=None,
                reactions=int(candidate.get("reactions") or 0),
                shares=0,
                comments_count=int(candidate.get("comments_count") or 0),
                has_media=False,
                source_collector=self.name,
                raw_path=str(raw_path),
                author=AuthorSnapshot(author_id=page_id, name=page_name),
            )
            posts.append(post)
        return posts

    @staticmethod
    def _accept_mobile_cookies(page: Any) -> None:
        for label in ("Only allow essential cookies", "Decline optional cookies"):
            try:
                button = page.get_by_text(label, exact=False)
                if button.count():
                    button.first.click(timeout=5000, force=True)
                    page.wait_for_timeout(2500)
                    return
            except Exception:
                continue

    @staticmethod
    def _extract_mobile_timeline_payload(page: Any) -> dict[str, Any]:
        script = """
        () => ({
          url: window.location.href,
          body_text: (document.body?.innerText || '').trim(),
          action_items: Array.from(document.querySelectorAll('[data-action-id]'))
            .map((node) => ({
              action_id: node.getAttribute('data-action-id'),
              text: (node.innerText || '').trim(),
            }))
            .filter((item) => item.text),
        })
        """
        return page.evaluate(script)

    @classmethod
    def _parse_mobile_timeline_candidates(
        cls,
        raw_action_items: list[dict[str, Any]],
        page_name: str,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, str]] = []
        for item in raw_action_items:
            text = cls._normalize_mobile_text(item.get("text") or "")
            if not text or cls._is_mobile_ui_text(text):
                continue
            if items and items[-1]["text"] == text:
                continue
            items.append({"action_id": str(item.get("action_id") or ""), "text": text})

        groups: list[list[dict[str, str]]] = []
        current_group: list[dict[str, str]] = []
        in_posts = False
        for item in items:
            text = item["text"]
            if text == "Posts":
                in_posts = True
                continue
            if not in_posts and cls._looks_like_mobile_post_header(text, page_name):
                in_posts = True
            if not in_posts:
                continue
            if cls._is_mobile_timeline_end(text):
                break
            if cls._looks_like_mobile_post_header(text, page_name):
                if current_group:
                    groups.append(current_group)
                current_group = [item]
                continue
            if current_group:
                current_group.append(item)
        if current_group:
            groups.append(current_group)

        candidates: list[dict[str, Any]] = []
        for group in groups:
            header_text = group[0]["text"]
            published_hint = cls._extract_mobile_published_hint(header_text)
            message = cls._extract_mobile_post_message(group, page_name)
            if len(message) < 20:
                continue
            candidates.append(
                {
                    "published_hint": published_hint,
                    "message": message,
                    "reactions": cls._extract_mobile_reactions(group),
                    "comments_count": cls._extract_mobile_comment_count(group),
                }
            )
        return candidates

    @classmethod
    def _postprocess_candidates(cls, raw_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        processed: list[dict[str, Any]] = []
        seen: set[str] = set()
        for candidate in raw_candidates:
            permalink = cls._normalize_post_permalink(candidate.get("permalink") or "")
            if not permalink or permalink in seen:
                continue
            seen.add(permalink)
            processed.append(
                {
                    "permalink": permalink,
                    "detail_url": candidate.get("detail_url") or permalink,
                    "published_hint": (candidate.get("published_hint") or "").strip(),
                    "published_at": candidate.get("published_at"),
                    "text": (candidate.get("text") or "").strip(),
                    "author_name": candidate.get("author_name"),
                    "reactions": cls._extract_metric_count(candidate.get("reactions_text") or ""),
                    "comments_count": cls._extract_metric_count(candidate.get("comments_text") or ""),
                    "shares": cls._extract_metric_count(candidate.get("shares_text") or ""),
                }
            )
        return processed

    @staticmethod
    def _merge_post_snapshots(existing: PostSnapshot | None, incoming: PostSnapshot) -> PostSnapshot:
        if existing is None:
            return incoming
        merged_comments = {comment.comment_id: comment for comment in existing.comments}
        for comment in incoming.comments:
            merged_comments.setdefault(comment.comment_id, comment)
        return existing.model_copy(
            update={
                "created_at": existing.created_at or incoming.created_at,
                "permalink": existing.permalink or incoming.permalink,
                "message": (
                    incoming.message
                    if len(incoming.message or "") > len(existing.message or "")
                    else existing.message
                ),
                "reactions": max(existing.reactions, incoming.reactions),
                "shares": max(existing.shares, incoming.shares),
                "comments_count": max(existing.comments_count, incoming.comments_count, len(merged_comments)),
                "comments": list(merged_comments.values()),
                "raw_path": existing.raw_path or incoming.raw_path,
            }
        )

    def _upsert_post_snapshot(self, posts_by_id: dict[str, PostSnapshot], post: PostSnapshot) -> None:
        for existing_id, existing in posts_by_id.items():
            if self._posts_match(existing, post):
                posts_by_id[existing_id] = self._merge_post_snapshots(existing, post)
                return
        posts_by_id[post.post_id] = post

    @classmethod
    def _posts_match(cls, left: PostSnapshot, right: PostSnapshot) -> bool:
        if left.created_at and right.created_at:
            left_dt = datetime.fromisoformat(left.created_at.replace("Z", "+00:00"))
            right_dt = datetime.fromisoformat(right.created_at.replace("Z", "+00:00"))
            if abs((left_dt - right_dt).total_seconds()) > 3600:
                return False
        left_text = cls._canonical_post_text(left.message or "")
        right_text = cls._canonical_post_text(right.message or "")
        if not left_text or not right_text:
            return False
        return (
            left_text[:80] == right_text[:80]
            or left_text.startswith(right_text[:60])
            or right_text.startswith(left_text[:60])
        )

    @classmethod
    def _canonical_post_text(cls, text: str) -> str:
        normalized = cls._normalize_mobile_text(text).lower()
        normalized = normalized.replace("see more", "")
        normalized = re.sub(r"^@\S.*$", "", normalized, flags=re.MULTILINE)
        return re.sub(r"\s+", " ", normalized).strip()

    def _within_configured_range(self, published_at: str) -> bool:
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        start_dt = self._range_boundary(self.config.date_range.start, is_end=False)
        end_dt = self._range_boundary(self.config.date_range.end, is_end=True)
        if start_dt and published < start_dt:
            return False
        if end_dt and published > end_dt:
            return False
        return True

    @staticmethod
    def _range_boundary(raw_value: str | None, is_end: bool) -> datetime | None:
        if not raw_value:
            return None
        if "T" not in raw_value and len(raw_value) == 10:
            parsed_date = date.fromisoformat(raw_value)
            boundary_time = time.max if is_end else time.min
            return datetime.combine(parsed_date, boundary_time, tzinfo=UTC)
        try:
            parsed = datetime.fromisoformat(raw_value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            parsed_date = date.fromisoformat(raw_value)
            boundary_time = time.max if is_end else time.min
            return datetime.combine(parsed_date, boundary_time, tzinfo=UTC)

    @staticmethod
    def _clean_post_text(raw_text: str, published_hint: str) -> str:
        text = raw_text.replace("\xa0", " ").strip()
        if "·" in text:
            text = text.split("·", 1)[1].strip()
        text = re.sub(r"\bMost relevant\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
        text = re.sub(r"\bAll reactions:.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
        text = re.sub(r"\bLike\s+Comment\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
        text = re.sub(r"\bLike\s+Comment\s+Share\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
        if published_hint and text.startswith(published_hint):
            text = text[len(published_hint) :].strip()
        return text

    @staticmethod
    def _clean_comment_text(raw_text: str, author_name: str, published_hint: str) -> str:
        text = raw_text.replace("\xa0", " ").strip()
        if author_name and text.startswith(author_name):
            text = text[len(author_name) :].lstrip(" \t:,-")
        lines = [PublicWebCollector._normalize_mobile_text(line) for line in text.splitlines() if line.strip()]
        if author_name and lines and lines[0] == author_name:
            lines = lines[1:]
        if published_hint and lines and lines[0].lower() == published_hint.lower():
            lines = lines[1:]
        if published_hint and lines and lines[-1].lower() == published_hint.lower():
            lines = lines[:-1]
        lines = [line for line in lines if not PublicWebCollector._is_comment_control_line(line)]
        while lines and re.fullmatch(r"[\W_]*\d+[\W_]*", lines[-1]):
            lines = lines[:-1]
        while lines and PublicWebCollector._parse_post_timestamp(lines[-1]):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
        if cleaned == published_hint or cleaned == author_name:
            return ""
        return cleaned

    @staticmethod
    def _is_comment_control_line(line: str) -> bool:
        normalized = PublicWebCollector._normalize_mobile_text(line).strip(" \t.;:|")
        if not normalized:
            return True
        if re.fullmatch(r"[\W_]+", normalized):
            return True
        if normalized in {"Like", "Reply", "Replies", "Most relevant", "All comments", "Newest"}:
            return True
        if re.fullmatch(r"\d+\s+Repl(?:y|ies)", normalized, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r".{1,80}\s+replied", normalized, flags=re.IGNORECASE):
            return True
        return normalized == "·"

    @staticmethod
    def _select_comment_author(raw_author_name: str | None, raw_text: str) -> str | None:
        normalized_author = PublicWebCollector._normalize_mobile_text(raw_author_name or "")
        if PublicWebCollector._is_plausible_comment_author(normalized_author):
            return normalized_author
        return PublicWebCollector._derive_comment_author(raw_text)

    @staticmethod
    def _is_plausible_comment_author(value: str) -> bool:
        candidate = PublicWebCollector._normalize_mobile_text(value).strip()
        if not candidate:
            return False
        if len(candidate) > 80 or "\n" in candidate:
            return False
        if PublicWebCollector._parse_post_timestamp(candidate):
            return False
        if re.fullmatch(r"[\W_]*\d+[\W_]*", candidate):
            return False
        if candidate in {
            "Like",
            "Comment",
            "Reply",
            "Most relevant",
            "All comments",
            "View more comments",
        }:
            return False
        return True

    @staticmethod
    def _is_meaningful_post_text(text: str) -> bool:
        normalized = (text or "").strip()
        if len(normalized) < 20:
            return False
        noisy_tokens = ("Log In", "Forgot Account?", "See more on Facebook")
        return not any(token in normalized for token in noisy_tokens)

    @staticmethod
    def _extract_generic_post_text(body_text: str, meta_description: str, page_name: str) -> str:
        lines = [line.strip() for line in body_text.replace("\xa0", " ").splitlines() if line.strip()]
        collected: list[str] = []
        started = False
        for line in lines:
            if not started:
                if PublicWebCollector._is_ui_line(line, page_name):
                    continue
                if len(line) >= 20:
                    started = True
                    collected.append(line)
                continue
            if PublicWebCollector._is_stop_line(line):
                break
            collected.append(line)
        text = "\n".join(collected).strip()
        if len(text) >= 20:
            return text
        return meta_description.strip()

    @staticmethod
    def _derive_comment_author(raw_text: str) -> str | None:
        normalized_text = PublicWebCollector._normalize_mobile_text(raw_text.replace("\xa0", " "))
        lines = [line.strip() for line in normalized_text.splitlines() if line.strip()]
        if lines:
            synthetic_first_line = re.sub(r"(?<=[a-zа-яіїєґ])(?=[A-ZА-ЯІЇЄҐ])", " ", lines[0])
            name_tokens: list[str] = []
            for token in re.findall(r"[A-Za-zА-ЯІЇЄҐа-яіїєґ'’-]+", synthetic_first_line):
                if not PublicWebCollector._looks_like_name_token(token):
                    break
                name_tokens.append(token)
                if len(name_tokens) == 2:
                    break
            if len(name_tokens) >= 2:
                candidate = " ".join(name_tokens)
                if PublicWebCollector._is_plausible_comment_author(candidate):
                    return candidate
        for line in lines:
            if PublicWebCollector._is_plausible_comment_author(line):
                return line
        return lines[0] if lines else None

    @staticmethod
    def _looks_like_name_token(token: str) -> bool:
        candidate = token.strip()
        if not candidate or len(candidate) > 40:
            return False
        if candidate.isupper():
            return True
        return candidate[0].isupper()

    @staticmethod
    def _derive_comment_published_hint(raw_text: str) -> str:
        lines = [line.strip() for line in raw_text.replace("\xa0", " ").splitlines() if line.strip()]
        for line in lines[1:]:
            extracted = PublicWebCollector._extract_supported_date_hint_safe(line)
            if extracted:
                return extracted
        return ""

    @staticmethod
    def _derive_published_hint_from_body(body_text: str, page_name: str) -> str:
        lines = [line.strip() for line in body_text.replace("\xa0", " ").splitlines() if line.strip()]
        for index, line in enumerate(lines):
            if line == page_name:
                for candidate in lines[index + 1 : index + 5]:
                    extracted = PublicWebCollector._extract_supported_date_hint_safe(candidate)
                    if extracted:
                        return extracted
        for line in lines:
            extracted = PublicWebCollector._extract_supported_date_hint_safe(line)
            if extracted:
                return extracted
        return ""

    @staticmethod
    def _extract_embedded_published_at(
        html: str,
        *,
        detail_url: str,
        post_permalink: str | None,
    ) -> str | None:
        if not html:
            return None

        patterns: list[str] = []
        for value in (detail_url, post_permalink or ""):
            media_id = PublicWebCollector._extract_numeric_media_id(value)
            if not media_id:
                continue
            escaped_media_id = re.escape(media_id)
            patterns.extend(
                [
                    rf'"id":"{escaped_media_id}".{{0,2500}}?"creation_time":(\d{{9,}})',
                    rf'"creation_time":(\d{{9,}}).{{0,2500}}?"id":"{escaped_media_id}"',
                    rf'"id":"{escaped_media_id}".{{0,2500}}?"publish_time":(\d{{9,}})',
                    rf'"publish_time":(\d{{9,}}).{{0,2500}}?"id":"{escaped_media_id}"',
                    rf'"story_fbid":\["{escaped_media_id}"\].{{0,1200}}?"publish_time":(\d{{9,}})',
                ]
            )

        patterns.extend(
            [
                r'"post_id":"[^"]+","creation_time":(\d{9,}),"unpublished_content_type"',
                r'"publish_time":(\d{9,}),"story_name"',
                r'"creation_time":(\d{9,})',
                r'"publish_time":(\d{9,})',
            ]
        )

        for pattern in patterns:
            match = re.search(pattern, html, flags=re.DOTALL)
            if not match:
                continue
            timestamp = PublicWebCollector._epoch_seconds_to_iso(match.group(1))
            if timestamp:
                return timestamp
        return None

    @staticmethod
    def _extract_numeric_media_id(value: str) -> str | None:
        if not value:
            return None
        for pattern in (
            r"/reel/(\d+)",
            r"/videos/(\d+)",
            r"[?&]fbid=(\d+)",
            r"[?&]story_fbid=(\d+)",
        ):
            match = re.search(pattern, value)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _epoch_seconds_to_iso(raw_value: str) -> str | None:
        try:
            epoch_value = int(raw_value)
        except (TypeError, ValueError):
            return None
        earliest_epoch = int(datetime(2010, 1, 1, tzinfo=UTC).timestamp())
        latest_epoch = int((datetime.now(tz=UTC) + timedelta(days=7)).timestamp())
        if epoch_value < earliest_epoch or epoch_value > latest_epoch:
            return None
        return datetime.fromtimestamp(epoch_value, tz=UTC).replace(microsecond=0).isoformat()

    @staticmethod
    def _extract_metric_count(raw_text: str) -> int:
        normalized = (raw_text or "").replace(",", "").strip()
        match = re.search(r"(\d+(?:\.\d+)?)\s*([KM]?)", normalized, flags=re.IGNORECASE)
        if not match:
            return 0
        value = float(match.group(1))
        suffix = match.group(2).upper()
        if suffix == "K":
            value *= 1000
        elif suffix == "M":
            value *= 1_000_000
        return int(value)

    @staticmethod
    def _extract_reaction_count(payload: dict[str, Any]) -> int:
        title = payload.get("meta_title") or ""
        match = re.search(r"(\d+(?:\.\d+)?\s*[KM]?)\s+reactions", title, flags=re.IGNORECASE)
        if match:
            return PublicWebCollector._extract_metric_count(match.group(1))
        return 0

    @staticmethod
    def _extract_comment_count(payload: dict[str, Any]) -> int:
        body_text = payload.get("body_text") or ""
        for line in body_text.replace("\xa0", " ").splitlines():
            match = re.search(r"(\d+(?:\.\d+)?\s*[KM]?)\s+comments?", line, flags=re.IGNORECASE)
            if match:
                return PublicWebCollector._extract_metric_count(match.group(1))
        return 0

    @staticmethod
    def _is_ui_line(line: str, page_name: str) -> bool:
        normalized = line.strip()
        if not normalized:
            return True
        ui_lines = {
            "Log In",
            "Forgot Account?",
            "More",
            "Home",
            "Live",
            "Reels",
            "Explore",
            "Follow",
            "Comments",
            "Video",
            "Public",
            "Like",
            "Comment",
            "Share",
        }
        if normalized in ui_lines or normalized == page_name:
            return True
        if re.fullmatch(r"\d+:\d+(?:\s*/\s*\d+:\d+)?", normalized):
            return True
        if re.fullmatch(r"\d+(?:\.\d+)?\s*[KM]?(?:\s+views?)?", normalized, flags=re.IGNORECASE):
            return True
        return False

    @staticmethod
    def _is_stop_line(line: str) -> bool:
        normalized = line.strip()
        stop_lines = {
            "Like",
            "Comment",
            "Share",
            "Comments",
            "Related Reels",
            "Related Videos",
            "Pages",
            "Privacy",
            "See more on Facebook",
            "Email or phone number",
            "Password",
            "Create new account",
        }
        return normalized in stop_lines

    @staticmethod
    def _normalize_mobile_text(text: str) -> str:
        cleaned = "".join(ch for ch in text.replace("\xa0", " ") if unicodedata.category(ch) != "Co")
        cleaned = cleaned.replace("􏍸", " ").replace("􏤦", " ").replace("􏌫", " ")
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    @staticmethod
    def _is_mobile_ui_text(text: str) -> bool:
        return text in {
            "Open app",
            "Log in",
            "Follow",
            "Reels",
            "Photos",
            "Videos",
            "See all",
            "Create new account",
        }

    @staticmethod
    def _is_mobile_timeline_end(text: str) -> bool:
        return text.startswith("There's more to see") or text.startswith("See more from ")

    @staticmethod
    def _extract_mobile_published_hint(text: str) -> str:
        match = re.search(r"\b\d+\s*(?:m(?:in)?s?|h|d|w)\b", text, flags=re.IGNORECASE)
        return match.group(0) if match else ""

    @classmethod
    def _looks_like_mobile_post_header(cls, text: str, page_name: str) -> bool:
        if not cls._extract_mobile_published_hint(text):
            return False
        first_line = text.splitlines()[0].strip()
        return first_line.startswith(page_name) or first_line == page_name or page_name in first_line

    @classmethod
    def _extract_mobile_post_message(cls, group: list[dict[str, str]], page_name: str) -> str:
        for item in group[1:]:
            text = item["text"]
            if cls._looks_like_mobile_post_header(text, page_name):
                continue
            if re.fullmatch(r"\d+", text):
                continue
            if re.fullmatch(r".+ and \d+ others", text):
                continue
            if len(text) < 20:
                continue
            return re.sub(r"\s+See more$", "", text).strip()
        return ""

    @classmethod
    def _extract_mobile_reactions(cls, group: list[dict[str, str]]) -> int:
        for item in group[1:]:
            match = re.search(r"\band (\d+) others\b", item["text"], flags=re.IGNORECASE)
            if match:
                return int(match.group(1)) + 1
        numeric = [int(item["text"]) for item in group[1:] if re.fullmatch(r"\d+", item["text"])]
        return numeric[0] if numeric else 0

    @classmethod
    def _extract_mobile_comment_count(cls, group: list[dict[str, str]]) -> int:
        saw_reaction_line = False
        numeric_after_reaction: list[int] = []
        for item in group:
            text = item["text"]
            if re.search(r"\band \d+ others\b", text, flags=re.IGNORECASE):
                saw_reaction_line = True
                continue
            if saw_reaction_line and re.fullmatch(r"\d+", text):
                numeric_after_reaction.append(int(text))
        if len(numeric_after_reaction) >= 2:
            return numeric_after_reaction[1]
        return 0

    @staticmethod
    def _normalize_permalink(url: str) -> str:
        if not url:
            return ""
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        keep_keys = ("story_fbid", "id", "comment_id", "fbid")
        normalized_query = [(key, query[key]) for key in keep_keys if key in query]
        return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), urlencode(normalized_query), ""))

    @staticmethod
    def _normalize_post_permalink(url: str) -> str:
        if not url:
            return ""
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        keep_keys = ("story_fbid", "id", "fbid")
        normalized_query = [(key, query[key]) for key in keep_keys if key in query]
        return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), urlencode(normalized_query), ""))

    @classmethod
    def _select_post_permalink(
        cls,
        *,
        payload_post_permalink: str | None,
        candidate_permalink: str | None,
        detail_url: str,
    ) -> str:
        generic_paths = {"/reel", "/videos", "/watch"}
        primary = cls._normalize_post_permalink(payload_post_permalink or "")
        if primary and urlsplit(primary).path not in generic_paths:
            return primary
        for fallback in (
            cls._normalize_post_permalink(candidate_permalink or ""),
            cls._normalize_post_permalink(detail_url),
        ):
            if fallback and urlsplit(fallback).path not in generic_paths:
                return fallback
        return primary or cls._normalize_post_permalink(candidate_permalink or "") or cls._normalize_post_permalink(detail_url)

    @staticmethod
    def _page_plugin_url(page_url: str) -> str:
        return urlunsplit(
            (
                "https",
                "www.facebook.com",
                "/plugins/page.php",
                urlencode(
                    {
                        "href": page_url,
                        "tabs": "timeline",
                        "width": "500",
                        "height": "5000",
                        "small_header": "false",
                        "adapt_container_width": "true",
                        "hide_cover": "false",
                        "show_facepile": "false",
                        "locale": "en_US",
                    }
                ),
                "",
            )
        )

    @staticmethod
    def _page_tab_url(page_url: str, tab_name: str) -> str:
        normalized_page_url = page_url.rstrip("/")
        return PublicWebCollector._with_locale(f"{normalized_page_url}/{tab_name}")

    @staticmethod
    def _mobile_page_url(page_url: str) -> str:
        parts = urlsplit(page_url)
        return urlunsplit(("https", "m.facebook.com", parts.path.rstrip("/"), "", ""))

    @staticmethod
    def _with_locale(url: str) -> str:
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["locale"] = "en_US"
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    @staticmethod
    def _parse_post_timestamp(raw_hint: str) -> str | None:
        hint = PublicWebCollector._normalize_mobile_text(raw_hint or "").replace("\u202f", " ").replace("\xa0", " ").strip()
        if not hint:
            return None
        parsed = PublicWebCollector._parse_timestamp_token(hint)
        if parsed:
            return parsed
        extracted_hint = PublicWebCollector._extract_supported_date_hint_safe(hint)
        if extracted_hint and extracted_hint != hint:
            return PublicWebCollector._parse_timestamp_token(extracted_hint)
        return None

    @staticmethod
    def _parse_timestamp_token(hint: str) -> str | None:
        now = datetime.now(tz=UTC).replace(microsecond=0)
        relative_patterns = [
            (r"^(\d+)\s*m(?:in)?s?$", "minutes"),
            (r"^(\d+)\s*h(?:r|rs)?s?$", "hours"),
            (r"^(\d+)\s*d(?:ay|ays)?s?$", "days"),
            (r"^(\d+)\s*w(?:eek|eeks)?s?$", "weeks"),
        ]
        lowered = hint.lower()
        for pattern, unit in relative_patterns:
            match = re.match(pattern, lowered)
            if match:
                delta_value = int(match.group(1))
                delta = timedelta(**{unit: delta_value})
                return (now - delta).isoformat()

        if lowered == "yesterday":
            return (now - timedelta(days=1)).isoformat()
        if lowered.startswith("yesterday at "):
            try:
                parsed_time = datetime.strptime(lowered.replace("yesterday at ", ""), "%I:%M %p").time()
                return datetime.combine((now - timedelta(days=1)).date(), parsed_time, tzinfo=UTC).isoformat()
            except ValueError:
                return (now - timedelta(days=1)).isoformat()

        formats = [
            "%B %d at %I:%M %p",
            "%b %d at %I:%M %p",
            "%B %d, %Y at %I:%M %p",
            "%b %d, %Y at %I:%M %p",
            "%B %d",
            "%b %d",
            "%B %d, %Y",
            "%b %d, %Y",
        ]
        for fmt in formats:
            try:
                parsed = datetime.strptime(hint, fmt)
                year = parsed.year if "%Y" in fmt else now.year
                final_dt = parsed.replace(year=year, tzinfo=UTC)
                return final_dt.isoformat()
            except ValueError:
                continue
        return PublicWebCollector._parse_localized_absolute_timestamp_safe(hint, now)

    @staticmethod
    def _extract_supported_date_hint(text: str) -> str:
        normalized = PublicWebCollector._normalize_mobile_text(text).strip(" .,;:|()[]")
        if not normalized:
            return ""
        patterns = [
            r"\b\d+\s*(?:m(?:in)?s?|h(?:r|rs)?s?|d(?:ay|ays)?s?|w(?:eek|eeks)?s?)\b",
            r"\byesterday(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
            r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
            r"\b\d{1,2}\s+[A-Za-z\u0400-\u04FF]+(?:\s+\d{4})?(?:\s*(?:року|года|р\.?))?\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if match:
                return match.group(0).strip(" .,;:|()[]")
        return ""

    @staticmethod
    def _parse_localized_absolute_timestamp(hint: str, now: datetime) -> str | None:
        month_map = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "sept": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
            "січня": 1,
            "лютого": 2,
            "березня": 3,
            "квітня": 4,
            "травня": 5,
            "червня": 6,
            "липня": 7,
            "серпня": 8,
            "вересня": 9,
            "жовтня": 10,
            "листопада": 11,
            "грудня": 12,
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12,
        }
        match = re.search(
            r"\b(\d{1,2})\s+([A-Za-z\u0400-\u04FF]+)(?:\s+(\d{4}))?(?:\s*(?:року|года|р\.?))?\b",
            hint,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        day = int(match.group(1))
        month = month_map.get(match.group(2).lower())
        if not month:
            return None
        year = int(match.group(3)) if match.group(3) else now.year
        try:
            return datetime(year, month, day, tzinfo=UTC).isoformat()
        except ValueError:
            return None

    @staticmethod
    def _extract_supported_date_hint_safe(text: str) -> str:
        normalized = PublicWebCollector._normalize_mobile_text(text).strip(" .,;:|()[]")
        if not normalized:
            return ""
        localized_suffix = r"(?:\s*(?:\u0440\u043e\u043a\u0443|\u0433\u043e\u0434\u0430|\u0440\.?))?"
        patterns = [
            r"\b\d+\s*(?:m(?:in)?s?|h(?:r|rs)?s?|d(?:ay|ays)?s?|w(?:eek|eeks)?s?)\b",
            r"\byesterday(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
            r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
            rf"\b\d{{1,2}}\s+[A-Za-z\u0400-\u04FF]+(?:\s+\d{{4}})?{localized_suffix}\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if match:
                return match.group(0).strip(" .,;:|()[]")
        return ""

    @staticmethod
    def _parse_localized_absolute_timestamp_safe(hint: str, now: datetime) -> str | None:
        month_map = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "sept": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
            "\u0441\u0456\u0447\u043d\u044f": 1,
            "\u043b\u044e\u0442\u043e\u0433\u043e": 2,
            "\u0431\u0435\u0440\u0435\u0437\u043d\u044f": 3,
            "\u043a\u0432\u0456\u0442\u043d\u044f": 4,
            "\u0442\u0440\u0430\u0432\u043d\u044f": 5,
            "\u0447\u0435\u0440\u0432\u043d\u044f": 6,
            "\u043b\u0438\u043f\u043d\u044f": 7,
            "\u0441\u0435\u0440\u043f\u043d\u044f": 8,
            "\u0432\u0435\u0440\u0435\u0441\u043d\u044f": 9,
            "\u0436\u043e\u0432\u0442\u043d\u044f": 10,
            "\u043b\u0438\u0441\u0442\u043e\u043f\u0430\u0434\u0430": 11,
            "\u0433\u0440\u0443\u0434\u043d\u044f": 12,
            "\u044f\u043d\u0432\u0430\u0440\u044f": 1,
            "\u0444\u0435\u0432\u0440\u0430\u043b\u044f": 2,
            "\u043c\u0430\u0440\u0442\u0430": 3,
            "\u0430\u043f\u0440\u0435\u043b\u044f": 4,
            "\u043c\u0430\u044f": 5,
            "\u0438\u044e\u043d\u044f": 6,
            "\u0438\u044e\u043b\u044f": 7,
            "\u0430\u0432\u0433\u0443\u0441\u0442\u0430": 8,
            "\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044f": 9,
            "\u043e\u043a\u0442\u044f\u0431\u0440\u044f": 10,
            "\u043d\u043e\u044f\u0431\u0440\u044f": 11,
            "\u0434\u0435\u043a\u0430\u0431\u0440\u044f": 12,
        }
        match = re.search(
            r"\b(\d{1,2})\s+([A-Za-z\u0400-\u04FF]+)(?:\s+(\d{4}))?(?:\s*(?:\u0440\u043e\u043a\u0443|\u0433\u043e\u0434\u0430|\u0440\.?))?\b",
            hint,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        day = int(match.group(1))
        month = month_map.get(match.group(2).lower())
        if not month:
            return None
        year = int(match.group(3)) if match.group(3) else now.year
        try:
            return datetime(year, month, day, tzinfo=UTC).isoformat()
        except ValueError:
            return None
