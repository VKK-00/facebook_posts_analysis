from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any

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
from .facebook_web_content import (
    build_comment_snapshots,
    canonical_post_text,
    clean_comment_text,
    clean_post_text,
    derive_comment_author,
    derive_comment_published_hint,
    derive_published_hint_from_body,
    extract_generic_post_text,
    extract_metric_count,
    extract_mobile_comment_count,
    extract_mobile_post_message,
    extract_mobile_published_hint,
    extract_mobile_reactions,
    is_comment_control_line,
    is_meaningful_post_text,
    is_mobile_timeline_end,
    is_mobile_ui_text,
    is_plausible_comment_author,
    is_stop_line,
    is_ui_line,
    looks_like_mobile_post_header,
    looks_like_name_token,
    merge_extracted_comments,
    normalize_permalink,
    normalize_post_permalink,
    parse_mobile_timeline_candidates,
    posts_match,
    select_comment_author,
    select_post_permalink,
)
from .facebook_web_extraction import (
    extract_feed_candidates,
    extract_mobile_timeline_payload,
    extract_photo_candidates,
    extract_plugin_feed_candidates,
    extract_post_page,
    extract_reel_candidates,
    extract_video_candidates,
    mobile_page_url,
    page_plugin_url,
    page_tab_url,
    postprocess_candidates,
    propagation_metadata,
    with_locale,
)
from .facebook_web_interactions import (
    accept_desktop_cookies,
    accept_mobile_cookies,
    click_buttonish_text,
    count_article_nodes,
    expand_comment_threads,
    prepare_post_detail_page,
    scroll_primary_comment_container,
)
from .facebook_web_timestamps import (
    epoch_seconds_to_iso,
    extract_embedded_published_at,
    extract_numeric_media_id,
    extract_supported_date_hint_safe,
    parse_localized_absolute_timestamp_safe,
    parse_post_timestamp,
    parse_timestamp_token,
)
from .range_utils import RangeFilter
from .web_runtime import WebCollectorRuntime, ensure_playwright_available, open_web_runtime


class PublicWebCollector(BaseCollector):
    name = "public_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.public_web
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Public web collector is disabled in config.collector.public_web.enabled.")
        ensure_playwright_available("Public web collector requires the playwright package and browser install.")

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings: list[str] = [
            "Public web extraction is best-effort and may require selector tuning for the target page."
        ]
        auth_login_wall_warning_added = False
        page_url = self.config.source.url or ""
        if not page_url:
            raise CollectorUnavailableError("Public web collector requires source.url.")

        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            discovery_runtime = runtime
            try:
                page_name = self.config.source.source_name
                page_payload: dict[str, Any] | None = None
                direct_feed_candidates: list[dict[str, Any]] = []
                plugin_candidates: list[dict[str, Any]] = []

                if self._uses_authenticated_browser():
                    page_payload, direct_feed_candidates = self._collect_direct_feed_candidates(
                        context=runtime.context,
                        page_url=page_url,
                        raw_store=raw_store,
                        snapshot_name="authenticated_feed_candidates",
                    )
                    if not page_name:
                        page_name = (page_payload.get("title") or "").split("|", 1)[0].strip() or None
                    discovery_runtime = self._open_public_context(playwright)

                if discovery_runtime is not None:
                    plugin_page = discovery_runtime.context.new_page()
                    plugin_payload: dict[str, Any] | None = None
                    try:
                        plugin_page.goto(
                            self._page_plugin_url(page_url),
                            wait_until="domcontentloaded",
                            timeout=int(self.settings.timeout_seconds * 1000),
                        )
                        plugin_page.wait_for_timeout(2500)
                        plugin_candidates = self._postprocess_candidates(self._extract_plugin_feed_candidates(plugin_page))
                        plugin_payload = {
                            "title": plugin_page.title(),
                            "url": plugin_page.url,
                            "candidates": plugin_candidates,
                        }
                        raw_store.write_json("web_extract", "page_plugin_candidates", plugin_payload)

                        if not page_name:
                            inferred_name = plugin_page.locator("body").inner_text().splitlines()
                            page_name = next((line.strip() for line in inferred_name if line.strip()), None)
                    finally:
                        plugin_page.close()
                    if page_payload is None and plugin_payload is not None:
                        page_payload = {
                            "title": plugin_payload["title"],
                            "url": plugin_payload["url"],
                        }

                video_candidates = self._discover_media_candidates(
                    context=discovery_runtime.context,
                    page_url=page_url,
                    tab_name="videos",
                    raw_store=raw_store,
                )
                photo_candidates = self._discover_media_candidates(
                    context=discovery_runtime.context,
                    page_url=page_url,
                    tab_name="photos",
                    raw_store=raw_store,
                )
                reel_candidates = self._discover_media_candidates(
                    context=discovery_runtime.context,
                    page_url=page_url,
                    tab_name="reels",
                    raw_store=raw_store,
                )
                candidates = direct_feed_candidates + plugin_candidates + video_candidates + photo_candidates + reel_candidates
                if not candidates:
                    page_payload, fallback_candidates = self._collect_direct_feed_candidates(
                        context=discovery_runtime.context,
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
                    post, login_wall_detected = self._collect_post_detail(
                        context=runtime.context,
                        page_id=page_id,
                        page_name=page_name,
                        candidate=candidate,
                        published_at=published_at,
                        raw_store=raw_store,
                    )
                    if login_wall_detected and self._uses_authenticated_browser() and not auth_login_wall_warning_added:
                        warnings.append(
                            "Authenticated browser mode is enabled, but Facebook still returned a login wall for at least one detail page. The selected browser profile may not be logged in to Facebook."
                        )
                        auth_login_wall_warning_added = True
                    if self.config.date_range.start or self.config.date_range.end:
                        if post.created_at is None:
                            warnings.append(f"Skipped post with unparsed timestamp: {permalink}")
                            continue
                        if not self._within_configured_range(post.created_at):
                            continue
                    existing = posts_by_id.get(post.post_id)
                    posts_by_id[post.post_id] = self._merge_post_snapshots(existing, post)

                if discovery_runtime is not None:
                    try:
                        mobile_posts = self._collect_mobile_timeline_posts(
                            browser=discovery_runtime.browser,
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
                if discovery_runtime is not runtime:
                    discovery_runtime.close()
                runtime.close()

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

    def _open_collection_context(self, playwright: Any) -> WebCollectorRuntime:
        if self._uses_authenticated_browser():
            return open_web_runtime(
                playwright,
                headless=self.settings.headless,
                browser_channel=self.settings.browser_channel,
                viewport={"width": 1400, "height": 1600},
                authenticated_browser=self.settings.authenticated_browser,
                profile_copy_prefix="facebook-posts-auth-",
                custom_user_data_error="Authenticated browser mode requires public_web.authenticated_browser.user_data_dir for browser='custom'.",
                missing_user_data_error_prefix="Browser user data directory does not exist",
                best_effort_profile_copy=True,
            )
        return self._open_public_context(playwright)

    def _open_public_context(self, playwright: Any) -> WebCollectorRuntime:
        return open_web_runtime(
            playwright,
            headless=self.settings.headless,
            browser_channel=self.settings.browser_channel,
            viewport={"width": 1400, "height": 1600},
            custom_user_data_error="Public web collector does not support authenticated browser mode in public context.",
        )

    def _collect_direct_feed_candidates(
        self,
        context: Any,
        page_url: str,
        raw_store: RawSnapshotStore,
        snapshot_name: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        feed_page = context.new_page()
        try:
            feed_page.goto(
                self._with_locale(page_url),
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            feed_page.wait_for_timeout(2500)
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
        finally:
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
    ) -> tuple[PostSnapshot, bool]:
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
        login_wall_detected = self._payload_looks_login_walled(payload)

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
        propagation_kind, origin_post_id, origin_external_id, origin_permalink = self._propagation_metadata(
            payload=payload,
            post_text=post_text,
            post_permalink=post_permalink,
        )
        if propagation_kind == "share":
            origin_post_id = self._resolve_visible_share_origin_post_id(
                page_id=page_id,
                origin_post_id=origin_post_id,
                origin_permalink=origin_permalink,
            )

        return PostSnapshot(
            post_id=post_id,
            platform="facebook",
            source_id=page_id,
            origin_post_id=origin_post_id,
            origin_external_id=origin_external_id,
            origin_permalink=origin_permalink,
            propagation_kind=propagation_kind,
            is_propagation=propagation_kind is not None,
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
        ), login_wall_detected

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
        return build_comment_snapshots(
            post_id=post_id,
            payload_comments=payload_comments,
            raw_path=raw_path,
            source_collector=self.name,
        )

    def _prepare_post_detail_page(
        self,
        page: Any,
        *,
        target_comment_count: int = 0,
        aggressive: bool = False,
        max_seconds: float | None = None,
    ) -> None:
        prepare_post_detail_page(
            page,
            target_comment_count=target_comment_count,
            aggressive=aggressive,
            max_seconds=max_seconds,
        )

    def _expand_comment_threads(self, page: Any, *, target_comment_count: int = 0, aggressive: bool = False) -> None:
        expand_comment_threads(
            page,
            target_comment_count=target_comment_count,
            aggressive=aggressive,
        )

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
        from .facebook_web_content import comment_article_limit

        return comment_article_limit(target_comment_count, aggressive)

    @staticmethod
    def _comment_sort_menu_patterns() -> list[str]:
        from .facebook_web_content import comment_sort_menu_patterns

        return comment_sort_menu_patterns()

    @staticmethod
    def _comment_sort_option_patterns(*, aggressive: bool) -> list[str]:
        from .facebook_web_content import comment_sort_option_patterns

        return comment_sort_option_patterns(aggressive=aggressive)

    @staticmethod
    def _comment_expansion_patterns() -> list[str]:
        from .facebook_web_content import comment_expansion_patterns

        return comment_expansion_patterns()

    @staticmethod
    def _reply_expansion_patterns() -> list[str]:
        from .facebook_web_content import reply_expansion_patterns

        return reply_expansion_patterns()

    @staticmethod
    def _accept_desktop_cookies(page: Any) -> None:
        accept_desktop_cookies(page)

    @staticmethod
    def _click_buttonish_text(
        page: Any,
        *,
        patterns: list[str],
        max_clicks: int,
        wait_ms: int,
    ) -> int:
        return click_buttonish_text(
            page,
            patterns=patterns,
            max_clicks=max_clicks,
            wait_ms=wait_ms,
        )

    @staticmethod
    def _scroll_primary_comment_container(page: Any) -> bool:
        return scroll_primary_comment_container(page)

    @staticmethod
    def _count_article_nodes(page: Any) -> int:
        return count_article_nodes(page)

    @staticmethod
    def _extract_feed_candidates(page: Any) -> list[dict[str, Any]]:
        return extract_feed_candidates(page)

    @staticmethod
    def _extract_plugin_feed_candidates(page: Any) -> list[dict[str, Any]]:
        return extract_plugin_feed_candidates(page)

    def _discover_media_candidates(
        self,
        context: Any,
        page_url: str,
        tab_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[dict[str, Any]]:
        tab_page = context.new_page()
        tab_url = self._page_tab_url(page_url, tab_name)
        try:
            tab_page.goto(
                tab_url,
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            tab_page.wait_for_timeout(2500)
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
        finally:
            tab_page.close()
        return self._postprocess_candidates(raw_candidates)

    @staticmethod
    def _extract_video_candidates(page: Any) -> list[dict[str, Any]]:
        return extract_video_candidates(page)

    @staticmethod
    def _extract_photo_candidates(page: Any) -> list[dict[str, Any]]:
        return extract_photo_candidates(page)

    @staticmethod
    def _extract_reel_candidates(page: Any) -> list[dict[str, Any]]:
        return extract_reel_candidates(page)

    @staticmethod
    def _extract_post_page(page: Any, *, comment_limit: int = 200) -> dict[str, Any]:
        payload = extract_post_page(page, comment_limit=comment_limit)
        payload["comments"] = merge_extracted_comments(
            payload.get("comments", []),
            payload.get("reel_fallback_comments", []),
            limit=comment_limit,
        )
        payload.pop("reel_fallback_comments", None)
        return payload

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
            propagation_kind, origin_post_id, origin_external_id, origin_permalink = self._propagation_metadata(
                payload=candidate,
                post_text=candidate.get("message") or "",
                post_permalink=None,
            )
            if propagation_kind == "share":
                origin_post_id = self._resolve_visible_share_origin_post_id(
                    page_id=page_id,
                    origin_post_id=origin_post_id,
                    origin_permalink=origin_permalink,
                )
            post = PostSnapshot(
                post_id=stable_id(
                    page_id,
                    candidate.get("published_hint") or "",
                    candidate.get("message") or "",
                ),
                platform="facebook",
                source_id=page_id,
                origin_post_id=origin_post_id,
                origin_external_id=origin_external_id,
                origin_permalink=origin_permalink,
                propagation_kind=propagation_kind,
                is_propagation=propagation_kind is not None,
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
    def _propagation_metadata(
        *,
        payload: dict[str, Any],
        post_text: str,
        post_permalink: str | None,
    ) -> tuple[str | None, str | None, str | None, str | None]:
        return propagation_metadata(
            payload=payload,
            post_text=post_text,
            post_permalink=post_permalink,
        )

    @staticmethod
    def _accept_mobile_cookies(page: Any) -> None:
        accept_mobile_cookies(page)

    @staticmethod
    def _extract_mobile_timeline_payload(page: Any) -> dict[str, Any]:
        return extract_mobile_timeline_payload(page)

    @classmethod
    def _parse_mobile_timeline_candidates(
        cls,
        raw_action_items: list[dict[str, Any]],
        page_name: str,
    ) -> list[dict[str, Any]]:
        return parse_mobile_timeline_candidates(raw_action_items, page_name)

    @classmethod
    def _postprocess_candidates(cls, raw_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return postprocess_candidates(raw_candidates)

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
        return posts_match(left, right)

    @classmethod
    def _canonical_post_text(cls, text: str) -> str:
        return canonical_post_text(text)

    def _resolve_visible_share_origin_post_id(
        self,
        *,
        page_id: str,
        origin_post_id: str | None,
        origin_permalink: str | None,
    ) -> str | None:
        if not origin_permalink or not self.config.source.url:
            return origin_post_id
        normalized_origin = self._normalize_post_permalink(origin_permalink)
        normalized_source = self._normalize_post_permalink(self.config.source.url)
        if normalized_origin.startswith(normalized_source.rstrip("/")):
            return stable_id(page_id, normalized_origin)
        return origin_post_id

    def _within_configured_range(self, published_at: str) -> bool:
        return self.range_filter.contains(published_at, allow_missing=True)

    @staticmethod
    def _clean_post_text(raw_text: str, published_hint: str) -> str:
        return clean_post_text(raw_text, published_hint)

    @staticmethod
    def _clean_comment_text(raw_text: str, author_name: str, published_hint: str) -> str:
        return clean_comment_text(raw_text, author_name, published_hint)

    @staticmethod
    def _is_comment_control_line(line: str) -> bool:
        return is_comment_control_line(line)

    @staticmethod
    def _select_comment_author(raw_author_name: str | None, raw_text: str) -> str | None:
        return select_comment_author(raw_author_name, raw_text)

    @staticmethod
    def _is_plausible_comment_author(value: str) -> bool:
        return is_plausible_comment_author(value)

    @staticmethod
    def _is_meaningful_post_text(text: str) -> bool:
        return is_meaningful_post_text(text)

    @staticmethod
    def _extract_generic_post_text(body_text: str, meta_description: str, page_name: str) -> str:
        return extract_generic_post_text(body_text, meta_description, page_name)

    @staticmethod
    def _derive_comment_author(raw_text: str) -> str | None:
        return derive_comment_author(raw_text)

    @staticmethod
    def _looks_like_name_token(token: str) -> bool:
        return looks_like_name_token(token)

    @staticmethod
    def _derive_comment_published_hint(raw_text: str) -> str:
        return derive_comment_published_hint(raw_text)

    @staticmethod
    def _derive_published_hint_from_body(body_text: str, page_name: str) -> str:
        return derive_published_hint_from_body(body_text, page_name)

    @staticmethod
    def _extract_embedded_published_at(
        html: str,
        *,
        detail_url: str,
        post_permalink: str | None,
    ) -> str | None:
        return extract_embedded_published_at(
            html,
            detail_url=detail_url,
            post_permalink=post_permalink,
        )

    @staticmethod
    def _extract_numeric_media_id(value: str) -> str | None:
        return extract_numeric_media_id(value)

    @staticmethod
    def _epoch_seconds_to_iso(raw_value: str) -> str | None:
        return epoch_seconds_to_iso(raw_value)

    @staticmethod
    def _extract_metric_count(raw_text: str) -> int:
        return extract_metric_count(raw_text)

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
    def _payload_looks_login_walled(payload: dict[str, Any]) -> bool:
        body_text = str(payload.get("body_text") or "")
        if not body_text:
            return False
        normalized = body_text.replace("\xa0", " ")
        markers = (
            "Log In",
            "Forgot password?",
            "Forgot Account?",
            "Create new account",
            "See more on Facebook",
            "Email or phone number",
            "Password",
        )
        return sum(1 for marker in markers if marker in normalized) >= 2

    @staticmethod
    def _is_ui_line(line: str, page_name: str) -> bool:
        return is_ui_line(line, page_name)

    @staticmethod
    def _is_stop_line(line: str) -> bool:
        return is_stop_line(line)

    @staticmethod
    def _normalize_mobile_text(text: str) -> str:
        cleaned = "".join(ch for ch in text.replace("\xa0", " ") if unicodedata.category(ch) != "Co")
        cleaned = cleaned.replace("􏍸", " ").replace("􏤦", " ").replace("􏌫", " ")
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    @staticmethod
    def _is_mobile_ui_text(text: str) -> bool:
        return is_mobile_ui_text(text)

    @staticmethod
    def _is_mobile_timeline_end(text: str) -> bool:
        return is_mobile_timeline_end(text)

    @staticmethod
    def _extract_mobile_published_hint(text: str) -> str:
        return extract_mobile_published_hint(text)

    @classmethod
    def _looks_like_mobile_post_header(cls, text: str, page_name: str) -> bool:
        return looks_like_mobile_post_header(text, page_name)

    @classmethod
    def _extract_mobile_post_message(cls, group: list[dict[str, str]], page_name: str) -> str:
        return extract_mobile_post_message(group, page_name)

    @classmethod
    def _extract_mobile_reactions(cls, group: list[dict[str, str]]) -> int:
        return extract_mobile_reactions(group)

    @classmethod
    def _extract_mobile_comment_count(cls, group: list[dict[str, str]]) -> int:
        return extract_mobile_comment_count(group)

    @staticmethod
    def _normalize_permalink(url: str) -> str:
        return normalize_permalink(url)

    @staticmethod
    def _normalize_post_permalink(url: str) -> str:
        return normalize_post_permalink(url)

    @classmethod
    def _select_post_permalink(
        cls,
        *,
        payload_post_permalink: str | None,
        candidate_permalink: str | None,
        detail_url: str,
    ) -> str:
        return select_post_permalink(
            payload_post_permalink=payload_post_permalink,
            candidate_permalink=candidate_permalink,
            detail_url=detail_url,
        )

    @staticmethod
    def _page_plugin_url(page_url: str) -> str:
        return page_plugin_url(page_url)

    @staticmethod
    def _page_tab_url(page_url: str, tab_name: str) -> str:
        return page_tab_url(page_url, tab_name)

    @staticmethod
    def _mobile_page_url(page_url: str) -> str:
        return mobile_page_url(page_url)

    @staticmethod
    def _with_locale(url: str) -> str:
        return with_locale(url)

    @staticmethod
    def _parse_post_timestamp(raw_hint: str) -> str | None:
        return parse_post_timestamp(raw_hint)

    @staticmethod
    def _parse_timestamp_token(hint: str) -> str | None:
        return parse_timestamp_token(hint)

    @staticmethod
    def _extract_supported_date_hint(text: str) -> str:
        return extract_supported_date_hint_safe(text)

    @staticmethod
    def _parse_localized_absolute_timestamp(hint: str, now: datetime) -> str | None:
        return parse_localized_absolute_timestamp_safe(hint, now)

    @staticmethod
    def _extract_supported_date_hint_safe(text: str) -> str:
        return extract_supported_date_hint_safe(text)

    @staticmethod
    def _parse_localized_absolute_timestamp_safe(hint: str, now: datetime) -> str | None:
        return parse_localized_absolute_timestamp_safe(hint, now)
