from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import parse_compact_number, slugify, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError


class XWebCollector(BaseCollector):
    name = "x_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.x_web
        if not self.settings.enabled:
            raise CollectorUnavailableError("X web collector is disabled in config.collector.x_web.enabled.")
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
        except ImportError as exc:
            raise CollectorUnavailableError("X web collector requires the playwright package and browser install.") from exc

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        profile_url = self._resolve_profile_url()
        warnings = [
            "X web extraction is best-effort and public replies may be limited without an authenticated browser session."
        ]

        with sync_playwright() as playwright:
            browser, context, temp_profile_dir, context_warnings = self._open_collection_context(playwright)
            warnings.extend(context_warnings)
            try:
                page = context.new_page()
                page.goto(profile_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                self._dismiss_cookie_banner(page)
                self._scroll_timeline(page)
                profile_payload = self._extract_profile_payload(page)
                source_path = raw_store.write_json("x_web_source", "profile_feed", profile_payload)

                source_name = profile_payload.get("source_name") or self.config.source.source_name or self._source_reference()
                source_id = profile_payload.get("source_id") or self._source_reference()
                posts = self._build_posts_from_payload(profile_payload, source_id=source_id, source_name=source_name, raw_store=raw_store)
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    replies = self._collect_replies_for_post(context=context, post=post, raw_store=raw_store)
                    if post.comments_count > 0 and not replies:
                        warnings.append(
                            f"X web detail page for {post.post_id} exposed reply counter {post.comments_count}, but no reply articles were visible."
                        )
                    updated_posts.append(
                        post.model_copy(update={"comments": replies, "comments_count": max(post.comments_count, len(replies))})
                    )
            finally:
                context.close()
                if browser is not None:
                    browser.close()
                if temp_profile_dir is not None:
                    shutil.rmtree(temp_profile_dir, ignore_errors=True)

        source_snapshot = SourceSnapshot(
            platform="x",
            source_id=source_id,
            source_name=source_name,
            source_url=profile_url,
            source_type="account",
            source_collector=self.name,
            raw_path=str(source_path),
        )
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=list(dict.fromkeys(warnings)),
            source=source_snapshot,
            posts=updated_posts,
        )

    def _build_posts_from_payload(
        self,
        payload: dict[str, Any],
        *,
        source_id: str,
        source_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        posts: list[PostSnapshot] = []
        expected_username = self._normalized_source_username(source_id)
        for item in payload.get("posts") or []:
            author_username = self._normalized_source_username(item.get("author_username"))
            if author_username and expected_username and author_username != expected_username:
                continue
            if not self._within_range(item.get("created_at")):
                continue
            post_id = f"x:{source_id}:{item['status_id']}"
            reaction_breakdown = {
                "reply_count": parse_compact_number(item.get("reply_count")),
                "retweet_count": parse_compact_number(item.get("retweet_count")),
                "like_count": parse_compact_number(item.get("like_count")),
                "view_count": parse_compact_number(item.get("view_count")),
            }
            raw_path = raw_store.write_json("x_web_posts", slugify(post_id), item)
            origin_external_id = item.get("origin_status_id") or None
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="x",
                    source_id=source_id,
                    origin_post_id=f"x:origin:{origin_external_id}" if origin_external_id else None,
                    origin_external_id=origin_external_id,
                    origin_permalink=item.get("origin_permalink") or (
                        f"https://x.com/i/status/{origin_external_id}" if origin_external_id else None
                    ),
                    propagation_kind=item.get("propagation_kind"),
                    is_propagation=bool(item.get("propagation_kind")),
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    permalink=item.get("permalink"),
                    reactions=reaction_breakdown["like_count"],
                    shares=reaction_breakdown["retweet_count"],
                    comments_count=reaction_breakdown["reply_count"],
                    views=reaction_breakdown["view_count"],
                    forwards=None,
                    reply_count=reaction_breakdown["reply_count"],
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    reaction_breakdown_json=json.dumps(reaction_breakdown, ensure_ascii=False),
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username") or source_id,
                        name=item.get("author_name") or source_name,
                        profile_url=f"https://x.com/{item.get('author_username')}" if item.get("author_username") else self.config.source.url,
                    ),
                    comments=[],
                )
            )
        return posts

    def _collect_replies_for_post(self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        if not post.permalink:
            return []
        page = context.new_page()
        try:
            page.goto(post.permalink, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
            self._dismiss_cookie_banner(page)
            self._scroll_timeline(page, passes=max(3, self.settings.max_scrolls // 2))
            detail_payload = self._extract_status_payload(page)
            raw_store.write_json("x_web_replies", slugify(self._native_status_id(post.post_id)), detail_payload)
        finally:
            page.close()

        reply_snapshots: list[CommentSnapshot] = []
        for item in detail_payload.get("replies") or []:
            if not self._within_range(item.get("created_at")):
                continue
            status_id = str(item.get("status_id") or "")
            if not status_id:
                continue
            comment_id = f"x:{post.source_id}:{self._native_status_id(post.post_id)}:comment:{status_id}"
            reaction_breakdown = {
                "reply_count": parse_compact_number(item.get("reply_count")),
                "retweet_count": parse_compact_number(item.get("retweet_count")),
                "like_count": parse_compact_number(item.get("like_count")),
                "view_count": parse_compact_number(item.get("view_count")),
            }
            raw_path = raw_store.write_json("x_web_reply_items", slugify(comment_id), item)
            reply_snapshots.append(
                CommentSnapshot(
                    comment_id=comment_id,
                    platform="x",
                    parent_post_id=post.post_id,
                    parent_comment_id=None,
                    reply_to_message_id=self._native_status_id(post.post_id),
                    thread_root_post_id=post.post_id,
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    permalink=item.get("permalink"),
                    reactions=reaction_breakdown["like_count"],
                    reaction_breakdown_json=json.dumps(reaction_breakdown, ensure_ascii=False),
                    source_collector=self.name,
                    depth=0,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username"),
                        name=item.get("author_name"),
                        profile_url=f"https://x.com/{item.get('author_username')}" if item.get("author_username") else None,
                    ),
                )
            )
        return reply_snapshots

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        payload = page.evaluate(
            """
            () => {
              const articleNodes = Array.from(document.querySelectorAll('article[data-testid="tweet"]'));
              const posts = articleNodes.map((node) => {
                const permalinkNode = Array.from(node.querySelectorAll('a[href*="/status/"]'))
                  .find((anchor) => !anchor.href.includes('/analytics'));
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/status/"]'))
                  .filter((anchor) => !anchor.href.includes('/analytics'));
                const timeNode = node.querySelector('time');
                const textNodes = Array.from(node.querySelectorAll('[data-testid="tweetText"]'));
                const socialContext = Array.from(node.querySelectorAll('span'))
                  .map((span) => (span.textContent || '').trim())
                  .find((value) => /reposted|quoted/i.test(value)) || '';
                const metric = (testId) => {
                  const metricNode = node.querySelector(`[data-testid="${testId}"]`);
                  return metricNode ? (metricNode.textContent || '').trim() : '';
                };
                const userNameNode = node.querySelector('[data-testid="User-Name"]');
                const nameLinks = userNameNode ? Array.from(userNameNode.querySelectorAll('a')) : [];
                const authorName = (nameLinks[0]?.textContent || '').trim();
                const authorUsername = ((nameLinks[1]?.textContent || '').trim() || '').replace(/^@/, '');
                const mediaType = node.querySelector('[data-testid="tweetPhoto"]')
                  ? 'photo'
                  : node.querySelector('video')
                    ? 'video'
                    : null;
                const viewsAnchor = Array.from(node.querySelectorAll('a[href*="/analytics"]'))[0];
                return {
                  permalink: permalinkNode?.href || '',
                  status_id: permalinkNode ? (permalinkNode.href.split('/status/')[1] || '').split(/[/?#]/)[0] : '',
                  origin_permalink: statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '',
                  origin_status_id: statusLinks.length > 1 ? ((statusLinks[1]?.href.split('/status/')[1] || '').split(/[/?#]/)[0]) : '',
                  propagation_kind: /quoted/i.test(socialContext)
                    ? 'quote'
                    : /reposted/i.test(socialContext)
                      ? 'repost'
                      : '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNodes.length ? (textNodes[0].innerText || '').trim() : '',
                  author_name: authorName,
                  author_username: authorUsername,
                  reply_count: metric('reply'),
                  retweet_count: metric('retweet'),
                  like_count: metric('like'),
                  view_count: (viewsAnchor?.textContent || '').trim(),
                  has_media: Boolean(mediaType),
                  media_type: mediaType,
                };
              }).filter((item) => item.status_id);
              const titleBits = document.title.split('(');
              return {
                page_title: document.title,
                source_name: (document.querySelector('h2[role="heading"]')?.innerText || titleBits[0] || '').trim(),
                source_id: location.pathname.replace(/^\\//, '').split('/')[0],
                source_url: location.href,
                posts,
              };
            }
            """
        )
        return payload

    def _extract_status_payload(self, page: Any) -> dict[str, Any]:
        payload = page.evaluate(
            """
            () => {
              const articleNodes = Array.from(document.querySelectorAll('article[data-testid="tweet"]'));
              const rows = articleNodes.map((node) => {
                const permalinkNode = Array.from(node.querySelectorAll('a[href*="/status/"]'))
                  .find((anchor) => !anchor.href.includes('/analytics'));
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/status/"]'))
                  .filter((anchor) => !anchor.href.includes('/analytics'));
                const timeNode = node.querySelector('time');
                const textNodes = Array.from(node.querySelectorAll('[data-testid="tweetText"]'));
                const socialContext = Array.from(node.querySelectorAll('span'))
                  .map((span) => (span.textContent || '').trim())
                  .find((value) => /reposted|quoted/i.test(value)) || '';
                const metric = (testId) => {
                  const metricNode = node.querySelector(`[data-testid="${testId}"]`);
                  return metricNode ? (metricNode.textContent || '').trim() : '';
                };
                const userNameNode = node.querySelector('[data-testid="User-Name"]');
                const nameLinks = userNameNode ? Array.from(userNameNode.querySelectorAll('a')) : [];
                const authorName = (nameLinks[0]?.textContent || '').trim();
                const authorUsername = ((nameLinks[1]?.textContent || '').trim() || '').replace(/^@/, '');
                const viewsAnchor = Array.from(node.querySelectorAll('a[href*="/analytics"]'))[0];
                return {
                  permalink: permalinkNode?.href || '',
                  status_id: permalinkNode ? (permalinkNode.href.split('/status/')[1] || '').split(/[/?#]/)[0] : '',
                  origin_permalink: statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '',
                  origin_status_id: statusLinks.length > 1 ? ((statusLinks[1]?.href.split('/status/')[1] || '').split(/[/?#]/)[0]) : '',
                  propagation_kind: /quoted/i.test(socialContext)
                    ? 'quote'
                    : /reposted/i.test(socialContext)
                      ? 'repost'
                      : '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNodes.length ? (textNodes[0].innerText || '').trim() : '',
                  author_name: authorName,
                  author_username: authorUsername,
                  reply_count: metric('reply'),
                  retweet_count: metric('retweet'),
                  like_count: metric('like'),
                  view_count: (viewsAnchor?.textContent || '').trim(),
                };
              }).filter((item) => item.status_id);
              return {
                main_status_id: rows.length ? rows[0].status_id : '',
                replies: rows.length ? rows.slice(1) : [],
              };
            }
            """
        )
        return payload

    def _open_collection_context(self, playwright: Any) -> tuple[Any | None, Any, Path | None, list[str]]:
        if self._uses_authenticated_browser():
            context, temp_profile_dir, warnings = self._open_authenticated_context(playwright)
            return None, context, temp_profile_dir, warnings
        browser = playwright.chromium.launch(headless=self.settings.headless, channel=self.settings.browser_channel)
        context = browser.new_context(locale="en-US", viewport={"width": 1400, "height": 1800})
        return browser, context, None, []

    def _open_authenticated_context(self, playwright: Any) -> tuple[Any, Path | None, list[str]]:
        auth_settings = self.settings.authenticated_browser
        source_user_data_dir = self._resolve_authenticated_user_data_dir()
        profile_directory = auth_settings.profile_directory
        launch_user_data_dir = source_user_data_dir
        temp_profile_dir: Path | None = None
        warnings: list[str] = []
        if auth_settings.copy_profile:
            temp_profile_dir = self._prepare_temp_profile_directory(source_user_data_dir, profile_directory)
            launch_user_data_dir = temp_profile_dir
            warnings.append(f"Using authenticated browser profile snapshot from {source_user_data_dir} ({profile_directory}).")

        args = [f"--profile-directory={profile_directory}"] if profile_directory else []
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(launch_user_data_dir),
            channel=self._resolve_authenticated_browser_channel(),
            headless=self.settings.headless,
            locale="en-US",
            viewport={"width": 1400, "height": 1800},
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
            raise CollectorUnavailableError("Authenticated X browser mode requires collector.x_web.authenticated_browser.user_data_dir.")
        if not resolved_path.exists():
            raise CollectorUnavailableError(f"Authenticated browser user data dir does not exist: {resolved_path}")
        return resolved_path

    def _prepare_temp_profile_directory(self, user_data_dir: Path, profile_directory: str) -> Path:
        temp_root = self.settings.authenticated_browser.temp_root_dir
        temp_dir = Path(tempfile.mkdtemp(prefix="x-web-profile-", dir=temp_root))
        profile_name = profile_directory or "Default"
        shutil.copytree(user_data_dir / profile_name, temp_dir / profile_name, dirs_exist_ok=True)
        for root_file in ("Local State", "First Run"):
            source_path = user_data_dir / root_file
            if source_path.exists():
                shutil.copy2(source_path, temp_dir / root_file)
        return temp_dir

    def _resolve_authenticated_browser_channel(self) -> str | None:
        browser_name = self.settings.authenticated_browser.browser
        if browser_name == "custom":
            return self.settings.browser_channel
        return browser_name

    def _uses_authenticated_browser(self) -> bool:
        return self.settings.authenticated_browser.enabled

    def _dismiss_cookie_banner(self, page: Any) -> None:
        for label in ("Refuse non-essential cookies", "Accept all cookies"):
            try:
                button = page.get_by_text(label, exact=True)
                if button.count():
                    button.first.click(timeout=2000)
                    page.wait_for_timeout(500)
                    break
            except Exception:
                continue

    def _scroll_timeline(self, page: Any, *, passes: int | None = None) -> None:
        for _ in range(passes or self.settings.max_scrolls):
            page.mouse.wheel(0, 2600)
            page.wait_for_timeout(self.settings.wait_after_scroll_ms)

    def _resolve_profile_url(self) -> str:
        if self.config.source.url:
            return self.config.source.url.rstrip("/")
        reference = self._source_reference().lstrip("@")
        return f"https://x.com/{reference}"

    @staticmethod
    def _normalized_source_username(value: str | None) -> str:
        return (value or "").strip().lstrip("@").lower()

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name
        if self.config.source.source_id:
            return self.config.source.source_id
        if self.config.source.url:
            parsed = urlparse(self.config.source.url)
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return parts[0]
        raise CollectorUnavailableError("X web collector requires source.url, source.source_name, or source.source_id.")

    def _within_range(self, created_at: str | None) -> bool:
        if created_at is None:
            return False
        try:
            current = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        start = self._parse_date(self.config.date_range.start, end_of_day=False)
        end = self._parse_date(self.config.date_range.end, end_of_day=True)
        if start and current < start:
            return False
        if end and current > end:
            return False
        return True

    @staticmethod
    def _parse_date(raw_value: str | None, *, end_of_day: bool) -> datetime | None:
        if not raw_value:
            return None
        try:
            if "T" in raw_value:
                parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            else:
                parsed = datetime.fromisoformat(f"{raw_value}T23:59:59+00:00" if end_of_day else f"{raw_value}T00:00:00+00:00")
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _native_status_id(post_id: str) -> str:
        match = re.search(r":(\d+)$", post_id)
        return match.group(1) if match else post_id
