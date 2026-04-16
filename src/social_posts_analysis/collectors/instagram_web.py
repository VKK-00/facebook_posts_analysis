from __future__ import annotations

import re
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
from social_posts_analysis.utils import parse_compact_number, slugify, stable_id, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError
from .range_utils import RangeFilter
from .web_runtime import WebCollectorRuntime, ensure_playwright_available, open_web_runtime, scroll_page


class InstagramWebCollector(BaseCollector):
    name = "instagram_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.instagram_web
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Instagram web collector is disabled in config.collector.instagram_web.enabled.")
        ensure_playwright_available("Instagram web collector requires the playwright package and browser install.")

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings = ["Instagram web extraction is best-effort and public comment visibility depends on the current web UI."]
        profile_url = self._resolve_profile_url()
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            try:
                page = runtime.context.new_page()
                page.goto(profile_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                self._scroll_timeline(page)
                payload = self._extract_profile_payload(page)
                source_path = raw_store.write_json("instagram_web_source", "profile_feed", payload)
                source_name = payload.get("source_name") or self.config.source.source_name or self._source_reference()
                source_id = payload.get("source_id") or self._source_reference()
                posts = self._build_posts_from_payload(payload, source_id=source_id, source_name=source_name, raw_store=raw_store)
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    comments = self._collect_comments_for_post(context=runtime.context, post=post, raw_store=raw_store)
                    updated_posts.append(
                        post.model_copy(update={"comments": comments, "comments_count": max(post.comments_count, len(comments))})
                    )
            finally:
                runtime.close()

        source_snapshot = SourceSnapshot(
            platform="instagram",
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
            warnings=warnings,
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
        for raw_item in payload.get("posts") or []:
            item = self._normalize_post_payload_item(raw_item)
            status_id = str(item.get("status_id") or "")
            if not status_id:
                continue
            if not self._within_range(item.get("created_at")):
                continue
            post_id = f"instagram:{source_id}:{status_id}"
            raw_path = raw_store.write_json("instagram_web_posts", slugify(post_id), item)
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="instagram",
                    source_id=source_id,
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    raw_text=item.get("raw_text"),
                    permalink=item.get("permalink"),
                    reactions=parse_compact_number(item.get("like_count")),
                    shares=0,
                    comments_count=parse_compact_number(item.get("comment_count")),
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username") or source_id,
                        name=item.get("author_name") or source_name,
                        profile_url=profile_url_from_name(item.get("author_username") or source_id),
                    ),
                )
            )
        return posts

    def discover_person_monitor_sources(
        self,
        *,
        queries: list[str],
        include_posts: bool,
        include_comments: bool,
        max_items_per_query: int,
    ) -> list[dict[str, str | None]]:
        if not include_posts and not include_comments:
            return []
        discovered: dict[str, dict[str, str | None]] = {}
        for query in queries:
            payload = self._resolve_search_discovery_profile(query)
            if not payload:
                continue
            source_id = payload.get("source_id")
            if source_id:
                discovered[source_id] = payload
        return list(discovered.values())

    def _collect_comments_for_post(self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        if not post.permalink:
            return []
        page = context.new_page()
        try:
            page.goto(post.permalink, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
            self._scroll_timeline(page, passes=max(2, self.settings.max_scrolls // 2))
            payload = self._extract_post_payload(page)
            raw_store.write_json("instagram_web_comments", slugify(post.post_id), payload)
        finally:
            page.close()
        comments: list[CommentSnapshot] = []
        comment_id_map: dict[str, str] = {}
        depth_map: dict[str, int] = {}
        for index, raw_item in enumerate(payload.get("comments") or []):
            item = self._normalize_comment_payload_item(raw_item, index=index)
            if item is None:
                continue
            if not self._within_range(item.get("created_at")):
                continue
            status_id = str(item.get("comment_id") or "")
            if not status_id:
                continue
            comment_id = f"{post.post_id}:comment:{status_id}"
            parent_native_id = str(item.get("reply_to_comment_id") or "")
            parent_comment_id = comment_id_map.get(parent_native_id) if parent_native_id else None
            depth = depth_map.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
            raw_path = raw_store.write_json("instagram_web_comment_items", slugify(comment_id), item)
            author_username = str(item.get("author_username") or "")
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="instagram",
                parent_post_id=post.post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=parent_native_id or None,
                thread_root_post_id=post.post_id,
                created_at=item.get("created_at"),
                message=item.get("text"),
                raw_text=item.get("raw_text"),
                permalink=None,
                reactions=parse_compact_number(item.get("like_count")),
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=author_username or None,
                    name=item.get("author_name"),
                    profile_url=profile_url_from_name(author_username) if author_username else None,
                ),
            )
            comments.append(snapshot)
            comment_id_map[status_id] = snapshot.comment_id
            depth_map[snapshot.comment_id] = snapshot.depth
        return comments

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const canonicalPostUrl = (href) => {
                try {
                  const url = new URL(href, 'https://www.instagram.com');
                  const parts = url.pathname.split('/').filter(Boolean);
                  if (parts.length >= 2 && ['p', 'reel'].includes(parts[0])) {
                    return `https://www.instagram.com/${parts[0]}/${parts[1]}/`;
                  }
                } catch (error) {}
                return href;
              };
              const links = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'));
              const seen = new Set();
              const posts = links.map((anchor) => {
                const href = canonicalPostUrl(anchor.href || '');
                if (seen.has(href)) return null;
                seen.add(href);
                const imageNode = anchor.querySelector('img');
                const rawText = (anchor.innerText || imageNode?.getAttribute('alt') || '').trim();
                return {
                  permalink: href,
                  status_id: href.includes('/reel/')
                    ? href.split('/reel/')[1].split(/[/?#]/)[0]
                    : href.split('/p/')[1].split(/[/?#]/)[0],
                  created_at: null,
                  text: imageNode?.getAttribute('alt') || '',
                  raw_text: rawText,
                  author_name: (document.querySelector('header section h2, header section h1')?.textContent || '').trim(),
                  author_username: (location.pathname.replace(/^\\//, '').split('/')[0] || '').trim(),
                  comment_count: '',
                  like_count: '',
                  has_media: Boolean(imageNode),
                  media_type: href.includes('/reel/') ? 'reel' : 'photo',
                };
              }).filter(Boolean);
              return {
                source_name: (document.querySelector('header section h2, header section h1')?.textContent || document.title || '').trim(),
                source_id: location.pathname.replace(/^\\//, '').split('/')[0],
                source_url: location.href,
                posts,
              };
            }
            """
        )

    def _extract_post_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const isProfileHref = (href) => {
                const parts = (href || '').split('?')[0].split('#')[0].split('/').filter(Boolean);
                if (parts.length !== 1) return false;
                return /^[A-Za-z0-9._]{1,30}$/.test(parts[0]);
              };
              const isUiNoise = (value) => {
                const text = (value || '').trim().toLowerCase();
                return !text || ['reply', 'see translation', 'view replies', 'view reply'].includes(text);
              };
              const isCommentCandidate = (node) => {
                const rawText = (node.innerText || '').trim();
                const authorLink = Array.from(node.querySelectorAll('a[href^="/"]')).find((anchor) => isProfileHref(anchor.getAttribute('href') || ''));
                const timeNode = node.querySelector('time[datetime]');
                return Boolean(rawText && (authorLink || timeNode || node.getAttribute('data-comment-id')));
              };
              const commentNodes = Array.from(document.querySelectorAll('article ul li, div[role="dialog"] ul li, ul li')).filter(isCommentCandidate);
              const comments = commentNodes.map((node, index) => {
                const authorLink = Array.from(node.querySelectorAll('a[href^="/"]')).find((anchor) => isProfileHref(anchor.getAttribute('href') || ''));
                const timeNode = node.querySelector('time');
                const textParts = Array.from(node.querySelectorAll('span')).map((span) => (span.textContent || '').trim()).filter((value) => value && !isUiNoise(value));
                const authorUsername = authorLink ? (authorLink.getAttribute('href') || '').replaceAll('/', '').split('?')[0].split('#')[0] : '';
                const authorName = textParts[0] || authorUsername;
                return {
                  comment_id: node.getAttribute('data-comment-id') || node.id || String(index + 1),
                  reply_to_comment_id: node.getAttribute('data-parent-comment-id') || '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textParts.slice(1).join(' ').trim(),
                  raw_text: (node.innerText || '').trim(),
                  author_name: authorName,
                  author_username: authorUsername,
                  like_count: '',
                };
              });
              return { comments };
            }
            """
        )

    @classmethod
    def _normalize_post_payload_item(cls, item: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(item)
        permalink = canonical_instagram_permalink(normalized.get("permalink"))
        if permalink:
            normalized["permalink"] = permalink
        if not normalized.get("status_id"):
            normalized["status_id"] = instagram_status_id_from_permalink(permalink)
        author_username = instagram_username_from_reference(normalized.get("author_username"))
        if author_username:
            normalized["author_username"] = author_username
        return normalized

    @classmethod
    def _normalize_comment_payload_item(cls, item: dict[str, Any], *, index: int) -> dict[str, Any] | None:
        raw_text = clean_text(item.get("raw_text"))
        text = clean_text(item.get("text"))
        author_username = instagram_username_from_reference(item.get("author_username")) or ""
        author_name = clean_text(item.get("author_name"))
        created_at = clean_text(item.get("created_at"))
        if not text and raw_text:
            text = cls._derive_comment_text(raw_text, author_username=author_username, author_name=author_name)
        if not any([raw_text, text, author_username, author_name, created_at]):
            return None
        comment_id = clean_text(item.get("comment_id")) or stable_id(raw_text, text, author_username, created_at, str(index))
        return {
            "comment_id": comment_id,
            "reply_to_comment_id": clean_text(item.get("reply_to_comment_id")),
            "created_at": created_at or None,
            "text": text,
            "raw_text": raw_text,
            "author_name": author_name or author_username,
            "author_username": author_username,
            "like_count": clean_text(item.get("like_count")),
        }

    @classmethod
    def _derive_comment_text(cls, raw_text: str, *, author_username: str, author_name: str) -> str:
        author_tokens = {author_username.casefold(), author_name.casefold()} - {""}
        lines: list[str] = []
        for line in [part.strip() for part in re.split(r"[\r\n]+", raw_text) if part.strip()]:
            lowered = line.casefold()
            if lowered in author_tokens or cls._is_comment_ui_noise(line):
                continue
            lines.append(line)
        return " ".join(lines).strip()

    @staticmethod
    def _is_comment_ui_noise(value: str) -> bool:
        lowered = value.strip().casefold()
        if lowered in {"reply", "see translation", "view reply", "view replies"}:
            return True
        return bool(re.fullmatch(r"\d+\s*(s|m|h|d|w)", lowered))

    @staticmethod
    def _resolve_search_discovery_profile(query: str) -> dict[str, str | None] | None:
        username = instagram_username_from_reference(query)
        if not username:
            return None
        return {
            "source_id": username,
            "source_name": username,
            "source_url": profile_url_from_name(username),
            "source_type": "account",
        }

    def _open_collection_context(self, playwright: Any) -> WebCollectorRuntime:
        return open_web_runtime(
            playwright,
            headless=self.settings.headless,
            browser_channel=self.settings.browser_channel,
            viewport={"width": 1400, "height": 1800},
            authenticated_browser=self.settings.authenticated_browser,
            profile_copy_prefix="instagram-web-profile-",
            custom_user_data_error="Instagram authenticated browser mode requires collector.instagram_web.authenticated_browser.user_data_dir.",
        )

    def _scroll_timeline(self, page: Any, *, passes: int | None = None) -> None:
        scroll_page(
            page,
            max_scrolls=self.settings.max_scrolls,
            wait_after_scroll_ms=self.settings.wait_after_scroll_ms,
            passes=passes,
            wheel_y=2400,
        )

    def _resolve_profile_url(self) -> str:
        if self.config.source.url:
            username = instagram_username_from_reference(self.config.source.url)
            return profile_url_from_name(username) if username else self.config.source.url.rstrip("/")
        return profile_url_from_name(self._source_reference())

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name.lstrip("@")
        if self.config.source.source_id:
            return self.config.source.source_id
        if self.config.source.url:
            username = instagram_username_from_reference(self.config.source.url)
            if username:
                return username
        raise CollectorUnavailableError("Instagram web collector requires source.url, source.source_name, or source.source_id.")

    def _within_range(self, raw_value: str | None) -> bool:
        return self.range_filter.contains(raw_value, allow_missing=True)


def profile_url_from_name(name: str) -> str:
    username = instagram_username_from_reference(name) or name.strip().lstrip("@")
    return f"https://www.instagram.com/{username}/"


def canonical_instagram_permalink(value: Any) -> str:
    if not value:
        return ""
    raw_value = str(value).strip()
    parsed = urlparse(raw_value if "://" in raw_value else f"https://www.instagram.com/{raw_value.lstrip('/')}")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] in {"p", "reel"}:
        return f"https://www.instagram.com/{parts[0]}/{parts[1]}/"
    return raw_value.rstrip("/")


def instagram_status_id_from_permalink(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://www.instagram.com/{value.lstrip('/')}")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] in {"p", "reel"}:
        return parts[1]
    return ""


def instagram_username_from_reference(value: Any) -> str | None:
    if not value:
        return None
    candidate = str(value).strip().lstrip("@")
    if not candidate:
        return None
    if "://" in candidate:
        parsed = urlparse(candidate)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            return None
        candidate = parts[0].lstrip("@")
    candidate = candidate.strip("/").casefold()
    if candidate in {"p", "reel", "reels", "explore", "accounts", "stories", "direct"}:
        return None
    if re.fullmatch(r"[a-z0-9._]{1,30}", candidate):
        return candidate
    return None


def clean_text(value: Any) -> str:
    return str(value or "").strip()
