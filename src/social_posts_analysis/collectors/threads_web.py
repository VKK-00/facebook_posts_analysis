from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote_plus

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
from .range_utils import RangeFilter
from .web_runtime import WebCollectorRuntime, ensure_playwright_available, open_web_runtime, scroll_page


class ThreadsWebCollector(BaseCollector):
    name = "threads_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.threads_web
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Threads web collector is disabled in config.collector.threads_web.enabled.")
        ensure_playwright_available("Threads web collector requires the playwright package and browser install.")

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings = ["Threads web extraction is best-effort and reply visibility depends on the current public web UI."]
        profile_url = self._resolve_profile_url()
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            try:
                page = runtime.context.new_page()
                page.goto(profile_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                self._scroll_timeline(page)
                payload = self._extract_profile_payload(page)
                source_path = raw_store.write_json("threads_web_source", "profile_feed", payload)
                source_name = payload.get("source_name") or self.config.source.source_name or self._source_reference()
                source_id = payload.get("source_id") or self._source_reference()
                posts = self._build_posts_from_payload(payload, source_id=source_id, source_name=source_name, raw_store=raw_store)
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    replies = self._collect_replies_for_post(context=runtime.context, post=post, raw_store=raw_store)
                    updated_posts.append(
                        post.model_copy(update={"comments": replies, "comments_count": max(post.comments_count, len(replies))})
                    )
            finally:
                runtime.close()

        source_snapshot = SourceSnapshot(
            platform="threads",
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

    def discover_person_monitor_sources(
        self,
        *,
        queries: list[str],
        include_posts: bool,
        include_comments: bool,
        max_items_per_query: int,
    ) -> list[dict[str, str | None]]:
        from playwright.sync_api import sync_playwright

        if not include_posts:
            return []

        discovered: dict[str, dict[str, str | None]] = {}
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            try:
                for query in queries:
                    page = runtime.context.new_page()
                    try:
                        page.goto(
                            self._search_url(query),
                            wait_until="domcontentloaded",
                            timeout=int(self.settings.timeout_seconds * 1000),
                        )
                        self._scroll_timeline(page, passes=self._search_scroll_passes(max_items_per_query))
                        payload = self._extract_search_payload(page)
                    finally:
                        page.close()
                    for item in self._search_surface_payloads(
                        payload,
                        include_posts=include_posts,
                        include_comments=include_comments,
                    ):
                        source_id = self._normalized_source_username(item.get("source_id"))
                        if not source_id:
                            continue
                        discovered[source_id] = item
            finally:
                runtime.close()
        return list(discovered.values())

    def _build_posts_from_payload(
        self,
        payload: dict[str, Any],
        *,
        source_id: str,
        source_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        posts: list[PostSnapshot] = []
        for item in payload.get("posts") or []:
            if not self._within_range(item.get("created_at")):
                continue
            post_id = f"threads:{source_id}:{item['status_id']}"
            raw_path = raw_store.write_json("threads_web_posts", slugify(post_id), item)
            origin_external_id = item.get("origin_status_id") or None
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="threads",
                    source_id=source_id,
                    origin_post_id=f"threads:origin:{origin_external_id}" if origin_external_id else None,
                    origin_external_id=origin_external_id,
                    origin_permalink=item.get("origin_permalink") or None,
                    propagation_kind=item.get("propagation_kind") or None,
                    is_propagation=bool(item.get("propagation_kind")),
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    raw_text=item.get("raw_text"),
                    permalink=item.get("permalink"),
                    reactions=parse_compact_number(item.get("like_count")),
                    shares=parse_compact_number(item.get("repost_count")),
                    comments_count=parse_compact_number(item.get("reply_count")),
                    views=parse_compact_number(item.get("view_count")) or None,
                    forwards=1 if item.get("propagation_kind") == "quote" else None,
                    reply_count=parse_compact_number(item.get("reply_count")),
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username") or source_id,
                        name=item.get("author_name") or source_name,
                        profile_url=f"https://www.threads.net/@{item.get('author_username')}" if item.get("author_username") else profile_url_from_name(source_id),
                    ),
                )
            )
        return posts

    def _collect_replies_for_post(self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        if not post.permalink:
            return []
        page = context.new_page()
        try:
            page.goto(post.permalink, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
            self._scroll_timeline(page, passes=max(3, self.settings.max_scrolls // 2))
            payload = self._extract_detail_payload(page)
            raw_store.write_json("threads_web_replies", slugify(post.post_id), payload)
        finally:
            page.close()
        comments: list[CommentSnapshot] = []
        comment_id_map: dict[str, str] = {}
        depth_map: dict[str, int] = {}
        for item in payload.get("replies") or []:
            if not self._within_range(item.get("created_at")):
                continue
            status_id = str(item.get("status_id") or "")
            if not status_id:
                continue
            comment_id = f"{post.post_id}:comment:{status_id}"
            parent_native_id = str(item.get("reply_to_status_id") or self._native_status_id(post.post_id))
            parent_comment_id = comment_id_map.get(parent_native_id) if parent_native_id != self._native_status_id(post.post_id) else None
            depth = depth_map.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
            raw_path = raw_store.write_json("threads_web_reply_items", slugify(comment_id), item)
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="threads",
                parent_post_id=post.post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=parent_native_id,
                thread_root_post_id=post.post_id,
                created_at=item.get("created_at"),
                message=item.get("text"),
                raw_text=item.get("raw_text"),
                permalink=item.get("permalink"),
                reactions=parse_compact_number(item.get("like_count")),
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("author_username"),
                    name=item.get("author_name"),
                    profile_url=f"https://www.threads.net/@{item.get('author_username')}" if item.get("author_username") else None,
                ),
            )
            comments.append(snapshot)
            comment_id_map[status_id] = snapshot.comment_id
            depth_map[snapshot.comment_id] = snapshot.depth
        return comments

    def _extract_search_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const containers = Array.from(document.querySelectorAll('[data-pressable-container="true"]'));
              const results = containers.map((node) => {
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => {
                  const href = anchor.getAttribute('href') || '';
                  return /^\\/@[^/]+$/.test(href);
                });
                const postLink = Array.from(node.querySelectorAll('a[href*="/post/"]')).find((anchor) => {
                  const href = anchor.getAttribute('href') || '';
                  return href.includes('/post/') && !href.includes('/media');
                });
                if (!authorLink || !postLink) {
                  return null;
                }
                const authorHref = authorLink.getAttribute('href') || '';
                const authorUsername = authorHref.split('@')[1]?.split(/[/?#]/)[0] || '';
                const timeNode = node.querySelector('time');
                return {
                  author_username: authorUsername,
                  author_name: authorUsername,
                  permalink: postLink.href || '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: (node.innerText || '').trim(),
                  is_reply: false,
                };
              }).filter((item) => item && item.author_username && item.permalink);
              return { results };
            }
            """
        )

    def _search_surface_payloads(
        self,
        payload: dict[str, Any],
        *,
        include_posts: bool,
        include_comments: bool,
    ) -> list[dict[str, str | None]]:
        if not include_posts:
            return []
        surfaces: dict[str, dict[str, str | None]] = {}
        for item in payload.get("results") or []:
            if not self._within_range(item.get("created_at")):
                continue
            if not self._search_item_matches_kind(
                item,
                include_posts=include_posts,
                include_comments=include_comments,
            ):
                continue
            author_username = self._normalized_source_username(item.get("author_username"))
            if not author_username:
                continue
            surfaces[author_username] = {
                "source_id": author_username,
                "source_name": (item.get("author_name") or author_username),
                "source_url": profile_url_from_name(author_username),
                "source_type": "account",
            }
        return list(surfaces.values())

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        payload = page.evaluate(
            """
            () => {
              const articles = Array.from(document.querySelectorAll('article'));
              const posts = articles.map((node) => {
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/post/"]'));
                const permalink = statusLinks[0]?.href || '';
                const originPermalink = statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '';
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const originStatusId = originPermalink ? originPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const timeNode = node.querySelector('time');
                const textNode = node.querySelector('[data-pressable-container="true"] div[dir="auto"], div[dir="auto"]');
                const allText = node.innerText || '';
                const metricFromText = (label) => {
                  const match = allText.match(new RegExp(`(\\\\d+(?:\\\\.\\\\d+)?[KMB]?)\\\\s+${label}`, 'i'));
                  return match ? match[1] : '';
                };
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => anchor.href.includes('/@'));
                const authorUsername = authorLink ? (authorLink.getAttribute('href') || '').split('@')[1].split(/[/?#]/)[0] : '';
                return {
                  permalink,
                  status_id: statusId,
                  origin_permalink: originPermalink,
                  origin_status_id: originStatusId,
                  propagation_kind: originStatusId ? (allText.toLowerCase().includes('quote') ? 'quote' : 'repost') : '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNode ? textNode.innerText.trim() : '',
                  author_name: authorUsername,
                  author_username: authorUsername,
                  reply_count: metricFromText('repl'),
                  repost_count: metricFromText('repost'),
                  like_count: metricFromText('like'),
                  view_count: metricFromText('view'),
                  has_media: Boolean(node.querySelector('img, video')),
                  media_type: node.querySelector('video') ? 'video' : (node.querySelector('img') ? 'photo' : null),
                };
              }).filter((item) => item.status_id);
              const pressablePosts = Array.from(document.querySelectorAll('[data-pressable-container="true"]')).map((node) => {
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => {
                  const href = anchor.getAttribute('href') || '';
                  return /^\\/@[^/]+$/.test(href);
                });
                const postLinks = Array.from(new Set(Array.from(node.querySelectorAll('a[href*="/post/"]'))
                  .map((anchor) => anchor.href || '')
                  .filter((href) => href && !href.includes('/media'))));
                const permalink = postLinks[0] || '';
                if (!authorLink || !permalink) {
                  return null;
                }
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                if (!statusId) {
                  return null;
                }
                const originPermalink = postLinks.length > 1 ? (postLinks[1] || '') : '';
                const originStatusId = originPermalink ? originPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const authorHref = authorLink.getAttribute('href') || '';
                const authorUsername = authorHref.split('@')[1]?.split(/[/?#]/)[0] || '';
                const timeNode = node.querySelector('time');
                return {
                  permalink,
                  status_id: statusId,
                  origin_permalink: originPermalink,
                  origin_status_id: originStatusId,
                  propagation_kind: originStatusId ? 'quote' : '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: '',
                  raw_text: (node.innerText || '').trim(),
                  author_name: (authorLink.innerText || '').trim() || authorUsername,
                  author_username: authorUsername,
                  reply_count: '',
                  repost_count: '',
                  like_count: '',
                  view_count: '',
                  has_media: Boolean(node.querySelector('img, video')),
                  media_type: node.querySelector('video') ? 'video' : (node.querySelector('img') ? 'photo' : null),
                };
              }).filter((item) => item && item.status_id);
              return {
                source_name: (document.querySelector('h1')?.innerText || document.title || '').trim(),
                source_id: (location.pathname.split('@')[1] || '').split(/[/?#]/)[0],
                source_url: location.href,
                posts,
                pressable_posts: pressablePosts,
              };
            }
            """
        )
        merged_posts = self._merge_profile_post_candidates(
            payload.get("posts") or [],
            payload.get("pressable_posts") or [],
        )
        return {
            "source_name": payload.get("source_name"),
            "source_id": payload.get("source_id"),
            "source_url": payload.get("source_url"),
            "posts": merged_posts,
        }

    def _extract_detail_payload(self, page: Any) -> dict[str, Any]:
        payload = page.evaluate(
            """
            () => {
              const articles = Array.from(document.querySelectorAll('article'));
              const rows = articles.map((node) => {
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/post/"]'));
                const permalink = statusLinks[0]?.href || '';
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const repliedToPermalink = statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '';
                const replyToStatusId = repliedToPermalink ? repliedToPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const timeNode = node.querySelector('time');
                const textNode = node.querySelector('[data-pressable-container="true"] div[dir="auto"], div[dir="auto"]');
                const allText = node.innerText || '';
                const metricFromText = (label) => {
                  const match = allText.match(new RegExp(`(\\\\d+(?:\\\\.\\\\d+)?[KMB]?)\\\\s+${label}`, 'i'));
                  return match ? match[1] : '';
                };
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => anchor.href.includes('/@'));
                const authorUsername = authorLink ? (authorLink.getAttribute('href') || '').split('@')[1].split(/[/?#]/)[0] : '';
                return {
                  permalink,
                  status_id: statusId,
                  reply_to_status_id: replyToStatusId,
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNode ? textNode.innerText.trim() : '',
                  raw_text: allText.trim(),
                  author_name: authorUsername,
                  author_username: authorUsername,
                  like_count: metricFromText('like'),
                };
              }).filter((item) => item.status_id);
              const pressableRows = Array.from(document.querySelectorAll('[data-pressable-container="true"]')).map((node) => {
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => {
                  const href = anchor.getAttribute('href') || '';
                  return /^\\/@[^/]+$/.test(href);
                });
                const postLinks = Array.from(new Set(Array.from(node.querySelectorAll('a[href*="/post/"]'))
                  .map((anchor) => anchor.href || '')
                  .filter((href) => href && !href.includes('/media'))));
                const permalink = postLinks[0] || '';
                if (!authorLink || !permalink) {
                  return null;
                }
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                if (!statusId) {
                  return null;
                }
                const repliedToPermalink = postLinks.find((href) => href !== permalink) || '';
                const replyToStatusId = repliedToPermalink ? repliedToPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const authorHref = authorLink.getAttribute('href') || '';
                const authorUsername = authorHref.split('@')[1]?.split(/[/?#]/)[0] || '';
                const timeNode = node.querySelector('time');
                return {
                  permalink,
                  status_id: statusId,
                  reply_to_status_id: replyToStatusId,
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: '',
                  raw_text: (node.innerText || '').trim(),
                  author_name: (authorLink.innerText || '').trim() || authorUsername,
                  author_username: authorUsername,
                  like_count: '',
                };
              }).filter((item) => item && item.status_id);
              return {
                main_status_id: rows.length ? rows[0].status_id : '',
                rows,
                pressable_rows: pressableRows,
              };
            }
            """
        )
        merged_rows = self._merge_detail_rows(
            payload.get("rows") or [],
            payload.get("pressable_rows") or [],
        )
        main_status_id = str(payload.get("main_status_id") or (merged_rows[0]["status_id"] if merged_rows else ""))
        normalized_rows = self._attach_detail_reply_targets(merged_rows, main_status_id=main_status_id)
        ordered_rows = self._order_detail_rows(normalized_rows, main_status_id=main_status_id)
        replies = ordered_rows[1:] if ordered_rows else []
        return {
            "main_status_id": main_status_id,
            "replies": replies,
        }

    @classmethod
    def _merge_profile_post_candidates(
        cls,
        primary_posts: list[dict[str, Any]],
        fallback_posts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for candidate in primary_posts:
            status_id = str(candidate.get("status_id") or "").strip()
            if not status_id:
                continue
            merged[status_id] = candidate
        for candidate in fallback_posts:
            normalized = cls._normalize_profile_fallback_candidate(candidate)
            status_id = str(normalized.get("status_id") or "").strip()
            if not status_id:
                continue
            existing = merged.get(status_id)
            if existing is None or cls._profile_post_score(normalized) > cls._profile_post_score(existing):
                merged[status_id] = normalized
        return list(merged.values())

    @classmethod
    def _merge_detail_rows(
        cls,
        primary_rows: list[dict[str, Any]],
        fallback_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for candidate in primary_rows:
            status_id = str(candidate.get("status_id") or "").strip()
            if not status_id:
                continue
            merged[status_id] = candidate
            order.append(status_id)
        for candidate in fallback_rows:
            normalized = cls._normalize_detail_fallback_candidate(candidate)
            status_id = str(normalized.get("status_id") or "").strip()
            if not status_id:
                continue
            existing = merged.get(status_id)
            if existing is None:
                merged[status_id] = normalized
                order.append(status_id)
                continue
            if cls._detail_row_score(normalized) > cls._detail_row_score(existing):
                merged[status_id] = normalized
        return [merged[status_id] for status_id in order if status_id in merged]

    @classmethod
    def _normalize_detail_fallback_candidate(cls, candidate: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(candidate)
        raw_text = candidate.get("raw_text")
        normalized["text"] = cls._extract_visible_post_text(
            raw_text,
            author_username=str(candidate.get("author_username") or ""),
            author_name=str(candidate.get("author_name") or ""),
        )
        normalized["like_count"] = candidate.get("like_count") or cls._extract_last_metric_value(raw_text)
        return normalized

    @staticmethod
    def _detail_row_score(item: dict[str, Any]) -> int:
        score = 0
        if item.get("text"):
            score += 4
        if item.get("created_at"):
            score += 2
        if item.get("permalink"):
            score += 2
        if item.get("reply_to_status_id"):
            score += 1
        if item.get("like_count"):
            score += 1
        return score

    @classmethod
    def _attach_detail_reply_targets(
        cls,
        rows: list[dict[str, Any]],
        *,
        main_status_id: str,
    ) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []
        for index, candidate in enumerate(rows):
            normalized = dict(candidate)
            status_id = str(normalized.get("status_id") or "").strip()
            reply_to_status_id = str(normalized.get("reply_to_status_id") or "").strip()
            if index == 0 or status_id == main_status_id:
                normalized["reply_to_status_id"] = ""
            elif not reply_to_status_id or reply_to_status_id == status_id:
                normalized["reply_to_status_id"] = main_status_id
            normalized_rows.append(normalized)
        return normalized_rows

    @staticmethod
    def _order_detail_rows(
        rows: list[dict[str, Any]],
        *,
        main_status_id: str,
    ) -> list[dict[str, Any]]:
        by_status_id: dict[str, dict[str, Any]] = {}
        original_order: list[str] = []
        for row in rows:
            status_id = str(row.get("status_id") or "").strip()
            if not status_id or status_id in by_status_id:
                continue
            by_status_id[status_id] = row
            original_order.append(status_id)

        ordered: list[dict[str, Any]] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(status_id: str) -> None:
            if status_id in visited:
                return
            row = by_status_id.get(status_id)
            if row is None:
                return
            if status_id in visiting:
                return
            visiting.add(status_id)
            reply_to_status_id = str(row.get("reply_to_status_id") or "").strip()
            if (
                reply_to_status_id
                and reply_to_status_id != main_status_id
                and reply_to_status_id != status_id
                and reply_to_status_id in by_status_id
            ):
                visit(reply_to_status_id)
            visiting.remove(status_id)
            if status_id not in visited:
                ordered.append(row)
                visited.add(status_id)

        for status_id in original_order:
            visit(status_id)
        return ordered

    @classmethod
    def _normalize_profile_fallback_candidate(cls, candidate: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(candidate)
        normalized["text"] = cls._extract_visible_post_text(
            candidate.get("raw_text"),
            author_username=str(candidate.get("author_username") or ""),
            author_name=str(candidate.get("author_name") or ""),
        )
        return normalized

    @staticmethod
    def _profile_post_score(item: dict[str, Any]) -> int:
        score = 0
        if item.get("text"):
            score += 4
        if item.get("created_at"):
            score += 2
        if item.get("permalink"):
            score += 2
        if item.get("origin_status_id"):
            score += 1
        if item.get("has_media"):
            score += 1
        return score

    @classmethod
    def _extract_visible_post_text(
        cls,
        raw_text: str | None,
        *,
        author_username: str,
        author_name: str,
    ) -> str:
        if not raw_text:
            return ""
        lines = [line.strip() for line in re.split(r"[\r\n]+", raw_text) if line.strip()]
        if not lines:
            return ""
        author_tokens = {
            author_username.strip().casefold(),
            author_name.strip().casefold(),
        }
        output: list[str] = []
        started = False
        for line in lines:
            lowered = line.casefold()
            if not started and (
                not lowered
                or lowered in author_tokens
                or cls._is_threads_time_hint(line)
                or cls._is_threads_profile_ui_line(line)
            ):
                continue
            if started and (
                cls._is_threads_metric_line(line)
                or cls._is_threads_post_stop_line(line)
            ):
                break
            if cls._is_threads_post_stop_line(line):
                if started:
                    break
                continue
            output.append(line)
            started = True
        return "\n".join(output).strip()

    @staticmethod
    def _is_threads_time_hint(value: str) -> bool:
        stripped = value.strip()
        return bool(
            re.fullmatch(r"\d+[smhdwy]", stripped, flags=re.IGNORECASE)
            or re.fullmatch(r"\d{2}/\d{2}/\d{2,4}", stripped)
        )

    @staticmethod
    def _is_threads_metric_line(value: str) -> bool:
        return bool(re.fullmatch(r"\d+(?:[.,]\d+)?[KMB]?", value.strip(), flags=re.IGNORECASE))

    @staticmethod
    def _extract_last_metric_value(raw_text: str | None) -> str:
        if not raw_text:
            return ""
        lines = [line.strip() for line in re.split(r"[\r\n]+", raw_text) if line.strip()]
        for line in reversed(lines):
            if re.fullmatch(r"\d+(?:[.,]\d+)?[KMB]?", line, flags=re.IGNORECASE):
                return line
            if line and not ThreadsWebCollector._is_threads_metric_summary_line(line):
                break
        return ""

    @staticmethod
    def _is_threads_metric_summary_line(value: str) -> bool:
        normalized = re.sub(r"[·•]", " ", value.strip().casefold())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return False
        return bool(
            re.fullmatch(
                r"(?:\d+(?:[.,]\d+)?[kmb]?\s+(?:reply|replies|repost|reposts|like|likes|view|views)\s*)+",
                normalized,
            )
        )

    @staticmethod
    def _is_threads_profile_ui_line(value: str) -> bool:
        stripped = value.strip()
        lowered = stripped.casefold()
        if lowered in {
            "log in",
            "follow",
            "mention",
            "threads",
            "replies",
            "media",
            "reposts",
            "thread",
            "related threads",
        }:
            return True
        return bool(re.fullmatch(r"\d+(?:[.,]\d+)?[kmb]?\s+followers", lowered))

    @classmethod
    def _is_threads_post_stop_line(cls, value: str) -> bool:
        stripped = value.strip()
        lowered = stripped.casefold()
        if cls._is_threads_profile_ui_line(stripped):
            return True
        if cls._is_threads_metric_summary_line(stripped):
            return True
        if lowered.startswith("log in to see more from"):
            return True
        if lowered.startswith("© ") or lowered.startswith("â© "):
            return True
        if lowered in {
            "threads terms",
            "privacy policy",
            "cookies policy",
            "report a problem",
        }:
            return True
        if lowered.startswith("allow the use of cookies"):
            return True
        return False

    @staticmethod
    def _search_item_matches_kind(
        item: dict[str, Any],
        *,
        include_posts: bool,
        include_comments: bool,
    ) -> bool:
        del item, include_comments
        return include_posts

    def _open_collection_context(self, playwright: Any) -> WebCollectorRuntime:
        return open_web_runtime(
            playwright,
            headless=self.settings.headless,
            browser_channel=self.settings.browser_channel,
            viewport={"width": 1400, "height": 1800},
            authenticated_browser=self.settings.authenticated_browser,
            profile_copy_prefix="threads-web-profile-",
            custom_user_data_error="Threads authenticated browser mode requires collector.threads_web.authenticated_browser.user_data_dir.",
        )

    def _scroll_timeline(self, page: Any, *, passes: int | None = None) -> None:
        scroll_page(
            page,
            max_scrolls=self.settings.max_scrolls,
            wait_after_scroll_ms=self.settings.wait_after_scroll_ms,
            passes=passes,
            wheel_y=2600,
        )

    def _search_url(self, query: str) -> str:
        return f"https://www.threads.net/search?q={quote_plus(query)}"

    def _search_scroll_passes(self, max_items_per_query: int) -> int:
        return max(2, min(self.settings.max_scrolls, max_items_per_query // 8 + 1))

    @staticmethod
    def _normalized_source_username(value: str | None) -> str:
        return (value or "").strip().lstrip("@").lower()

    def _resolve_profile_url(self) -> str:
        if self.config.source.url:
            return self.config.source.url.rstrip("/")
        return profile_url_from_name(self._source_reference())

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name.lstrip("@")
        if self.config.source.source_id:
            return self.config.source.source_id
        if self.config.source.url:
            return self.config.source.url.rstrip("/").split("@")[-1].split("/")[-1]
        raise CollectorUnavailableError("Threads web collector requires source.url, source.source_name, or source.source_id.")

    def _within_range(self, raw_value: str | None) -> bool:
        return self.range_filter.contains(raw_value, allow_missing=False)

    @staticmethod
    def _native_status_id(post_id: str) -> str:
        return post_id.split(":")[-1]


def profile_url_from_name(name: str) -> str:
    return f"https://www.threads.net/@{name.lstrip('@')}"
