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
                source_id = instagram_username_from_reference(payload.get("source_id")) or self._source_reference()
                posts = self._build_posts_from_payload(payload, source_id=source_id, source_name=source_name, raw_store=raw_store)
                warnings.extend(self._profile_payload_warnings(payload, source_id=source_id))
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    comments = self._collect_comments_for_post(
                        context=runtime.context,
                        post=post,
                        raw_store=raw_store,
                        warnings=warnings,
                    )
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

    def diagnose_browser_session(self, target_url: str | None) -> dict[str, Any]:
        from playwright.sync_api import sync_playwright

        resolved_target_url = target_url or self._resolve_profile_url()
        settings = self.settings.authenticated_browser
        warnings: list[str] = []
        payload: dict[str, Any] = {}
        status = "runtime_error"
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            page = None
            try:
                page = runtime.context.new_page()
                page.goto(
                    resolved_target_url,
                    wait_until="domcontentloaded",
                    timeout=int(self.settings.timeout_seconds * 1000),
                )
                payload = self._extract_session_diagnostic_payload(page)
                status = self._session_diagnostic_status(payload)
            except Exception as exc:
                message = str(exc).strip() or exc.__class__.__name__
                warnings.append(f"Instagram web session diagnostic failed after browser launch: {message}")
            finally:
                if page is not None:
                    page.close()
                runtime.close()

        warnings.extend(self._session_diagnostic_warnings(status))
        target_status_id = instagram_status_id_from_permalink(resolved_target_url) or instagram_status_id_from_permalink(
            clean_text(payload.get("final_url"))
        )
        page_state = self._normalize_session_page_state(payload.get("page_state") or {})
        extraction_sources = payload.get("extraction_sources") or {}
        serialized_candidates = self._normalize_serialized_candidate_samples(
            payload.get("serialized_candidates") or {},
            target_status_id=target_status_id,
        )
        target_author_username = self._target_author_username(serialized_candidates)
        warnings.extend(self._target_author_warnings(target_author_username))
        return {
            "collector": self.name,
            "target_url": resolved_target_url,
            "final_url": clean_text(payload.get("final_url")) or resolved_target_url,
            "target_status_id": target_status_id,
            "target_author_username": target_author_username,
            "authenticated_browser_enabled": settings.enabled,
            "browser": settings.browser,
            "profile_directory": settings.profile_directory,
            "copy_profile": settings.copy_profile,
            "status": status,
            "page_state": page_state,
            "extraction_sources": {
                "post_links": self._int_value(extraction_sources.get("post_links")),
                "json_script_blocks": self._int_value(extraction_sources.get("json_script_blocks")),
                "media_candidates": self._int_value(extraction_sources.get("media_candidates")),
                "comment_candidates": self._int_value(extraction_sources.get("comment_candidates")),
                "target_media_candidates": self._int_value(extraction_sources.get("target_media_candidates")),
                "other_media_candidates": self._int_value(extraction_sources.get("other_media_candidates")),
            },
            "serialized_candidates": serialized_candidates,
            "serialized_structure": self._normalize_serialized_structure(payload.get("serialized_structure") or {}),
            "warnings": list(dict.fromkeys(warnings)),
            "body_sample": clean_text(payload.get("body_sample")),
        }

    def _collect_comments_for_post(
        self,
        *,
        context: Any,
        post: PostSnapshot,
        raw_store: RawSnapshotStore,
        warnings: list[str] | None = None,
    ) -> list[CommentSnapshot]:
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
        if warnings is not None:
            warnings.extend(self._post_payload_warnings(payload, post=post))
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
        payload = page.evaluate(
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
              const statusIdFromUrl = (href) => {
                try {
                  const url = new URL(href, 'https://www.instagram.com');
                  const parts = url.pathname.split('/').filter(Boolean);
                  if (parts.length >= 2 && ['p', 'reel'].includes(parts[0])) {
                    return parts[1];
                  }
                } catch (error) {}
                return '';
              };
              const textValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'string') return value.trim();
                return '';
              };
              const countValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'number') return String(value);
                if (typeof value === 'string') return value.trim();
                if (typeof value === 'object' && typeof value.count === 'number') return String(value.count);
                return '';
              };
              const firstText = (...values) => values.map(textValue).find(Boolean) || '';
              const captionText = (item) => {
                const edgeCaption = item?.edge_media_to_caption?.edges?.[0]?.node?.text;
                const caption = item?.caption;
                return firstText(
                  edgeCaption,
                  caption?.text,
                  caption?.caption,
                  caption,
                  item?.accessibility_caption,
                  item?.title,
                  item?.text
                );
              };
              const ownerUsername = (item) => firstText(
                item?.owner?.username,
                item?.user?.username,
                item?.profile_grid_owner?.username,
                item?.author?.username
              );
              const timestampValue = (value) => {
                if (!value) return null;
                const numeric = Number(value);
                if (!Number.isFinite(numeric)) return null;
                const milliseconds = numeric > 100000000000 ? numeric : numeric * 1000;
                return new Date(milliseconds).toISOString();
              };
              const mediaType = (item, permalink) => {
                const typeValue = String(item?.__typename || item?.media_type || item?.product_type || '').toLowerCase();
                if (permalink.includes('/reel/') || typeValue.includes('video') || typeValue.includes('clips')) return 'reel';
                return 'photo';
              };
              const candidateFromMedia = (item) => {
                if (!item || typeof item !== 'object') return null;
                const shortcode = firstText(item.shortcode, item.code);
                if (!shortcode || !/^[A-Za-z0-9_-]{3,}$/.test(shortcode)) return null;
                const hasMediaSignal = Boolean(
                  item.display_url ||
                  item.thumbnail_src ||
                  item.thumbnail_url ||
                  item.edge_media_to_caption ||
                  item.caption ||
                  item.taken_at_timestamp ||
                  item.owner ||
                  item.is_video !== undefined ||
                  item.like_count !== undefined ||
                  item.comment_count !== undefined
                );
                if (!hasMediaSignal) return null;
                const permalinkKind = item.is_video || String(item.product_type || '').toLowerCase().includes('clips') ? 'reel' : 'p';
                const permalink = `https://www.instagram.com/${permalinkKind}/${shortcode}/`;
                return {
                  permalink,
                  status_id: shortcode,
                  created_at: timestampValue(item.taken_at_timestamp || item.taken_at || item.created_time),
                  text: captionText(item),
                  raw_text: captionText(item),
                  author_name: ownerUsername(item),
                  author_username: ownerUsername(item),
                  comment_count: countValue(item.edge_media_to_comment) || countValue(item.comment_count),
                  like_count: countValue(item.edge_liked_by) || countValue(item.edge_media_preview_like) || countValue(item.like_count),
                  has_media: true,
                  media_type: mediaType(item, permalink),
                };
              };
              const collectMediaCandidates = (root, output, seenObjects) => {
                if (!root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                const candidate = candidateFromMedia(root);
                if (candidate) {
                  output.push(candidate);
                }
                if (Array.isArray(root)) {
                  for (const item of root) collectMediaCandidates(item, output, seenObjects);
                  return;
                }
                for (const value of Object.values(root)) {
                  collectMediaCandidates(value, output, seenObjects);
                }
              };
              const collectScriptPosts = () => {
                const posts = [];
                for (const script of Array.from(document.querySelectorAll('script[type="application/json"], script:not([src])'))) {
                  const raw = (script.textContent || '').trim();
                  if (!raw || (!raw.startsWith('{') && !raw.startsWith('['))) continue;
                  try {
                    const parsed = JSON.parse(raw);
                    collectMediaCandidates(parsed, posts, new Set());
                  } catch (error) {}
                }
                const byStatus = new Map();
                for (const post of posts) {
                  if (!post.status_id || byStatus.has(post.status_id)) continue;
                  byStatus.set(post.status_id, post);
                }
                return Array.from(byStatus.values());
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
                  status_id: statusIdFromUrl(href),
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
              }).filter((item) => item && item.status_id);
              const bodyText = (document.body?.innerText || '').trim();
              const lowerBodyText = bodyText.toLowerCase();
              const scriptPosts = collectScriptPosts();
              return {
                source_name: (document.querySelector('header section h2, header section h1')?.textContent || document.title || '').trim(),
                source_id: location.pathname.replace(/^\\//, '').split('/')[0],
                source_url: location.href,
                page_state: {
                  title: document.title || '',
                  url: location.href,
                  post_link_count: links.length,
                  script_post_count: scriptPosts.length,
                  body_text_sample: bodyText.slice(0, 500),
                  login_wall_detected: /log in|sign up|log into instagram|create an account/.test(lowerBodyText),
                  profile_unavailable_detected: /sorry, this page isn't available|page isn't available|content isn't available/.test(lowerBodyText),
                  serialized_data_detected: scriptPosts.length > 0,
                },
                posts,
                script_posts: scriptPosts,
              };
            }
            """
        )
        dom_posts = payload.get("posts") or []
        script_posts = payload.get("script_posts") or []
        merged_posts = self._merge_profile_post_candidates(dom_posts, script_posts)
        return {
            "source_name": payload.get("source_name"),
            "source_id": payload.get("source_id"),
            "source_url": payload.get("source_url"),
            "page_state": payload.get("page_state") or {},
            "extraction_sources": {
                "dom_posts": len(dom_posts),
                "script_posts": len(script_posts),
                "merged_posts": len(merged_posts),
            },
            "posts": merged_posts,
        }

    def _extract_post_payload(self, page: Any) -> dict[str, Any]:
        payload = page.evaluate(
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
              const textValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'string') return value.trim();
                return '';
              };
              const countValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'number') return String(value);
                if (typeof value === 'string') return value.trim();
                if (typeof value === 'object' && typeof value.count === 'number') return String(value.count);
                return '';
              };
              const firstText = (...values) => values.map(textValue).find(Boolean) || '';
              const timestampValue = (value) => {
                if (!value) return null;
                const numeric = Number(value);
                if (!Number.isFinite(numeric)) return textValue(value) || null;
                const milliseconds = numeric > 100000000000 ? numeric : numeric * 1000;
                return new Date(milliseconds).toISOString();
              };
              const statusIdFromUrl = (href) => {
                try {
                  const url = new URL(href, 'https://www.instagram.com');
                  const parts = url.pathname.split('/').filter(Boolean);
                  if (parts.length >= 2 && ['p', 'reel'].includes(parts[0])) return parts[1];
                } catch (error) {}
                return '';
              };
              const ownerUsername = (item) => firstText(
                item?.owner?.username,
                item?.user?.username,
                item?.author?.username,
                item?.from?.username
              );
              const ownerName = (item) => firstText(
                item?.owner?.full_name,
                item?.owner?.name,
                item?.user?.full_name,
                item?.user?.name,
                item?.author?.full_name,
                item?.author?.name,
                item?.from?.name,
                ownerUsername(item)
              );
              const commentText = (item) => firstText(item?.text, item?.body, item?.message, item?.comment_text);
              const commentId = (item) => firstText(item?.id, item?.pk, item?.comment_id);
              const explicitParentId = (item, parentId) => firstText(
                item?.parent_comment_id,
                item?.parent_comment?.id,
                item?.parent?.id,
                item?.replied_to_comment_id,
                parentId
              );
              const mediaStatusId = (item) => {
                if (!item || typeof item !== 'object') return '';
                const shortcode = firstText(item.shortcode, item.code);
                if (!shortcode || !/^[A-Za-z0-9_-]{3,}$/.test(shortcode)) return '';
                const hasMediaSignal = Boolean(
                  item.display_url ||
                  item.thumbnail_src ||
                  item.thumbnail_url ||
                  item.edge_media_to_caption ||
                  item.caption ||
                  item.taken_at_timestamp ||
                  item.owner ||
                  item.user ||
                  item.is_video !== undefined ||
                  item.like_count !== undefined ||
                  item.comment_count !== undefined
                );
                return hasMediaSignal ? shortcode : '';
              };
              const candidateFromComment = (item, parentId) => {
                if (!item || typeof item !== 'object') return null;
                const text = commentText(item);
                const id = commentId(item);
                const username = ownerUsername(item);
                const createdAt = timestampValue(item.created_at || item.created_time || item.taken_at);
                if (!text || !id || !username) return null;
                return {
                  comment_id: id,
                  reply_to_comment_id: explicitParentId(item, parentId),
                  created_at: createdAt,
                  text,
                  raw_text: text,
                  author_name: ownerName(item),
                  author_username: username,
                  like_count: countValue(item.edge_liked_by) || countValue(item.like_count) || countValue(item.likes_count),
                };
              };
              const collectCommentCandidates = (root, output, parentId, seenObjects, skipRoot = false) => {
                if (!root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                const candidate = skipRoot ? null : candidateFromComment(root, parentId);
                const nextParentId = candidate ? candidate.comment_id : parentId;
                if (candidate) {
                  output.push(candidate);
                }
                if (Array.isArray(root)) {
                  for (const item of root) collectCommentCandidates(item, output, nextParentId, seenObjects);
                  return;
                }
                for (const value of Object.values(root)) {
                  collectCommentCandidates(value, output, nextParentId, seenObjects);
                }
              };
              const collectTargetCommentCandidates = (root, targetStatusId, output, seenObjects) => {
                if (!targetStatusId || !root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                if (mediaStatusId(root) === targetStatusId) {
                  collectCommentCandidates(root, output, '', new Set(), true);
                  return;
                }
                if (Array.isArray(root)) {
                  for (const item of root) collectTargetCommentCandidates(item, targetStatusId, output, seenObjects);
                  return;
                }
                for (const value of Object.values(root)) {
                  collectTargetCommentCandidates(value, targetStatusId, output, seenObjects);
                }
              };
              const dedupeComments = (comments) => {
                const byId = new Map();
                for (const comment of comments) {
                  if (!comment.comment_id || byId.has(comment.comment_id)) continue;
                  byId.set(comment.comment_id, comment);
                }
                return Array.from(byId.values());
              };
              const collectScriptComments = () => {
                const allComments = [];
                const targetComments = [];
                const targetStatusId = statusIdFromUrl(location.href);
                for (const script of Array.from(document.querySelectorAll('script[type="application/json"], script:not([src])'))) {
                  const raw = (script.textContent || '').trim();
                  if (!raw || (!raw.startsWith('{') && !raw.startsWith('['))) continue;
                  try {
                    const parsed = JSON.parse(raw);
                    collectCommentCandidates(parsed, allComments, '', new Set());
                    collectTargetCommentCandidates(parsed, targetStatusId, targetComments, new Set());
                  } catch (error) {}
                }
                const allScriptComments = dedupeComments(allComments);
                const targetScriptComments = dedupeComments(targetComments);
                return {
                  all_comments: allScriptComments,
                  target_comments: targetScriptComments,
                  selected_comments: targetScriptComments.length ? targetScriptComments : allScriptComments,
                };
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
              const bodyText = (document.body?.innerText || '').trim();
              const lowerBodyText = bodyText.toLowerCase();
              const scriptCommentSets = collectScriptComments();
              const scriptComments = scriptCommentSets.selected_comments;
              return {
                comments,
                script_comments: scriptComments,
                target_script_comments: scriptCommentSets.target_comments,
                shell_body_text: bodyText.slice(0, 12000),
                comment_extraction_sources: {
                  dom_comments: comments.length,
                  script_comments: scriptComments.length,
                  target_script_comments: scriptCommentSets.target_comments.length,
                  all_script_comments: scriptCommentSets.all_comments.length,
                },
                page_state: {
                  title: document.title || '',
                  url: location.href,
                  body_text_sample: bodyText.slice(0, 500),
                  login_wall_detected: /log in|sign up|log into instagram|create an account/.test(lowerBodyText),
                  serialized_comment_data_detected: scriptComments.length > 0,
                },
              };
            }
            """
        )
        return self._select_comments_for_post_payload(payload)

    def _select_comments_for_post_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        dom_comments = payload.get("comments") or []
        all_script_comments = payload.get("script_comments") or []
        target_script_comments = payload.get("target_script_comments") or []
        script_comments = target_script_comments if target_script_comments else all_script_comments
        script_comments_source = "target_media" if target_script_comments else "global"
        raw_shell_comments = payload.get("shell_text_comments")
        shell_text_comments = (
            raw_shell_comments
            if isinstance(raw_shell_comments, list)
            else self._extract_shell_text_comments(
                clean_text(payload.get("shell_body_text")),
                source_username=self._expected_source_username(),
            )
        )
        fallback_comments = (
            script_comments
            if dom_comments
            else self._merge_fallback_comment_candidates(script_comments, shell_text_comments)
        )
        merged_comments = self._merge_comment_candidates(dom_comments, fallback_comments)
        return {
            "comments": merged_comments,
            "script_comments": script_comments,
            "comment_extraction_sources": {
                "dom_comments": len(dom_comments),
                "script_comments": len(script_comments),
                "target_script_comments": len(target_script_comments),
                "all_script_comments": len(all_script_comments),
                "shell_text_comments": len(shell_text_comments),
                "fallback_comments": len(fallback_comments),
                "merged_comments": len(merged_comments),
                "script_comments_source": script_comments_source,
            },
            "page_state": payload.get("page_state") or {},
        }

    def _extract_session_diagnostic_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const textValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'string') return value.trim();
                return '';
              };
              const countValue = (value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'number') return String(value);
                if (typeof value === 'string') return value.trim();
                if (typeof value === 'object' && typeof value.count === 'number') return String(value.count);
                return '';
              };
              const firstText = (...values) => values.map(textValue).find(Boolean) || '';
              const statusIdFromUrl = (href) => {
                try {
                  const url = new URL(href, 'https://www.instagram.com');
                  const parts = url.pathname.split('/').filter(Boolean);
                  if (parts.length >= 2 && ['p', 'reel'].includes(parts[0])) return parts[1];
                } catch (error) {}
                return '';
              };
              const captionText = (item) => {
                const edgeCaption = item?.edge_media_to_caption?.edges?.[0]?.node?.text;
                const caption = item?.caption;
                return firstText(
                  edgeCaption,
                  caption?.text,
                  caption?.caption,
                  caption,
                  item?.accessibility_caption,
                  item?.title,
                  item?.text
                );
              };
              const ownerUsername = (item) => firstText(
                item?.owner?.username,
                item?.user?.username,
                item?.profile_grid_owner?.username,
                item?.author?.username,
                item?.from?.username
              );
              const commentText = (item) => firstText(item?.text, item?.body, item?.message, item?.comment_text);
              const commentId = (item) => firstText(item?.id, item?.pk, item?.comment_id);
              const explicitParentId = (item, parentId) => firstText(
                item?.parent_comment_id,
                item?.parent_comment?.id,
                item?.parent?.id,
                item?.replied_to_comment_id,
                parentId
              );
              const mediaCandidateFromItem = (item) => {
                if (!item || typeof item !== 'object') return null;
                const shortcode = firstText(item.shortcode, item.code);
                if (!shortcode || !/^[A-Za-z0-9_-]{3,}$/.test(shortcode)) return null;
                const hasMediaSignal = Boolean(
                  item.display_url ||
                  item.thumbnail_src ||
                  item.thumbnail_url ||
                  item.edge_media_to_caption ||
                  item.caption ||
                  item.taken_at_timestamp ||
                  item.owner ||
                  item.is_video !== undefined ||
                  item.like_count !== undefined ||
                  item.comment_count !== undefined
                );
                if (!hasMediaSignal) return null;
                const text = captionText(item);
                const permalinkKind = item.is_video || String(item.product_type || '').toLowerCase().includes('clips') ? 'reel' : 'p';
                return {
                  status_id: shortcode,
                  permalink: `https://www.instagram.com/${permalinkKind}/${shortcode}/`,
                  author_username: ownerUsername(item),
                  has_text: Boolean(text),
                  text_sample: text.slice(0, 160),
                  comment_count: countValue(item.edge_media_to_comment) || countValue(item.comment_count),
                  like_count: countValue(item.edge_liked_by) || countValue(item.edge_media_preview_like) || countValue(item.like_count),
                };
              };
              const commentCandidateFromItem = (item, parentId) => {
                if (!item || typeof item !== 'object') return null;
                const text = commentText(item);
                const id = commentId(item);
                const username = ownerUsername(item);
                if (!text || !id || !username) return null;
                return {
                  comment_id: id,
                  author_username: username,
                  reply_to_comment_id: explicitParentId(item, parentId),
                  has_text: Boolean(text),
                  text_sample: text.slice(0, 160),
                };
              };
              const collectMediaCandidates = (root, output, seenObjects) => {
                if (!root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                const candidate = mediaCandidateFromItem(root);
                if (candidate) output.push(candidate);
                if (Array.isArray(root)) {
                  for (const item of root) collectMediaCandidates(item, output, seenObjects);
                  return;
                }
                for (const value of Object.values(root)) {
                  collectMediaCandidates(value, output, seenObjects);
                }
              };
              const collectCommentCandidates = (root, output, parentId, seenObjects) => {
                if (!root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                const candidate = commentCandidateFromItem(root, parentId);
                const nextParentId = candidate ? candidate.comment_id : parentId;
                if (candidate) output.push(candidate);
                if (Array.isArray(root)) {
                  for (const item of root) collectCommentCandidates(item, output, nextParentId, seenObjects);
                  return;
                }
                for (const value of Object.values(root)) {
                  collectCommentCandidates(value, output, nextParentId, seenObjects);
                }
              };
              const dedupeByKey = (items, keyName) => {
                const byKey = new Map();
                for (const item of items) {
                  const key = item?.[keyName];
                  if (!key || byKey.has(key)) continue;
                  byKey.set(key, item);
                }
                return Array.from(byKey.values());
              };
              const valueType = (value) => {
                if (Array.isArray(value)) return 'array';
                if (value === null) return 'null';
                return typeof value;
              };
              const safeKey = (key) => {
                const value = String(key || '');
                if (/^[A-Za-z_$][A-Za-z0-9_$-]{0,63}$/.test(value)) return value;
                return '*';
              };
              const countMap = (map, key, amount = 1) => {
                map.set(key, (map.get(key) || 0) + amount);
              };
              const topCounts = (map, keyName, limit) => Array.from(map.entries())
                .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
                .slice(0, limit)
                .map(([key, count]) => ({ [keyName]: key, count }));
              const recordKeyPath = (stats, path, value) => {
                const current = stats.get(path) || { count: 0, valueTypes: new Set(), sampleKeys: new Set() };
                current.count += 1;
                current.valueTypes.add(valueType(value));
                if (value && typeof value === 'object' && !Array.isArray(value)) {
                  for (const key of Object.keys(value).slice(0, 12)) {
                    const cleaned = safeKey(key);
                    if (cleaned !== '*') current.sampleKeys.add(cleaned);
                  }
                }
                stats.set(path, current);
              };
              const shapeSample = (value, depth = 0, seenObjects = new Set()) => {
                const type = valueType(value);
                if (!value || typeof value !== 'object') return { type };
                if (seenObjects.has(value)) return { type, circular: true };
                seenObjects.add(value);
                if (Array.isArray(value)) {
                  const itemTypes = Array.from(new Set(value.slice(0, 20).map(valueType)));
                  const sample = depth < 2 && value.length ? shapeSample(value[0], depth + 1, seenObjects) : undefined;
                  const result = { type: 'array', length: value.length, item_types: itemTypes };
                  if (sample) result.sample = sample;
                  return result;
                }
                const keys = Object.keys(value).map(safeKey).filter((key) => key !== '*').slice(0, 16);
                const result = { type: 'object', keys };
                if (depth < 2) {
                  const children = {};
                  for (const key of Object.keys(value).slice(0, 8)) {
                    const cleaned = safeKey(key);
                    if (cleaned === '*') continue;
                    const child = value[key];
                    const childType = valueType(child);
                    children[cleaned] = Array.isArray(child)
                      ? { type: childType, length: child.length, item_types: Array.from(new Set(child.slice(0, 10).map(valueType))) }
                      : (child && typeof child === 'object' ? { type: childType, keys: Object.keys(child).map(safeKey).filter((item) => item !== '*').slice(0, 8) } : { type: childType });
                  }
                  result.children = children;
                }
                return result;
              };
              const collectSerializedStructure = (parsedScripts) => {
                const topLevelTypes = new Map();
                const topLevelKeys = new Map();
                const keyPathStats = new Map();
                const markerKeyStats = new Map();
                const markerKeys = new Set(['__bbox', '__typename', '__is', 'typename', 'type', 'operationName', 'module', 'props', 'data']);
                const shapeSamples = [];
                const walk = (value, path, depth, seenObjects, budget) => {
                  if (budget.count <= 0 || depth > 7 || !value || typeof value !== 'object' || seenObjects.has(value)) return;
                  seenObjects.add(value);
                  budget.count -= 1;
                  if (Array.isArray(value)) {
                    recordKeyPath(keyPathStats, `${path}[]`, value);
                    for (const item of value.slice(0, 80)) {
                      walk(item, `${path}[]`, depth + 1, seenObjects, budget);
                    }
                    return;
                  }
                  recordKeyPath(keyPathStats, path, value);
                  for (const [rawKey, child] of Object.entries(value)) {
                    const wildcardMapKey = /\\.(rsrcMap|gkxData|clpData|qplData|qexData|justknobxData)$/.test(path);
                    const key = wildcardMapKey ? '*' : safeKey(rawKey);
                    const childPath = key === '*' ? `${path}.*` : `${path}.${key}`;
                    recordKeyPath(keyPathStats, childPath, child);
                    if (markerKeys.has(rawKey)) {
                      const current = markerKeyStats.get(rawKey) || { count: 0, paths: new Set() };
                      current.count += 1;
                      current.paths.add(path);
                      markerKeyStats.set(rawKey, current);
                    }
                    walk(child, childPath, depth + 1, seenObjects, budget);
                  }
                };
                parsedScripts.forEach((parsed, index) => {
                  countMap(topLevelTypes, valueType(parsed));
                  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    for (const key of Object.keys(parsed).slice(0, 24)) {
                      const cleaned = safeKey(key);
                      if (cleaned !== '*') countMap(topLevelKeys, cleaned);
                    }
                  }
                  if (shapeSamples.length < 5) {
                    shapeSamples.push({
                      script_index: index,
                      root_type: valueType(parsed),
                      top_level_keys: parsed && typeof parsed === 'object' && !Array.isArray(parsed)
                        ? Object.keys(parsed).map(safeKey).filter((key) => key !== '*').slice(0, 16)
                        : [],
                      shape: shapeSample(parsed),
                    });
                  }
                  walk(parsed, '$', 0, new Set(), { count: 4000 });
                });
                const keyPaths = Array.from(keyPathStats.entries())
                  .sort((left, right) => right[1].count - left[1].count || left[0].localeCompare(right[0]))
                  .slice(0, 80)
                  .map(([path, item]) => ({
                    path,
                    count: item.count,
                    value_types: Array.from(item.valueTypes).sort(),
                    sample_keys: Array.from(item.sampleKeys).sort().slice(0, 12),
                  }));
                const markers = Array.from(markerKeyStats.entries())
                  .sort((left, right) => right[1].count - left[1].count || left[0].localeCompare(right[0]))
                  .slice(0, 40)
                  .map(([key, item]) => ({ key, count: item.count, paths: Array.from(item.paths).sort().slice(0, 12) }));
                return {
                  top_level_types: topCounts(topLevelTypes, 'type', 20),
                  top_level_keys: topCounts(topLevelKeys, 'key', 40),
                  key_paths: keyPaths,
                  marker_keys: markers,
                  shape_samples: shapeSamples,
                };
              };
              const bodyText = (document.body?.innerText || '').trim();
              const lowerBodyText = bodyText.toLowerCase();
              const jsonScripts = Array.from(document.querySelectorAll('script[type="application/json"], script:not([src])'))
                .map((script) => (script.textContent || '').trim())
                .filter((text) => text && (text.startsWith('{') || text.startsWith('[')));
              const postLinks = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'));
              const mediaCandidates = [];
              const commentCandidates = [];
              const parsedScripts = [];
              let parseErrors = 0;
              for (const rawJson of jsonScripts) {
                try {
                  const parsed = JSON.parse(rawJson);
                  parsedScripts.push(parsed);
                  collectMediaCandidates(parsed, mediaCandidates, new Set());
                  collectCommentCandidates(parsed, commentCandidates, '', new Set());
                } catch (error) {
                  parseErrors += 1;
                }
              }
              const uniqueMediaCandidates = dedupeByKey(mediaCandidates, 'status_id');
              const uniqueCommentCandidates = dedupeByKey(commentCandidates, 'comment_id');
              const targetStatusId = statusIdFromUrl(location.href);
              const targetMediaCandidates = targetStatusId
                ? uniqueMediaCandidates.filter((item) => item.status_id === targetStatusId)
                : [];
              const otherMediaCandidates = targetStatusId
                ? uniqueMediaCandidates.filter((item) => item.status_id !== targetStatusId)
                : uniqueMediaCandidates;
              const serializedStructure = collectSerializedStructure(parsedScripts);
              return {
                final_url: location.href,
                page_state: {
                  title: document.title || '',
                  url: location.href,
                  login_wall_detected: /log in|sign up|log into instagram|create an account/.test(lowerBodyText),
                  profile_unavailable_detected: /sorry, this page isn't available|page isn't available|content isn't available/.test(lowerBodyText),
                  serialized_data_detected: jsonScripts.length > 0,
                  body_text_length: bodyText.length,
                },
                extraction_sources: {
                  post_links: postLinks.length,
                  json_script_blocks: jsonScripts.length,
                  media_candidates: uniqueMediaCandidates.length,
                  comment_candidates: uniqueCommentCandidates.length,
                  target_media_candidates: targetMediaCandidates.length,
                  other_media_candidates: otherMediaCandidates.length,
                },
                serialized_candidates: {
                  media: uniqueMediaCandidates.slice(0, 5),
                  target_media: targetMediaCandidates.slice(0, 5),
                  other_media: otherMediaCandidates.slice(0, 5),
                  comments: uniqueCommentCandidates.slice(0, 5),
                },
                serialized_structure: {
                  scripts_analyzed: parsedScripts.length,
                  parse_errors: parseErrors,
                  top_level_types: serializedStructure.top_level_types,
                  top_level_keys: serializedStructure.top_level_keys,
                  key_paths: serializedStructure.key_paths,
                  marker_keys: serializedStructure.marker_keys,
                  shape_samples: serializedStructure.shape_samples,
                },
                body_sample: bodyText.slice(0, 500),
              };
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
    def _merge_profile_post_candidates(
        cls,
        primary_posts: list[dict[str, Any]],
        fallback_posts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for candidate in primary_posts:
            normalized = cls._normalize_post_payload_item(candidate)
            status_id = str(normalized.get("status_id") or "").strip()
            if not status_id:
                continue
            merged[status_id] = normalized
            order.append(status_id)
        for candidate in fallback_posts:
            normalized = cls._normalize_post_payload_item(candidate)
            status_id = str(normalized.get("status_id") or "").strip()
            if not status_id:
                continue
            existing = merged.get(status_id)
            if existing is None:
                merged[status_id] = normalized
                order.append(status_id)
                continue
            if cls._profile_post_score(normalized) > cls._profile_post_score(existing):
                merged[status_id] = normalized
        return [merged[status_id] for status_id in order if status_id in merged]

    @staticmethod
    def _profile_post_score(item: dict[str, Any]) -> int:
        score = 0
        if item.get("text"):
            score += 4
        if item.get("raw_text"):
            score += 3
        if item.get("created_at"):
            score += 2
        if item.get("permalink"):
            score += 2
        if item.get("author_username"):
            score += 1
        if item.get("comment_count"):
            score += 1
        if item.get("like_count"):
            score += 1
        return score

    def _profile_payload_warnings(self, payload: dict[str, Any], *, source_id: str) -> list[str]:
        warnings: list[str] = []
        page_state = payload.get("page_state") or {}
        posts = payload.get("posts") or []
        if page_state.get("login_wall_detected"):
            if self._uses_authenticated_browser():
                warnings.append(
                    "Authenticated browser mode is enabled, but Instagram still returned login/signup UI for the profile feed. "
                    "The selected browser profile may not be logged in to Instagram."
                )
            else:
                warnings.append(
                    "Instagram public web returned login/signup UI for the profile feed; enable authenticated_browser to scan this surface with a logged-in browser profile."
                )
        if page_state.get("profile_unavailable_detected"):
            warnings.append(f"Instagram profile surface for {source_id} appears unavailable or inaccessible in the current web UI.")
        if not posts:
            extraction_sources = payload.get("extraction_sources") or {}
            warnings.append(
                "Instagram web profile feed exposed no post candidates for "
                f"{source_id}; extraction counts: dom_posts={extraction_sources.get('dom_posts', 0)}, "
                f"script_posts={extraction_sources.get('script_posts', 0)}."
            )
        return warnings

    def _post_payload_warnings(self, payload: dict[str, Any], *, post: PostSnapshot) -> list[str]:
        warnings: list[str] = []
        page_state = payload.get("page_state") or {}
        comments = payload.get("comments") or []
        if page_state.get("login_wall_detected"):
            if self._uses_authenticated_browser():
                warnings.append(
                    f"Authenticated browser mode is enabled, but Instagram still returned login/signup UI for detail page {post.post_id}. "
                    "The selected browser profile may not be logged in to Instagram."
                )
            else:
                warnings.append(
                    f"Instagram public web returned login/signup UI for detail page {post.post_id}; enable authenticated_browser to scan comments with a logged-in browser profile."
                )
        if post.comments_count > 0 and not comments:
            extraction_sources = payload.get("comment_extraction_sources") or {}
            warnings.append(
                f"Instagram web detail page for {post.post_id} exposed comment counter {post.comments_count}, "
                "but no comments were visible in DOM or serialized page data; "
                f"extraction counts: dom_comments={extraction_sources.get('dom_comments', 0)}, "
                f"script_comments={extraction_sources.get('script_comments', 0)}."
            )
        return warnings

    def _session_diagnostic_status(self, payload: dict[str, Any]) -> str:
        page_state = self._normalize_session_page_state(payload.get("page_state") or {})
        extraction_sources = payload.get("extraction_sources") or {}
        if page_state["login_wall_detected"]:
            return "login_wall"
        if page_state["profile_unavailable_detected"]:
            return "profile_unavailable"
        if (
            page_state["serialized_data_detected"]
            or self._int_value(extraction_sources.get("post_links")) > 0
            or page_state["body_text_length"] > 0
        ):
            return "content_visible"
        return "empty_dom"

    def _session_diagnostic_warnings(self, status: str) -> list[str]:
        if status == "login_wall":
            if self._uses_authenticated_browser():
                return [
                    "Authenticated browser mode is enabled, but Instagram still returned login/signup UI. "
                    "The selected browser profile may not be logged in to Instagram."
                ]
            return [
                "Instagram public web returned login/signup UI; enable authenticated_browser with a logged-in browser profile."
            ]
        if status == "profile_unavailable":
            return ["Instagram profile surface appears unavailable or inaccessible in the current web UI."]
        if status == "empty_dom":
            return ["Instagram web session diagnostic loaded an empty DOM with no visible profile or serialized data signals."]
        return []

    @staticmethod
    def _target_author_username(serialized_candidates: dict[str, list[dict[str, Any]]]) -> str:
        target_media = serialized_candidates.get("target_media") or []
        if not target_media:
            return ""
        return clean_text(target_media[0].get("author_username"))

    def _target_author_warnings(self, target_author_username: str) -> list[str]:
        expected_username = self._expected_source_username()
        if not target_author_username or not expected_username or target_author_username == expected_username:
            return []
        return [
            "Instagram detail target media author "
            f"{target_author_username} does not match configured Instagram source {expected_username}; "
            "the target URL may not belong to the requested profile."
        ]

    def _expected_source_username(self) -> str:
        for value in (self.config.source.url, self.config.source.source_id, self.config.source.source_name):
            username = instagram_username_from_reference(value)
            if username:
                return username
        return ""

    @classmethod
    def _normalize_session_page_state(cls, page_state: dict[str, Any]) -> dict[str, Any]:
        return {
            "login_wall_detected": bool(page_state.get("login_wall_detected")),
            "profile_unavailable_detected": bool(page_state.get("profile_unavailable_detected")),
            "serialized_data_detected": bool(page_state.get("serialized_data_detected")),
            "body_text_length": cls._int_value(page_state.get("body_text_length")),
        }

    @classmethod
    def _normalize_serialized_candidate_samples(
        cls,
        candidates: dict[str, Any],
        *,
        target_status_id: str,
    ) -> dict[str, list[dict[str, Any]]]:
        raw_media = candidates.get("media") if isinstance(candidates, dict) else []
        raw_target_media = candidates.get("target_media") if isinstance(candidates, dict) else []
        raw_other_media = candidates.get("other_media") if isinstance(candidates, dict) else []
        raw_comments = candidates.get("comments") if isinstance(candidates, dict) else []
        media = raw_media if isinstance(raw_media, list) else []
        target_media = raw_target_media if isinstance(raw_target_media, list) else []
        other_media = raw_other_media if isinstance(raw_other_media, list) else []
        comments = raw_comments if isinstance(raw_comments, list) else []
        normalized_media = [cls._normalize_media_candidate_sample(item) for item in media[:5] if isinstance(item, dict)]
        normalized_target_media = [
            cls._normalize_media_candidate_sample(item) for item in target_media[:5] if isinstance(item, dict)
        ]
        normalized_other_media = [
            cls._normalize_media_candidate_sample(item) for item in other_media[:5] if isinstance(item, dict)
        ]
        if not normalized_target_media and target_status_id:
            normalized_target_media = [
                item for item in normalized_media if item.get("status_id") == target_status_id
            ][:5]
        if not normalized_other_media:
            normalized_other_media = [
                item for item in normalized_media if not target_status_id or item.get("status_id") != target_status_id
            ][:5]
        return {
            "media": normalized_media,
            "target_media": normalized_target_media,
            "other_media": normalized_other_media,
            "comments": [
                {
                    "comment_id": clean_text(item.get("comment_id")),
                    "author_username": instagram_username_from_reference(item.get("author_username")) or "",
                    "reply_to_comment_id": clean_text(item.get("reply_to_comment_id")),
                    "has_text": bool(item.get("has_text")),
                    "text_sample": clean_text(item.get("text_sample"))[:160],
                }
                for item in comments[:5]
                if isinstance(item, dict)
            ],
        }

    @staticmethod
    def _normalize_media_candidate_sample(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "status_id": clean_text(item.get("status_id")),
            "permalink": canonical_instagram_permalink(item.get("permalink")),
            "author_username": instagram_username_from_reference(item.get("author_username")) or "",
            "has_text": bool(item.get("has_text")),
            "text_sample": clean_text(item.get("text_sample"))[:160],
            "comment_count": clean_text(item.get("comment_count")),
            "like_count": clean_text(item.get("like_count")),
        }

    @classmethod
    def _normalize_serialized_structure(cls, structure: dict[str, Any]) -> dict[str, Any]:
        return {
            "scripts_analyzed": cls._int_value(structure.get("scripts_analyzed")),
            "parse_errors": cls._int_value(structure.get("parse_errors")),
            "top_level_types": cls._normalize_count_rows(structure.get("top_level_types"), key_name="type", limit=20),
            "top_level_keys": cls._normalize_count_rows(structure.get("top_level_keys"), key_name="key", limit=40),
            "key_paths": cls._normalize_key_path_rows(structure.get("key_paths")),
            "marker_keys": cls._normalize_marker_key_rows(structure.get("marker_keys")),
            "shape_samples": cls._normalize_shape_samples(structure.get("shape_samples")),
        }

    @classmethod
    def _normalize_count_rows(cls, rows: Any, *, key_name: str, limit: int) -> list[dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows[:limit]:
            if not isinstance(item, dict):
                continue
            key_value = clean_text(item.get(key_name))
            if not key_value:
                continue
            normalized.append({key_name: key_value, "count": cls._int_value(item.get("count"))})
        return normalized

    @classmethod
    def _normalize_key_path_rows(cls, rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows[:80]:
            if not isinstance(item, dict):
                continue
            path = clean_text(item.get("path"))
            if not path:
                continue
            normalized.append(
                {
                    "path": path[:240],
                    "count": cls._int_value(item.get("count")),
                    "value_types": cls._string_list(item.get("value_types"), limit=12),
                    "sample_keys": cls._string_list(item.get("sample_keys"), limit=12),
                }
            )
        return normalized

    @classmethod
    def _normalize_marker_key_rows(cls, rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows[:40]:
            if not isinstance(item, dict):
                continue
            key = clean_text(item.get("key"))
            if not key:
                continue
            normalized.append(
                {
                    "key": key[:80],
                    "count": cls._int_value(item.get("count")),
                    "paths": cls._string_list(item.get("paths"), limit=12, max_length=240),
                }
            )
        return normalized

    @classmethod
    def _normalize_shape_samples(cls, rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows[:5]:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "script_index": cls._int_value(item.get("script_index")),
                    "root_type": clean_text(item.get("root_type"))[:40],
                    "top_level_keys": cls._string_list(item.get("top_level_keys"), limit=16, max_length=80),
                    "shape": cls._redact_shape_sample(item.get("shape"), depth=0),
                }
            )
        return normalized

    @classmethod
    def _redact_shape_sample(cls, value: Any, *, depth: int) -> dict[str, Any]:
        if not isinstance(value, dict) or depth > 3:
            return {"type": "unknown"}
        result: dict[str, Any] = {"type": clean_text(value.get("type"))[:40] or "unknown"}
        if "length" in value:
            result["length"] = cls._int_value(value.get("length"))
        if "circular" in value:
            result["circular"] = bool(value.get("circular"))
        if "item_types" in value:
            result["item_types"] = cls._string_list(value.get("item_types"), limit=12, max_length=40)
        if "keys" in value:
            result["keys"] = cls._string_list(value.get("keys"), limit=16, max_length=80)
        child_sample = value.get("sample")
        if isinstance(child_sample, dict):
            result["sample"] = cls._redact_shape_sample(child_sample, depth=depth + 1)
        raw_children = value.get("children")
        if isinstance(raw_children, dict) and depth < 2:
            children: dict[str, Any] = {}
            for key, child in list(raw_children.items())[:8]:
                child_key = clean_text(key)[:80]
                if child_key and isinstance(child, dict):
                    children[child_key] = cls._redact_shape_sample(child, depth=depth + 1)
            result["children"] = children
        return result

    @staticmethod
    def _string_list(value: Any, *, limit: int, max_length: int = 80) -> list[str]:
        if not isinstance(value, list):
            return []
        output: list[str] = []
        for item in value[:limit]:
            text = clean_text(item)[:max_length]
            if text:
                output.append(text)
        return output

    @staticmethod
    def _int_value(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

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
    def _merge_comment_candidates(
        cls,
        primary_comments: list[dict[str, Any]],
        fallback_comments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for index, candidate in enumerate(primary_comments):
            normalized = cls._normalize_comment_payload_item(candidate, index=index)
            if normalized is None:
                continue
            comment_id = str(normalized.get("comment_id") or "").strip()
            if not comment_id:
                continue
            merged[comment_id] = normalized
            order.append(comment_id)
        for index, candidate in enumerate(fallback_comments, start=len(primary_comments)):
            normalized = cls._normalize_comment_payload_item(candidate, index=index)
            if normalized is None:
                continue
            comment_id = str(normalized.get("comment_id") or "").strip()
            if not comment_id:
                continue
            existing = merged.get(comment_id)
            if existing is None:
                merged[comment_id] = normalized
                order.append(comment_id)
                continue
            if cls._comment_candidate_score(normalized) > cls._comment_candidate_score(existing):
                merged[comment_id] = normalized
        return [merged[comment_id] for comment_id in order if comment_id in merged]

    @classmethod
    def _merge_fallback_comment_candidates(
        cls,
        script_comments: list[dict[str, Any]],
        shell_text_comments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        seen_signatures: set[str] = set()
        index = 0
        for group in (script_comments, shell_text_comments):
            for candidate in group:
                normalized = cls._normalize_comment_payload_item(candidate, index=index)
                index += 1
                if normalized is None:
                    continue
                comment_id = str(normalized.get("comment_id") or "").strip()
                signature = cls._comment_signature(normalized)
                if (comment_id and comment_id in seen_ids) or (signature and signature in seen_signatures):
                    continue
                merged.append(normalized)
                if comment_id:
                    seen_ids.add(comment_id)
                if signature:
                    seen_signatures.add(signature)
        return merged

    @staticmethod
    def _comment_candidate_score(item: dict[str, Any]) -> int:
        score = 0
        if item.get("text"):
            score += 4
        if item.get("raw_text"):
            score += 3
        if item.get("author_username"):
            score += 2
        if item.get("created_at"):
            score += 2
        if item.get("reply_to_comment_id"):
            score += 1
        if item.get("like_count"):
            score += 1
        return score

    @classmethod
    def _extract_shell_text_comments(cls, body_text: str, *, source_username: str) -> list[dict[str, Any]]:
        lines = cls._shell_text_lines(body_text)
        source = instagram_username_from_reference(source_username) or ""
        comments: list[dict[str, Any]] = []
        index = 0
        while index < len(lines):
            author_username = instagram_username_from_reference(lines[index])
            if not author_username:
                index += 1
                continue
            time_index = cls._find_shell_time_index(lines, index)
            if time_index < 0:
                index += 1
                continue
            next_index = time_index + 1
            text_lines: list[str] = []
            while next_index < len(lines):
                if instagram_username_from_reference(lines[next_index]) and cls._find_shell_time_index(lines, next_index) > -1:
                    break
                if not cls._is_shell_comment_noise(lines[next_index]):
                    text_lines.append(lines[next_index])
                next_index += 1
            text = clean_text(" ".join(text_lines))
            is_initial_source_caption = bool(source and author_username == source and not comments)
            if text and not is_initial_source_caption:
                comments.append(
                    {
                        "comment_id": f"shell:{stable_id(author_username, lines[time_index], text)}",
                        "reply_to_comment_id": "",
                        "created_at": None,
                        "text": text,
                        "raw_text": "\n".join([author_username, lines[time_index], *text_lines]),
                        "author_name": author_username,
                        "author_username": author_username,
                        "like_count": "",
                    }
                )
            index = max(next_index, index + 1)
        return comments

    @classmethod
    def _find_shell_time_index(cls, lines: list[str], author_index: int) -> int:
        max_index = min(len(lines), author_index + 6)
        for index in range(author_index + 1, max_index):
            if cls._is_shell_time_line(lines[index]):
                return index
            if instagram_username_from_reference(lines[index]) and not cls._is_shell_comment_noise(lines[index]):
                return -1
        return -1

    @staticmethod
    def _shell_text_lines(body_text: str) -> list[str]:
        return [line.strip() for line in body_text.replace("\xa0", "\n").splitlines() if line.strip()]

    @classmethod
    def _comment_signature(cls, item: dict[str, Any]) -> str:
        author_username = instagram_username_from_reference(item.get("author_username")) or ""
        text = clean_text(item.get("text")).casefold()
        return f"{author_username}\0{text}" if author_username and text else ""

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

    @classmethod
    def _is_shell_comment_noise(cls, value: str) -> bool:
        lowered = value.strip().casefold()
        if lowered in {
            "like",
            "reply",
            "follow",
            "log in",
            "sign up",
            "see translation",
            "view reply",
            "view replies",
        }:
            return True
        if lowered.startswith("never miss a post from ") or lowered.startswith("sign up for instagram"):
            return True
        return cls._is_shell_time_line(value)

    @staticmethod
    def _is_shell_time_line(value: str) -> bool:
        lowered = value.strip().casefold()
        return bool(re.fullmatch(r"\d+\s*(s|m|h|d|w|y)", lowered))

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
            best_effort_profile_copy=True,
        )

    def _uses_authenticated_browser(self) -> bool:
        return self.settings.authenticated_browser.enabled

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
