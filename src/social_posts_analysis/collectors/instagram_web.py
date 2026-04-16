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
              const collectCommentCandidates = (root, output, parentId, seenObjects) => {
                if (!root || typeof root !== 'object' || seenObjects.has(root)) return;
                seenObjects.add(root);
                const candidate = candidateFromComment(root, parentId);
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
              const collectScriptComments = () => {
                const comments = [];
                for (const script of Array.from(document.querySelectorAll('script[type="application/json"], script:not([src])'))) {
                  const raw = (script.textContent || '').trim();
                  if (!raw || (!raw.startsWith('{') && !raw.startsWith('['))) continue;
                  try {
                    const parsed = JSON.parse(raw);
                    collectCommentCandidates(parsed, comments, '', new Set());
                  } catch (error) {}
                }
                const byId = new Map();
                for (const comment of comments) {
                  if (!comment.comment_id || byId.has(comment.comment_id)) continue;
                  byId.set(comment.comment_id, comment);
                }
                return Array.from(byId.values());
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
              const scriptComments = collectScriptComments();
              return {
                comments,
                script_comments: scriptComments,
                comment_extraction_sources: {
                  dom_comments: comments.length,
                  script_comments: scriptComments.length,
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
        dom_comments = payload.get("comments") or []
        script_comments = payload.get("script_comments") or []
        merged_comments = self._merge_comment_candidates(dom_comments, script_comments)
        return {
            "comments": merged_comments,
            "script_comments": script_comments,
            "comment_extraction_sources": {
                "dom_comments": len(dom_comments),
                "script_comments": len(script_comments),
                "merged_comments": len(merged_comments),
            },
            "page_state": payload.get("page_state") or {},
        }

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
