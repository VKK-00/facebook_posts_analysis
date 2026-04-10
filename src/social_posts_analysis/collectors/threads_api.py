from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import slugify, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError


class ThreadsApiCollector(BaseCollector):
    name = "threads_api"
    PROFILE_FIELDS = "id,username,name,threads_profile_picture_url,threads_biography,is_verified"
    MEDIA_FIELDS = (
        "id,media_product_type,media_type,media_url,permalink,owner,username,text,timestamp,"
        "shortcode,thumbnail_url,children,is_quote_post,quoted_post,reposted_post,has_replies"
    )
    REPLY_FIELDS = (
        "id,text,username,permalink,timestamp,media_product_type,media_type,media_url,shortcode,"
        "thumbnail_url,children,is_quote_post,quoted_post,has_replies,root_post,replied_to,is_reply"
    )

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.threads_api
        if not self.settings.enabled:
            raise CollectorUnavailableError("Threads API collector is disabled in config.collector.threads_api.enabled.")
        if not self.settings.access_token:
            raise CollectorUnavailableError("Threads API collector requires THREADS_ACCESS_TOKEN or collector.threads_api.access_token.")
        self.client = httpx.Client(timeout=self.settings.timeout_seconds)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        source_payload = self._resolve_source()
        source_data = source_payload.get("data") or source_payload
        source_id = str(source_data.get("id") or self.config.source.source_id or self._source_reference())
        source_path = raw_store.write_json("threads_source", "source_metadata", source_payload)

        source_snapshot = SourceSnapshot(
            platform="threads",
            source_id=source_id,
            source_name=source_data.get("name") or source_data.get("username") or self.config.source.source_name,
            source_url=self._source_url(source_data),
            source_type="account",
            about=source_data.get("threads_biography"),
            source_collector=self.name,
            raw_path=str(source_path),
        )

        posts: list[PostSnapshot] = []
        next_cursor = ""
        warnings: list[str] = []
        for page_index, payload in enumerate(self._iter_user_threads_pages(source_id), start=1):
            raw_store.write_json("threads_media_pages", f"media-page-{page_index}", payload)
            if not next_cursor:
                next_cursor = str(((payload.get("paging") or {}).get("cursors") or {}).get("after") or "")
            for item in payload.get("data") or []:
                if not self._within_range(item.get("timestamp")):
                    continue
                post = self._build_post_snapshot(item=item, source_snapshot=source_snapshot, raw_store=raw_store)
                replies = self._collect_replies(post_snapshot=post, raw_store=raw_store)
                posts.append(post.model_copy(update={"comments": replies, "comments_count": max(post.comments_count, len(replies))}))

        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=warnings,
            cursors={"after": next_cursor} if next_cursor else {},
            source=source_snapshot,
            posts=posts,
        )

    def _resolve_source(self) -> dict[str, Any]:
        if self.config.source.source_id:
            return self._get_json(f"/{self.config.source.source_id}", params={"fields": self.PROFILE_FIELDS})
        return self._get_json("/profile_lookup", params={"username": self._source_reference(), "fields": self.PROFILE_FIELDS})

    def _iter_user_threads_pages(self, source_id: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"fields": self.MEDIA_FIELDS, "limit": min(max(10, self.settings.page_size), 100)}
        pages: list[dict[str, Any]] = []
        next_cursor: str | None = None
        while True:
            current_params = {**params, **({"after": next_cursor} if next_cursor else {})}
            payload = self._get_json(f"/{source_id}/threads", params=current_params)
            pages.append(payload)
            next_cursor = str(((payload.get("paging") or {}).get("cursors") or {}).get("after") or "") or None
            if not next_cursor:
                break
        return pages

    def _collect_replies(self, *, post_snapshot: PostSnapshot, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        media_id = self._native_media_id(post_snapshot.post_id)
        payload = self._get_json(f"/{media_id}/conversation", params={"fields": self.REPLY_FIELDS, "reverse": "false"})
        raw_store.write_json("threads_replies", slugify(media_id), payload)
        reply_map: dict[str, str] = {}
        depth_map: dict[str, int] = {}
        comments: list[CommentSnapshot] = []
        for item in payload.get("data") or []:
            if not self._within_range(item.get("timestamp")):
                continue
            comment_id = f"{post_snapshot.post_id}:comment:{item['id']}"
            parent_native_id = str(item.get("replied_to") or "")
            parent_comment_id = reply_map.get(parent_native_id) if parent_native_id and parent_native_id != media_id else None
            depth = depth_map.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
            raw_path = raw_store.write_json("threads_reply_items", slugify(comment_id), item)
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="threads",
                parent_post_id=post_snapshot.post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=parent_native_id or media_id,
                thread_root_post_id=post_snapshot.post_id,
                created_at=item.get("timestamp"),
                message=item.get("text"),
                permalink=item.get("permalink"),
                reactions=0,
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("username"),
                    name=item.get("username"),
                    profile_url=f"https://www.threads.net/@{item.get('username')}" if item.get("username") else None,
                ),
            )
            comments.append(snapshot)
            reply_map[str(item["id"])] = snapshot.comment_id
            depth_map[snapshot.comment_id] = snapshot.depth
        return comments

    def _build_post_snapshot(
        self,
        *,
        item: dict[str, Any],
        source_snapshot: SourceSnapshot,
        raw_store: RawSnapshotStore,
    ) -> PostSnapshot:
        media_id = str(item["id"])
        post_id = f"threads:{source_snapshot.source_id}:{media_id}"
        propagation_kind = "quote" if item.get("is_quote_post") else "repost" if item.get("reposted_post") else None
        origin_external_id = str(item.get("quoted_post") or item.get("reposted_post") or "") or None
        raw_path = raw_store.write_json("threads_posts", slugify(post_id), item)
        media_type = str(item.get("media_type") or "") or None
        media_url = item.get("media_url")
        return PostSnapshot(
            post_id=post_id,
            platform="threads",
            source_id=source_snapshot.source_id,
            origin_post_id=f"threads:origin:{origin_external_id}" if origin_external_id else None,
            origin_external_id=origin_external_id,
            origin_permalink=f"https://www.threads.net/t/{origin_external_id}" if origin_external_id else None,
            propagation_kind=propagation_kind,
            is_propagation=propagation_kind is not None,
            created_at=item.get("timestamp"),
            message=item.get("text"),
            permalink=item.get("permalink"),
            reactions=0,
            shares=1 if propagation_kind == "repost" else 0,
            comments_count=1 if item.get("has_replies") else 0,
            has_media=bool(media_type or media_url),
            media_type=media_type,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=AuthorSnapshot(
                author_id=source_snapshot.source_id,
                name=source_snapshot.source_name,
                profile_url=source_snapshot.source_url,
            ),
        )

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name.lstrip("@")
        if self.config.source.url:
            return self.config.source.url.rstrip("/").split("@")[-1].split("/")[-1]
        raise CollectorUnavailableError("Threads API collector requires source.source_name, source.source_id, or source.url.")

    @staticmethod
    def _source_url(source_data: dict[str, Any]) -> str | None:
        username = source_data.get("username")
        if not username:
            return None
        return f"https://www.threads.net/@{username}"

    @staticmethod
    def _native_media_id(post_id: str) -> str:
        return post_id.split(":")[-1]

    def _within_range(self, raw_value: object) -> bool:
        if raw_value is None:
            return False
        current = self._parse_datetime(str(raw_value))
        if current is None:
            return False
        start = self._parse_datetime(self.config.date_range.start, end_of_day=False)
        end = self._parse_datetime(self.config.date_range.end, end_of_day=True)
        if start and current < start:
            return False
        if end and current > end:
            return False
        return True

    @staticmethod
    def _parse_datetime(raw_value: str | None, *, end_of_day: bool = False) -> datetime | None:
        if not raw_value:
            return None
        try:
            if "T" in raw_value:
                parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            else:
                parsed = datetime.fromisoformat(
                    f"{raw_value}T23:59:59+00:00" if end_of_day else f"{raw_value}T00:00:00+00:00"
                )
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.client.get(
            f"{self.settings.base_url.rstrip('/')}{endpoint}",
            params={**(params or {}), "access_token": self.settings.access_token},
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise CollectorUnavailableError(str(payload["error"]))
        return payload
