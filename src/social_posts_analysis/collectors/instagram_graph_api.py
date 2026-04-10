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
    MediaReference,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import slugify, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError


class InstagramGraphApiCollector(BaseCollector):
    name = "instagram_graph_api"
    USER_FIELDS = "id,username,name,biography,followers_count,follows_count,media_count,profile_picture_url"
    MEDIA_FIELDS = (
        "id,caption,media_type,media_product_type,media_url,permalink,timestamp,comments_count,"
        "like_count,thumbnail_url,children{media_type,media_url,thumbnail_url,permalink}"
    )
    COMMENT_FIELDS = "id,text,timestamp,username,like_count,replies{id,text,timestamp,username,like_count,parent_id},parent_id"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.instagram_graph_api
        if not self.settings.enabled:
            raise CollectorUnavailableError(
                "Instagram Graph API collector is disabled in config.collector.instagram_graph_api.enabled."
            )
        if not self.settings.access_token:
            raise CollectorUnavailableError(
                "Instagram Graph API collector requires INSTAGRAM_ACCESS_TOKEN or collector.instagram_graph_api.access_token."
            )
        self.client = httpx.Client(timeout=self.settings.timeout_seconds)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        source_id = str(self.config.source.source_id)
        source_payload = self._get_json(
            f"/{self.settings.api_version}/{source_id}",
            params={"fields": self.USER_FIELDS},
        )
        source_path = raw_store.write_json("instagram_source", "source_metadata", source_payload)
        source_snapshot = SourceSnapshot(
            platform="instagram",
            source_id=source_id,
            source_name=source_payload.get("name") or source_payload.get("username") or self.config.source.source_name,
            source_url=f"https://www.instagram.com/{source_payload.get('username')}/" if source_payload.get("username") else self.config.source.url,
            source_type="account",
            about=source_payload.get("biography"),
            followers_count=self._safe_int(source_payload.get("followers_count")),
            source_collector=self.name,
            raw_path=str(source_path),
        )

        posts: list[PostSnapshot] = []
        next_after = ""
        for page_index, payload in enumerate(self._iter_media_pages(source_id), start=1):
            raw_store.write_json("instagram_media_pages", f"media-page-{page_index}", payload)
            if not next_after:
                next_after = str((((payload.get("paging") or {}).get("cursors") or {}).get("after")) or "")
            for media in payload.get("data") or []:
                if not self._within_range(media.get("timestamp")):
                    continue
                posts.append(self._collect_post(media=media, source_snapshot=source_snapshot, raw_store=raw_store))

        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="success",
            warnings=[],
            cursors={"after": next_after} if next_after else {},
            source=source_snapshot,
            posts=posts,
        )

    def _iter_media_pages(self, source_id: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"fields": self.MEDIA_FIELDS, "limit": min(max(10, self.settings.page_size), 100)}
        pages: list[dict[str, Any]] = []
        next_after: str | None = None
        while True:
            current_params = {**params, **({"after": next_after} if next_after else {})}
            payload = self._get_json(f"/{self.settings.api_version}/{source_id}/media", params=current_params)
            pages.append(payload)
            next_after = str((((payload.get("paging") or {}).get("cursors") or {}).get("after")) or "") or None
            if not next_after:
                break
        return pages

    def _collect_post(self, *, media: dict[str, Any], source_snapshot: SourceSnapshot, raw_store: RawSnapshotStore) -> PostSnapshot:
        media_id = str(media["id"])
        post_id = f"instagram:{source_snapshot.source_id}:{media_id}"
        raw_path = raw_store.write_json("instagram_posts", slugify(post_id), media)
        comments = self._collect_comments(media_id=media_id, parent_post_id=post_id, raw_store=raw_store)
        media_refs = self._extract_media_refs(post_id, media)
        return PostSnapshot(
            post_id=post_id,
            platform="instagram",
            source_id=source_snapshot.source_id,
            created_at=media.get("timestamp"),
            message=media.get("caption"),
            permalink=media.get("permalink"),
            reactions=self._safe_int(media.get("like_count")) or 0,
            shares=0,
            comments_count=max(self._safe_int(media.get("comments_count")) or 0, len(comments)),
            has_media=bool(media.get("media_type")),
            media_type=str(media.get("media_type") or "") or None,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=AuthorSnapshot(
                author_id=source_snapshot.source_id,
                name=source_snapshot.source_name,
                profile_url=source_snapshot.source_url,
            ),
            media_refs=media_refs,
            comments=comments,
        )

    def _collect_comments(self, *, media_id: str, parent_post_id: str, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        payload = self._get_json(f"/{self.settings.api_version}/{media_id}/comments", params={"fields": self.COMMENT_FIELDS})
        raw_store.write_json("instagram_comment_pages", slugify(parent_post_id), payload)
        comments: list[CommentSnapshot] = []
        for item in payload.get("data") or []:
            comment_id = f"{parent_post_id}:comment:{item['id']}"
            raw_path = raw_store.write_json("instagram_comment_items", slugify(comment_id), item)
            top_level = CommentSnapshot(
                comment_id=comment_id,
                platform="instagram",
                parent_post_id=parent_post_id,
                parent_comment_id=None,
                thread_root_post_id=parent_post_id,
                created_at=item.get("timestamp"),
                message=item.get("text"),
                permalink=None,
                reactions=self._safe_int(item.get("like_count")) or 0,
                source_collector=self.name,
                depth=0,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("username"),
                    name=item.get("username"),
                    profile_url=f"https://www.instagram.com/{item.get('username')}/" if item.get("username") else None,
                ),
            )
            comments.append(top_level)
            for reply in (item.get("replies") or {}).get("data") or []:
                reply_id = f"{parent_post_id}:comment:{reply['id']}"
                reply_path = raw_store.write_json("instagram_reply_items", slugify(reply_id), reply)
                comments.append(
                    CommentSnapshot(
                        comment_id=reply_id,
                        platform="instagram",
                        parent_post_id=parent_post_id,
                        parent_comment_id=top_level.comment_id,
                        reply_to_message_id=str(item["id"]),
                        thread_root_post_id=parent_post_id,
                        created_at=reply.get("timestamp"),
                        message=reply.get("text"),
                        permalink=None,
                        reactions=self._safe_int(reply.get("like_count")) or 0,
                        source_collector=self.name,
                        depth=1,
                        raw_path=str(reply_path),
                        author=AuthorSnapshot(
                            author_id=reply.get("username"),
                            name=reply.get("username"),
                            profile_url=f"https://www.instagram.com/{reply.get('username')}/" if reply.get("username") else None,
                        ),
                    )
                )
        return comments

    @staticmethod
    def _extract_media_refs(post_id: str, media: dict[str, Any]) -> list[MediaReference]:
        refs: list[MediaReference] = []
        media_url = media.get("media_url")
        if media_url:
            refs.append(
                MediaReference(
                    media_id=f"{post_id}:media:1",
                    owner_post_id=post_id,
                    media_type=str(media.get("media_type") or "") or None,
                    url=str(media_url),
                    preview_url=str(media.get("thumbnail_url") or media_url),
                )
            )
        for index, child in enumerate(((media.get("children") or {}).get("data") if isinstance(media.get("children"), dict) else []) or [], start=2):
            refs.append(
                MediaReference(
                    media_id=f"{post_id}:media:{index}",
                    owner_post_id=post_id,
                    media_type=child.get("media_type"),
                    url=child.get("media_url"),
                    preview_url=child.get("thumbnail_url") or child.get("media_url"),
                )
            )
        return refs

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

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

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
