from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

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


class MetaApiCollector(BaseCollector):
    name = "meta_api"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.meta_api
        if not self.settings.enabled:
            raise CollectorUnavailableError("Meta API collector is disabled in config.collector.meta_api.enabled.")
        if not self.settings.access_token:
            raise CollectorUnavailableError(
                "Meta API collector requires META_ACCESS_TOKEN or collector.meta_api.access_token."
            )
        self.client = httpx.Client(timeout=self.settings.timeout_seconds)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        source_ref = self.config.source.source_id or self._page_reference_from_url(self.config.source.url or "")
        source_payload = self._get_json(
            f"/{source_ref}",
            params={
                "fields": "id,name,link,about,fan_count,followers_count",
                "access_token": self.settings.access_token,
            },
        )
        source_raw_path = raw_store.write_json("api_source", "source_metadata", source_payload)
        source_snapshot = SourceSnapshot(
            platform="facebook",
            source_id=str(source_payload.get("id", source_ref)),
            source_name=source_payload.get("name") or self.config.source.source_name,
            source_url=source_payload.get("link") or self.config.source.url,
            source_type="page",
            about=source_payload.get("about"),
            fan_count=source_payload.get("fan_count"),
            followers_count=source_payload.get("followers_count"),
            source_collector=self.name,
            raw_path=str(source_raw_path),
        )

        posts: list[PostSnapshot] = []
        warnings: list[str] = []
        post_cursor = ""

        for feed_payload in self._iter_feed_pages(source_snapshot.source_id, raw_store):
            if not post_cursor:
                post_cursor = self._extract_cursor(feed_payload)
            for post_payload in feed_payload.get("data", []):
                try:
                    posts.append(self._collect_post(post_payload, source_snapshot.source_id, raw_store))
                except httpx.HTTPError as exc:
                    warnings.append(f"Failed to collect comments for post {post_payload.get('id')}: {exc}")

        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=warnings,
            cursors={"feed_after": post_cursor} if post_cursor else {},
            source=source_snapshot,
            posts=posts,
        )

    def _collect_post(self, payload: dict[str, Any], source_id: str, raw_store: RawSnapshotStore) -> PostSnapshot:
        post_id = str(payload["id"])
        comments = self._collect_comments_for_parent(
            parent_id=post_id,
            parent_post_id=post_id,
            parent_comment_id=None,
            depth=0,
            raw_store=raw_store,
        )
        attachments = self._extract_media_refs(payload)
        author_payload = payload.get("from") or {"id": payload.get("id"), "name": self.config.source.source_name}

        return PostSnapshot(
            post_id=post_id,
            platform="facebook",
            source_id=source_id,
            created_at=payload.get("created_time"),
            message=payload.get("message"),
            permalink=payload.get("permalink_url"),
            reactions=self._summary_total(payload.get("reactions")),
            shares=(payload.get("shares") or {}).get("count", 0) or 0,
            comments_count=self._summary_total(payload.get("comments")),
            has_media=bool(attachments),
            media_type=attachments[0].media_type if attachments else None,
            source_collector=self.name,
            raw_path=str(raw_store.write_json("api_posts", slugify(post_id), payload)),
            author=AuthorSnapshot(
                author_id=str(author_payload.get("id")) if author_payload.get("id") else None,
                name=author_payload.get("name"),
            ),
            media_refs=attachments,
            comments=comments,
        )

    def _collect_comments_for_parent(
        self,
        parent_id: str,
        parent_post_id: str,
        parent_comment_id: str | None,
        depth: int,
        raw_store: RawSnapshotStore,
    ) -> list[CommentSnapshot]:
        comments: list[CommentSnapshot] = []
        for page_index, payload in enumerate(
            self._iter_comment_pages(parent_id=parent_id, raw_store=raw_store, depth=depth),
            start=1,
        ):
            raw_store.write_json(
                "api_comments",
                f"{slugify(parent_id)}-depth-{depth}-page-{page_index}",
                payload,
            )
            for comment_payload in payload.get("data", []):
                author_payload = comment_payload.get("from") or {}
                comment_id = str(comment_payload["id"])
                comment = CommentSnapshot(
                    comment_id=comment_id,
                    platform="facebook",
                    parent_post_id=parent_post_id,
                    parent_comment_id=parent_comment_id,
                    created_at=comment_payload.get("created_time"),
                    message=comment_payload.get("message"),
                    permalink=comment_payload.get("permalink_url"),
                    reactions=(comment_payload.get("like_count") or 0),
                    source_collector=self.name,
                    depth=depth,
                    raw_path=str(
                        raw_store.write_json(
                            "api_comment_items",
                            slugify(comment_id),
                            comment_payload,
                        )
                    ),
                    author=AuthorSnapshot(
                        author_id=str(author_payload.get("id")) if author_payload.get("id") else None,
                        name=author_payload.get("name"),
                    ),
                )
                comments.append(comment)
                if (comment_payload.get("comment_count") or 0) > 0:
                    comments.extend(
                        self._collect_comments_for_parent(
                            parent_id=comment_id,
                            parent_post_id=parent_post_id,
                            parent_comment_id=comment_id,
                            depth=depth + 1,
                            raw_store=raw_store,
                        )
                    )
        return comments

    def _iter_feed_pages(self, source_id: str, raw_store: RawSnapshotStore) -> list[dict[str, Any]]:
        endpoint = f"/{source_id}/feed"
        params: dict[str, Any] = {
            "fields": (
                "id,message,created_time,permalink_url,from,"
                "shares,reactions.limit(0).summary(true),"
                "comments.limit(0).summary(true),"
                "attachments{media_type,media,url,title,description}"
            ),
            "limit": self.settings.page_size,
            "access_token": self.settings.access_token,
        }
        if self.config.date_range.start:
            params["since"] = self.config.date_range.start
        if self.config.date_range.end:
            params["until"] = self.config.date_range.end

        pages: list[dict[str, Any]] = []
        current_params: dict[str, Any] | None = params
        next_url: str | None = None
        page_number = 0
        while True:
            page_number += 1
            payload = self._get_json(endpoint, params=current_params, full_url=next_url)
            raw_store.write_json("api_feed_pages", f"feed-page-{page_number}", payload)
            pages.append(payload)
            next_url = (payload.get("paging") or {}).get("next")
            if not next_url:
                break
            current_params = None
        return pages

    def _iter_comment_pages(
        self,
        parent_id: str,
        raw_store: RawSnapshotStore,
        depth: int,
    ) -> list[dict[str, Any]]:
        endpoint = f"/{parent_id}/comments"
        params: dict[str, Any] = {
            "fields": "id,message,created_time,from,permalink_url,comment_count,like_count",
            "limit": self.settings.page_size,
            "access_token": self.settings.access_token,
        }
        pages: list[dict[str, Any]] = []
        current_params: dict[str, Any] | None = params
        next_url: str | None = None
        page_number = 0
        while True:
            page_number += 1
            payload = self._get_json(endpoint, params=current_params, full_url=next_url)
            raw_store.write_json(
                "api_comment_pages",
                f"{slugify(parent_id)}-depth-{depth}-page-{page_number}",
                payload,
            )
            pages.append(payload)
            next_url = (payload.get("paging") or {}).get("next")
            if not next_url:
                break
            current_params = None
        return pages

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _get_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        full_url: str | None = None,
    ) -> dict[str, Any]:
        url = full_url or f"{self.settings.base_url.rstrip('/')}/{self.settings.api_version}{endpoint}"
        response = self.client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise CollectorUnavailableError(str(payload["error"]))
        return payload

    @staticmethod
    def _summary_total(payload: dict[str, Any] | None) -> int:
        if not payload:
            return 0
        summary = payload.get("summary") or {}
        return int(summary.get("total_count") or 0)

    @staticmethod
    def _extract_media_refs(payload: dict[str, Any]) -> list[MediaReference]:
        data = ((payload.get("attachments") or {}).get("data") or [])
        post_id = str(payload["id"])
        refs: list[MediaReference] = []
        for index, item in enumerate(data, start=1):
            refs.append(
                MediaReference(
                    media_id=f"{post_id}:media:{index}",
                    owner_post_id=post_id,
                    media_type=item.get("media_type"),
                    title=item.get("title"),
                    url=item.get("url"),
                    preview_url=((item.get("media") or {}).get("image") or {}).get("src"),
                )
            )
        return refs

    @staticmethod
    def _page_reference_from_url(url: str) -> str:
        parsed = urlparse(url)
        if parsed.query:
            qs = parse_qs(parsed.query)
            if "id" in qs and qs["id"]:
                return qs["id"][0]
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise CollectorUnavailableError("Unable to infer page reference from URL.")
        return parts[-1]

    @staticmethod
    def _extract_cursor(payload: dict[str, Any]) -> str:
        cursors = (payload.get("paging") or {}).get("cursors") or {}
        return str(cursors.get("after") or "")
