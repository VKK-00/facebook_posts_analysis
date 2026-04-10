from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlparse

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


class XApiCollector(BaseCollector):
    name = "x_api"
    USER_FIELDS = "id,name,username,description,public_metrics,url"
    TWEET_FIELDS = (
        "id,text,created_at,conversation_id,author_id,attachments,referenced_tweets,"
        "in_reply_to_user_id,public_metrics,lang"
    )
    MEDIA_FIELDS = "media_key,type,url,preview_image_url"
    EXPANSIONS = "author_id,attachments.media_keys"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.x_api
        if not self.settings.enabled:
            raise CollectorUnavailableError("X API collector is disabled in config.collector.x_api.enabled.")
        if not self.settings.bearer_token:
            raise CollectorUnavailableError("X API collector requires X_BEARER_TOKEN or collector.x_api.bearer_token.")
        self.client = httpx.Client(
            timeout=self.settings.timeout_seconds,
            headers={
                "Authorization": f"Bearer {self.settings.bearer_token}",
                "User-Agent": "social-posts-analysis/0.1",
            },
        )

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        warnings = self._search_window_warnings()
        source_payload = self._resolve_source()
        source_data = source_payload.get("data") or {}
        source_raw_path = raw_store.write_json("x_source", "source_metadata", source_payload)

        source_snapshot = SourceSnapshot(
            platform="x",
            source_id=str(source_data.get("id") or self._source_reference()),
            source_name=source_data.get("name") or self.config.source.source_name,
            source_url=self._source_url(source_data),
            source_type="account",
            about=source_data.get("description"),
            followers_count=self._safe_int(((source_data.get("public_metrics") or {}).get("followers_count"))),
            source_collector=self.name,
            raw_path=str(source_raw_path),
        )

        posts: list[PostSnapshot] = []
        post_cursor = ""
        for page_index, payload in enumerate(self._iter_user_timeline_pages(source_snapshot.source_id), start=1):
            raw_store.write_json("x_timeline_pages", f"timeline-page-{page_index}", payload)
            if not post_cursor:
                post_cursor = str((payload.get("meta") or {}).get("next_token") or "")
            includes = self._build_includes(payload.get("includes") or {})
            for tweet_payload in payload.get("data") or []:
                post_snapshot = self._build_post_snapshot(
                    tweet_payload=tweet_payload,
                    includes=includes,
                    source_snapshot=source_snapshot,
                    raw_store=raw_store,
                )
                try:
                    comments = self._collect_replies_for_post(
                        post_snapshot=post_snapshot,
                        source_snapshot=source_snapshot,
                        raw_store=raw_store,
                    )
                except CollectorUnavailableError as exc:
                    warnings.append(f"Replies search failed for post {post_snapshot.post_id}: {exc}")
                    comments = []
                post_snapshot = post_snapshot.model_copy(
                    update={"comments": comments, "comments_count": max(post_snapshot.comments_count, len(comments))}
                )
                posts.append(post_snapshot)

        status: Literal["success", "partial", "failed"] = "partial" if warnings else "success"
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status=status,
            warnings=list(dict.fromkeys(warnings)),
            cursors={"timeline_next_token": post_cursor} if post_cursor else {},
            source=source_snapshot,
            posts=posts,
        )

    def _resolve_source(self) -> dict[str, Any]:
        source_id = (self.config.source.source_id or "").strip()
        if source_id.isdigit():
            return self._get_json(
                f"/users/{source_id}",
                params={"user.fields": self.USER_FIELDS},
            )

        username = self._source_reference()
        return self._get_json(
            f"/users/by/username/{username}",
            params={"user.fields": self.USER_FIELDS},
        )

    def _source_reference(self) -> str:
        for raw_value in (self.config.source.source_name, self.config.source.source_id, self.config.source.url):
            if not raw_value:
                continue
            parsed = self._extract_username(raw_value)
            if parsed:
                return parsed
        raise CollectorUnavailableError("X source requires source.source_name, source.source_id, or source.url.")

    @staticmethod
    def _extract_username(raw_value: str) -> str | None:
        value = raw_value.strip()
        if not value:
            return None
        if value.isdigit():
            return value
        if value.startswith("@"):
            return value[1:]
        if value.startswith("http://") or value.startswith("https://"):
            parsed = urlparse(value)
            parts = [part for part in parsed.path.split("/") if part]
            if not parts:
                return None
            if parts[0].startswith("@"):
                return parts[0][1:]
            return parts[0]
        return value

    def _iter_user_timeline_pages(self, user_id: str) -> list[dict[str, Any]]:
        endpoint = f"/users/{user_id}/tweets"
        params: dict[str, Any] = {
            "max_results": min(max(10, self.settings.page_size), 100),
            "exclude": "replies",
            "tweet.fields": self.TWEET_FIELDS,
            "user.fields": self.USER_FIELDS,
            "media.fields": self.MEDIA_FIELDS,
            "expansions": self.EXPANSIONS,
        }
        start_time = self._start_time()
        end_time = self._end_time()
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        pages: list[dict[str, Any]] = []
        next_token: str | None = None
        while True:
            current_params = {**params, **({"pagination_token": next_token} if next_token else {})}
            payload = self._get_json(endpoint, params=current_params)
            pages.append(payload)
            next_token = (payload.get("meta") or {}).get("next_token")
            if not next_token:
                break
        return pages

    def _collect_replies_for_post(
        self,
        *,
        post_snapshot: PostSnapshot,
        source_snapshot: SourceSnapshot,
        raw_store: RawSnapshotStore,
    ) -> list[CommentSnapshot]:
        post_tweet_id = self._native_tweet_id(post_snapshot.post_id)
        replies: dict[str, dict[str, Any]] = {}
        include_users: dict[str, dict[str, Any]] = {}
        search_pages = self._iter_reply_search_pages(post_tweet_id)
        for page_index, payload in enumerate(search_pages, start=1):
            raw_store.write_json("x_reply_pages", f"{slugify(post_tweet_id)}-page-{page_index}", payload)
            includes = self._build_includes(payload.get("includes") or {})
            include_users.update(includes["users"])
            for tweet_payload in payload.get("data") or []:
                tweet_id = str(tweet_payload.get("id") or "")
                if not tweet_id or tweet_id == post_tweet_id:
                    continue
                if str(tweet_payload.get("conversation_id") or "") != post_tweet_id:
                    continue
                if not self._replied_to_tweet_id(tweet_payload):
                    continue
                replies[tweet_id] = tweet_payload

        ordered_replies = sorted(replies.values(), key=lambda item: (item.get("created_at") or "", str(item.get("id") or "")))
        tweet_id_to_comment_id: dict[str, str] = {}
        comment_depths: dict[str, int] = {}
        comment_snapshots: list[CommentSnapshot] = []
        for tweet_payload in ordered_replies:
            comment_snapshot = self._build_comment_snapshot(
                tweet_payload=tweet_payload,
                source_snapshot=source_snapshot,
                post_snapshot=post_snapshot,
                include_users=include_users,
                raw_store=raw_store,
                tweet_id_to_comment_id=tweet_id_to_comment_id,
                comment_depths=comment_depths,
            )
            comment_snapshots.append(comment_snapshot)
        return comment_snapshots

    def _iter_reply_search_pages(self, conversation_id: str) -> list[dict[str, Any]]:
        endpoint = f"/tweets/search/{self.settings.search_scope}"
        query = f"conversation_id:{conversation_id} -is:retweet"
        params: dict[str, Any] = {
            "query": query,
            "max_results": min(max(10, self.settings.page_size), 100),
            "tweet.fields": self.TWEET_FIELDS,
            "user.fields": self.USER_FIELDS,
            "media.fields": self.MEDIA_FIELDS,
            "expansions": self.EXPANSIONS,
            "sort_order": "recency",
        }
        start_time = self._start_time()
        end_time = self._end_time()
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        pages: list[dict[str, Any]] = []
        next_token: str | None = None
        while True:
            current_params = {**params, **({"next_token": next_token} if next_token else {})}
            payload = self._get_json(endpoint, params=current_params)
            pages.append(payload)
            next_token = (payload.get("meta") or {}).get("next_token")
            if not next_token:
                break
        return pages

    def _build_post_snapshot(
        self,
        *,
        tweet_payload: dict[str, Any],
        includes: dict[str, dict[str, dict[str, Any]]],
        source_snapshot: SourceSnapshot,
        raw_store: RawSnapshotStore,
    ) -> PostSnapshot:
        tweet_id = str(tweet_payload["id"])
        post_id = self._post_id(source_snapshot.source_id, tweet_id)
        raw_path = raw_store.write_json("x_posts", slugify(post_id), tweet_payload)
        public_metrics = dict(tweet_payload.get("public_metrics") or {})
        media_refs = self._extract_media_refs(post_id, tweet_payload, includes["media"])
        reaction_breakdown = self._metric_breakdown(tweet_payload)
        author = self._author_from_payload(tweet_payload, includes["users"]) or AuthorSnapshot(
            author_id=source_snapshot.source_id,
            name=source_snapshot.source_name,
            profile_url=source_snapshot.source_url,
        )
        propagation_kind, origin_post_id, origin_external_id = self._propagation_metadata(tweet_payload)
        return PostSnapshot(
            post_id=post_id,
            platform="x",
            source_id=source_snapshot.source_id,
            origin_post_id=origin_post_id,
            origin_external_id=origin_external_id,
            origin_permalink=self._origin_permalink(origin_external_id),
            propagation_kind=propagation_kind,
            is_propagation=propagation_kind is not None,
            created_at=tweet_payload.get("created_at"),
            message=tweet_payload.get("text"),
            permalink=self._tweet_permalink(author, tweet_id),
            reactions=self._safe_int(public_metrics.get("like_count")) or 0,
            shares=self._safe_int(public_metrics.get("retweet_count")) or 0,
            comments_count=self._safe_int(public_metrics.get("reply_count")) or 0,
            views=self._extract_views(tweet_payload),
            forwards=self._safe_int(public_metrics.get("quote_count")),
            reply_count=self._safe_int(public_metrics.get("reply_count")),
            has_media=bool(media_refs),
            media_type=media_refs[0].media_type if media_refs else None,
            reaction_breakdown_json=json.dumps(reaction_breakdown, ensure_ascii=False) if reaction_breakdown else None,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=author,
            media_refs=media_refs,
        )

    def _build_comment_snapshot(
        self,
        *,
        tweet_payload: dict[str, Any],
        source_snapshot: SourceSnapshot,
        post_snapshot: PostSnapshot,
        include_users: dict[str, dict[str, Any]],
        raw_store: RawSnapshotStore,
        tweet_id_to_comment_id: dict[str, str],
        comment_depths: dict[str, int],
    ) -> CommentSnapshot:
        tweet_id = str(tweet_payload["id"])
        comment_id = self._comment_id(source_snapshot.source_id, self._native_tweet_id(post_snapshot.post_id), tweet_id)
        raw_path = raw_store.write_json(
            "x_reply_items",
            slugify(comment_id),
            tweet_payload,
        )
        replied_to_tweet_id = self._replied_to_tweet_id(tweet_payload)
        parent_comment_id = (
            tweet_id_to_comment_id.get(replied_to_tweet_id)
            if replied_to_tweet_id and replied_to_tweet_id != self._native_tweet_id(post_snapshot.post_id)
            else None
        )
        depth = comment_depths.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
        public_metrics = dict(tweet_payload.get("public_metrics") or {})
        author = self._author_from_payload(tweet_payload, include_users)
        snapshot = CommentSnapshot(
            comment_id=comment_id,
            platform="x",
            parent_post_id=post_snapshot.post_id,
            parent_comment_id=parent_comment_id,
            reply_to_message_id=replied_to_tweet_id,
            thread_root_post_id=post_snapshot.post_id,
            created_at=tweet_payload.get("created_at"),
            message=tweet_payload.get("text"),
            permalink=self._tweet_permalink(author, tweet_id),
            reactions=self._safe_int(public_metrics.get("like_count")) or 0,
            reaction_breakdown_json=json.dumps(self._metric_breakdown(tweet_payload), ensure_ascii=False),
            source_collector=self.name,
            depth=depth,
            raw_path=str(raw_path),
            author=author,
        )
        tweet_id_to_comment_id[tweet_id] = snapshot.comment_id
        comment_depths[snapshot.comment_id] = snapshot.depth
        return snapshot

    def _build_includes(self, includes_payload: dict[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
        users = {
            str(user.get("id")): user
            for user in includes_payload.get("users") or []
            if user.get("id") is not None
        }
        media = {
            str(item.get("media_key")): item
            for item in includes_payload.get("media") or []
            if item.get("media_key") is not None
        }
        return {"users": users, "media": media}

    def _author_from_payload(
        self,
        tweet_payload: dict[str, Any],
        include_users: dict[str, dict[str, Any]],
    ) -> AuthorSnapshot | None:
        author_id = str(tweet_payload.get("author_id") or "")
        if not author_id:
            return None
        user_payload = include_users.get(author_id) or {}
        username = user_payload.get("username")
        return AuthorSnapshot(
            author_id=author_id,
            name=user_payload.get("name") or username,
            profile_url=f"https://x.com/{username}" if username else None,
        )

    def _extract_media_refs(
        self,
        post_id: str,
        tweet_payload: dict[str, Any],
        include_media: dict[str, dict[str, Any]],
    ) -> list[MediaReference]:
        media_keys = ((tweet_payload.get("attachments") or {}).get("media_keys") or [])
        refs: list[MediaReference] = []
        for index, media_key in enumerate(media_keys, start=1):
            media_payload = include_media.get(str(media_key), {})
            refs.append(
                MediaReference(
                    media_id=f"{post_id}:media:{index}",
                    owner_post_id=post_id,
                    media_type=media_payload.get("type"),
                    url=media_payload.get("url"),
                    preview_url=media_payload.get("preview_image_url"),
                )
            )
        return refs

    def _metric_breakdown(self, tweet_payload: dict[str, Any]) -> dict[str, int]:
        public_metrics = dict(tweet_payload.get("public_metrics") or {})
        breakdown: dict[str, int] = {}
        for key in ("like_count", "retweet_count", "reply_count", "quote_count", "bookmark_count"):
            if key in public_metrics:
                breakdown[key] = self._safe_int(public_metrics.get(key)) or 0
        view_count = self._extract_views(tweet_payload)
        if view_count is not None:
            breakdown["view_count"] = view_count
        return breakdown

    @classmethod
    def _propagation_metadata(cls, tweet_payload: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
        for item in tweet_payload.get("referenced_tweets") or []:
            reference_type = str(item.get("type") or "")
            reference_id = str(item.get("id") or "")
            if not reference_id:
                continue
            if reference_type == "quoted":
                return "quote", cls._origin_placeholder_post_id(reference_id), reference_id
            if reference_type == "retweeted":
                return "repost", cls._origin_placeholder_post_id(reference_id), reference_id
        return None, None, None

    @staticmethod
    def _origin_placeholder_post_id(native_tweet_id: str) -> str:
        return f"x:origin:{native_tweet_id}"

    @staticmethod
    def _origin_permalink(native_tweet_id: str | None) -> str | None:
        if not native_tweet_id:
            return None
        return f"https://x.com/i/status/{native_tweet_id}"

    @staticmethod
    def _extract_views(tweet_payload: dict[str, Any]) -> int | None:
        metrics_candidates = [
            tweet_payload.get("public_metrics") or {},
            tweet_payload.get("organic_metrics") or {},
            tweet_payload.get("non_public_metrics") or {},
        ]
        for payload in metrics_candidates:
            for key in ("impression_count", "view_count"):
                value = payload.get(key)
                parsed = XApiCollector._safe_int(value)
                if parsed is not None:
                    return parsed
        return XApiCollector._safe_int(tweet_payload.get("view_count"))

    @staticmethod
    def _replied_to_tweet_id(tweet_payload: dict[str, Any]) -> str | None:
        for item in tweet_payload.get("referenced_tweets") or []:
            if item.get("type") == "replied_to" and item.get("id") is not None:
                return str(item["id"])
        return None

    @staticmethod
    def _source_url(source_data: dict[str, Any]) -> str | None:
        username = source_data.get("username")
        if username:
            return f"https://x.com/{username}"
        return source_data.get("url")

    @staticmethod
    def _tweet_permalink(author: AuthorSnapshot | None, tweet_id: str) -> str | None:
        if author is None or not author.profile_url:
            return None
        return f"{author.profile_url}/status/{tweet_id}"

    @staticmethod
    def _post_id(source_id: str, tweet_id: str) -> str:
        return f"x:{source_id}:{tweet_id}"

    @staticmethod
    def _comment_id(source_id: str, post_tweet_id: str, tweet_id: str) -> str:
        return f"x:{source_id}:{post_tweet_id}:comment:{tweet_id}"

    @staticmethod
    def _native_tweet_id(post_id: str) -> str:
        return post_id.split(":")[-1]

    def _search_window_warnings(self) -> list[str]:
        if self.settings.search_scope != "recent" or not self.config.date_range.start:
            return []
        start_dt = self._parse_config_datetime(self.config.date_range.start, end_of_day=False)
        if start_dt is None:
            return []
        cutoff_dt = datetime.now(tz=UTC) - timedelta(days=7)
        if start_dt < cutoff_dt:
            return [
                (
                    "collector.x_api.search_scope='recent' may omit replies before "
                    f"{cutoff_dt.date().isoformat()} for a requested start date of {start_dt.date().isoformat()}."
                )
            ]
        return []

    def _start_time(self) -> str | None:
        if not self.config.date_range.start:
            return None
        parsed = self._parse_config_datetime(self.config.date_range.start, end_of_day=False)
        return self._iso_z(parsed) if parsed else None

    def _end_time(self) -> str | None:
        if not self.config.date_range.end:
            return None
        parsed = self._parse_config_datetime(self.config.date_range.end, end_of_day=True)
        return self._iso_z(parsed) if parsed else None

    @staticmethod
    def _parse_config_datetime(raw_value: str, *, end_of_day: bool) -> datetime | None:
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
    def _iso_z(value: datetime) -> str:
        return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

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
    def _get_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.settings.base_url.rstrip('/')}{endpoint}"
        response = self.client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
        if "errors" in payload:
            raise CollectorUnavailableError(json.dumps(payload["errors"], ensure_ascii=False))
        return payload
