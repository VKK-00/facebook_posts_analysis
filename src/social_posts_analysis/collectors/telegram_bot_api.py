from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

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


class TelegramBotApiCollector(BaseCollector):
    name = "telegram_bot_api"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.telegram_bot_api
        if not self.settings.enabled:
            raise CollectorUnavailableError("Telegram Bot API collector is disabled in config.collector.telegram_bot_api.enabled.")
        if not self.settings.bot_token:
            raise CollectorUnavailableError("Telegram Bot API collector requires TELEGRAM_BOT_TOKEN or collector.telegram_bot_api.bot_token.")
        self.client = httpx.Client(timeout=self.settings.timeout_seconds)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        warnings = [
            "Telegram Bot API collector reads only updates currently visible to the bot and does not backfill channel history."
        ]
        updates_payload = self._get_updates()
        raw_store.write_json("telegram_bot_updates", "updates", updates_payload)
        updates = updates_payload.get("result") or []

        source_reference = self._source_reference()
        discussion_reference = self.config.source.telegram.discussion_chat_id
        posts_by_message_id: dict[int, PostSnapshot] = {}
        comment_maps: dict[int, dict[int, str]] = {}
        comment_depths: dict[str, int] = {}
        source_chat_payload: dict[str, Any] | None = None
        discussion_chat_payload: dict[str, Any] | None = None
        max_update_id: int | None = None

        for update in updates:
            max_update_id = max(max_update_id or 0, int(update.get("update_id") or 0))
            message = self._extract_message(update)
            if not message:
                continue

            chat = message.get("chat") or {}
            if self._chat_matches(chat, source_reference):
                source_chat_payload = source_chat_payload or chat
                if self._is_channel_post(message):
                    post_snapshot = self._build_post_snapshot(message=message, chat=chat, raw_store=raw_store)
                    posts_by_message_id[self._message_id(message)] = post_snapshot
                continue

            if discussion_reference and self._chat_matches(chat, discussion_reference):
                discussion_chat_payload = discussion_chat_payload or chat
                if self._is_automatic_forward(message):
                    continue
                thread_id = self._thread_id(message)
                if thread_id is None:
                    continue
                if thread_id not in comment_maps:
                    comment_maps[thread_id] = {}
                parent_post = posts_by_message_id.get(thread_id)
                if parent_post is None:
                    continue
                comment_snapshot = self._build_comment_snapshot(
                    message=message,
                    parent_post=parent_post,
                    raw_store=raw_store,
                    thread_comment_ids=comment_maps[thread_id],
                    comment_depths=comment_depths,
                )
                posts_by_message_id[thread_id] = parent_post.model_copy(
                    update={
                        "comments": [*parent_post.comments, comment_snapshot],
                        "comments_count": max(parent_post.comments_count, len(parent_post.comments) + 1),
                    }
                )
                comment_maps[thread_id][self._message_id(message)] = comment_snapshot.comment_id

        if not posts_by_message_id:
            warnings.append("No matching channel_post updates were visible to the bot for the configured source.")
        if discussion_reference and discussion_chat_payload is None:
            warnings.append("Configured discussion chat was not visible in the current bot update queue.")

        if self.settings.consume_updates and max_update_id is not None:
            try:
                self._acknowledge_updates(max_update_id + 1)
            except Exception as exc:
                warnings.append(f"Telegram Bot API update acknowledgement failed: {exc}")

        source_snapshot = SourceSnapshot(
            platform="telegram",
            source_id=self._chat_identifier(source_chat_payload) or source_reference,
            source_name=self._chat_name(source_chat_payload) or self.config.source.source_name or source_reference,
            source_url=self._chat_url(source_chat_payload) or self.config.source.url,
            source_type="channel",
            discussion_chat_id=self._chat_identifier(discussion_chat_payload) or discussion_reference,
            discussion_chat_name=self._chat_name(discussion_chat_payload),
            discussion_linked=discussion_chat_payload is not None if discussion_reference else None,
            source_collector=self.name,
            raw_path=str(raw_store.run_dir / "telegram_bot_updates/updates.json"),
        )
        posts = [
            post.model_copy(update={"comments": sorted(post.comments, key=lambda item: (item.depth, item.created_at or ""))})
            for _, post in sorted(posts_by_message_id.items(), key=lambda item: item[0], reverse=True)
        ]
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=warnings,
            cursors={"last_update_id": str(max_update_id)} if max_update_id is not None else {},
            source=source_snapshot,
            posts=posts,
        )

    def _build_post_snapshot(self, *, message: dict[str, Any], chat: dict[str, Any], raw_store: RawSnapshotStore) -> PostSnapshot:
        source_id = self._chat_identifier(chat) or self._source_reference()
        message_id = self._message_id(message)
        post_id = f"telegram:{source_id}:{message_id}"
        raw_path = raw_store.write_json("telegram_bot_posts", slugify(post_id), message)
        propagation_kind, origin_post_id, origin_external_id = self._propagation_metadata(message)
        return PostSnapshot(
            post_id=post_id,
            platform="telegram",
            source_id=source_id,
            origin_post_id=origin_post_id,
            origin_external_id=origin_external_id,
            propagation_kind=propagation_kind,
            is_propagation=propagation_kind is not None,
            created_at=self._created_at(message),
            message=self._message_text(message),
            permalink=self._message_permalink(chat, message_id),
            reactions=0,
            comments_count=0,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=AuthorSnapshot(
                author_id=source_id,
                name=self._chat_name(chat),
                profile_url=self._chat_url(chat),
            ),
            comments=[],
        )

    def _build_comment_snapshot(
        self,
        *,
        message: dict[str, Any],
        parent_post: PostSnapshot,
        raw_store: RawSnapshotStore,
        thread_comment_ids: dict[int, str],
        comment_depths: dict[str, int],
    ) -> CommentSnapshot:
        message_id = self._message_id(message)
        comment_id = f"{parent_post.post_id}:comment:{message_id}"
        reply_to = (message.get("reply_to_message") or {}).get("message_id")
        parent_comment_id = None
        if reply_to is not None and int(reply_to) != self._native_post_message_id(parent_post.post_id):
            parent_comment_id = thread_comment_ids.get(int(reply_to))
        depth = comment_depths.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
        raw_path = raw_store.write_json("telegram_bot_comments", slugify(comment_id), message)
        comment = CommentSnapshot(
            comment_id=comment_id,
            platform="telegram",
            parent_post_id=parent_post.post_id,
            parent_comment_id=parent_comment_id,
            reply_to_message_id=str(reply_to) if reply_to is not None else None,
            thread_root_post_id=parent_post.post_id,
            created_at=self._created_at(message),
            message=self._message_text(message),
            permalink=None,
            reactions=0,
            source_collector=self.name,
            depth=depth,
            raw_path=str(raw_path),
            author=self._author_snapshot(message.get("from") or {}),
        )
        comment_depths[comment.comment_id] = comment.depth
        return comment

    def _get_updates(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "limit": self.settings.update_limit,
            "timeout": 0,
            "allowed_updates": json.dumps(["channel_post", "edited_channel_post", "message", "edited_message"]),
        }
        if self.settings.offset is not None:
            params["offset"] = self.settings.offset
        payload = self._get_json("/getUpdates", params=params)
        if not payload.get("ok", False):
            raise CollectorUnavailableError(f"Telegram Bot API getUpdates failed: {payload}")
        return payload

    def _acknowledge_updates(self, offset: int) -> None:
        payload = self._get_json("/getUpdates", params={"offset": offset, "limit": 1, "timeout": 0})
        if not payload.get("ok", False):
            raise CollectorUnavailableError(f"Telegram Bot API acknowledgement failed: {payload}")

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.settings.base_url.rstrip('/')}/bot{self.settings.bot_token}{endpoint}"
        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _extract_message(update: dict[str, Any]) -> dict[str, Any] | None:
        for key in ("channel_post", "edited_channel_post", "message", "edited_message"):
            payload = update.get(key)
            if isinstance(payload, dict):
                return payload
        return None

    @staticmethod
    def _is_channel_post(message: dict[str, Any]) -> bool:
        chat = message.get("chat") or {}
        return chat.get("type") == "channel"

    @staticmethod
    def _is_automatic_forward(message: dict[str, Any]) -> bool:
        return bool(message.get("is_automatic_forward"))

    @staticmethod
    def _propagation_metadata(message: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
        forward_origin = message.get("forward_origin") or {}
        if message.get("is_automatic_forward") or forward_origin:
            message_id = (
                (message.get("reply_to_message") or {}).get("message_id")
                or message.get("forward_from_message_id")
                or message.get("message_id")
            )
            origin_external_id = str(message_id) if message_id is not None else None
            origin_post_id = f"telegram:origin:{origin_external_id}" if origin_external_id else None
            return "forward", origin_post_id, origin_external_id
        return None, None, None

    @staticmethod
    def _thread_id(message: dict[str, Any]) -> int | None:
        value = message.get("message_thread_id")
        if value is None:
            reply_to = message.get("reply_to_message") or {}
            value = reply_to.get("message_thread_id") or reply_to.get("message_id")
        return int(value) if value is not None else None

    @staticmethod
    def _message_id(message: dict[str, Any]) -> int:
        return int(message["message_id"])

    @staticmethod
    def _created_at(message: dict[str, Any]) -> str | None:
        unix_time = message.get("date")
        if unix_time is None:
            return None
        return datetime.fromtimestamp(int(unix_time), tz=UTC).replace(microsecond=0).isoformat()

    @staticmethod
    def _message_text(message: dict[str, Any]) -> str | None:
        for key in ("text", "caption"):
            value = message.get(key)
            if value:
                return str(value)
        return None

    @staticmethod
    def _author_snapshot(user: dict[str, Any]) -> AuthorSnapshot | None:
        if not user:
            return None
        username = user.get("username")
        return AuthorSnapshot(
            author_id=str(user.get("id")) if user.get("id") is not None else None,
            name=(" ".join(part for part in [user.get("first_name"), user.get("last_name")] if part) or username),
            profile_url=f"https://t.me/{username}" if username else None,
        )

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name.lstrip("@")
        if self.config.source.source_id:
            return str(self.config.source.source_id)
        if self.config.source.url:
            parsed = urlparse(self.config.source.url)
            parts = [part for part in parsed.path.split("/") if part and part != "s"]
            if parts:
                return parts[-1].lstrip("@")
        raise CollectorUnavailableError("Telegram Bot API collector requires source.source_name, source.source_id, or source.url.")

    @staticmethod
    def _chat_matches(chat: dict[str, Any], reference: str | None) -> bool:
        if not reference:
            return False
        chat_id = str(chat.get("id")) if chat.get("id") is not None else None
        username = str(chat.get("username") or "").lstrip("@")
        title = str(chat.get("title") or "")
        normalized_reference = str(reference).lstrip("@")
        return normalized_reference in {chat_id, username, title}

    @staticmethod
    def _chat_identifier(chat: dict[str, Any] | None) -> str | None:
        if not chat:
            return None
        return str(chat.get("username") or chat.get("id")) if chat.get("username") or chat.get("id") is not None else None

    @staticmethod
    def _chat_name(chat: dict[str, Any] | None) -> str | None:
        if not chat:
            return None
        return chat.get("title") or chat.get("username")

    @staticmethod
    def _chat_url(chat: dict[str, Any] | None) -> str | None:
        if not chat:
            return None
        username = chat.get("username")
        return f"https://t.me/{username}" if username else None

    @staticmethod
    def _message_permalink(chat: dict[str, Any], message_id: int) -> str | None:
        username = chat.get("username")
        if not username:
            return None
        return f"https://t.me/{username}/{message_id}"

    @staticmethod
    def _native_post_message_id(post_id: str) -> int:
        return int(post_id.split(":")[-1])
