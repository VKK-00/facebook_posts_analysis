from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

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
from .range_utils import RangeFilter, parse_configured_datetime
from .value_utils import safe_int


@dataclass(slots=True)
class DiscussionContext:
    chat: Any
    root_message_id: int
    expected_comment_count: int = 0


class TelegramMtprotoCollector(BaseCollector):
    name = "telegram_mtproto"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.telegram_mtproto
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Telegram MTProto collector is disabled in config.collector.telegram_mtproto.enabled.")
        if not self.settings.session_file:
            raise CollectorUnavailableError(
                "Telegram MTProto collector requires collector.telegram_mtproto.session_file."
            )
        if self.settings.api_id is None or not self.settings.api_hash:
            raise CollectorUnavailableError(
                "Telegram MTProto collector requires collector.telegram_mtproto.api_id and api_hash."
            )
        try:
            from telethon.sync import TelegramClient  # noqa: F401
        except ImportError as exc:
            raise CollectorUnavailableError("Telegram MTProto collector requires the telethon package.") from exc

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        warnings: list[str] = []
        filtered_service_message_count = 0
        discussion_linked = False
        discussion_chat_id: str | None = None
        discussion_chat_name: str | None = None

        client = self._open_client()
        source_snapshot_id = self.config.source.source_id or self.config.source.source_name or self._source_reference()
        try:
            source_entity = self._resolve_source_entity(client)
            source_payload = self._serialize_object(source_entity)
            source_raw_path = raw_store.write_json("telegram_source", "source_metadata", source_payload)
            source_snapshot_id = self._stringify(self._entity_id(source_entity)) or source_snapshot_id

            discussion_entity = self._resolve_discussion_entity(client, source_entity)
            if discussion_entity is None:
                warnings.append("Telegram channel has no linked discussion chat; collected posts only.")
            else:
                discussion_linked = True
                discussion_chat_id = self._stringify(self._entity_id(discussion_entity))
                discussion_chat_name = self._entity_title(discussion_entity)

            posts: list[PostSnapshot] = []
            for message in self._iter_source_messages(client, source_entity):
                if self._is_service_message(message):
                    filtered_service_message_count += 1
                    continue
                post_snapshot = self._build_post_snapshot(
                    message=message,
                    source_entity=source_entity,
                    raw_store=raw_store,
                )
                if discussion_entity is not None:
                    discussion_context = self._fetch_discussion_context(client, source_entity, message)
                    if discussion_context is not None:
                        comment_snapshots, skipped_service_messages = self._collect_discussion_comments(
                            client=client,
                            discussion_context=discussion_context,
                            post_snapshot=post_snapshot,
                            raw_store=raw_store,
                        )
                        filtered_service_message_count += skipped_service_messages
                        if discussion_context.expected_comment_count > len(comment_snapshots):
                            warnings.append(
                                "Telegram discussion comments incomplete for "
                                f"{post_snapshot.post_id}: visible={discussion_context.expected_comment_count}, "
                                f"extracted={len(comment_snapshots)}."
                            )
                        post_snapshot = post_snapshot.model_copy(
                            update={
                                "comments": comment_snapshots,
                                "comments_count": max(post_snapshot.comments_count, len(comment_snapshots)),
                            }
                        )
                posts.append(post_snapshot)
            if self.config.history.active and len(posts) >= self.config.history.max_items_per_window:
                warnings.append(
                    "Telegram history window reached "
                    f"history.max_items_per_window={self.config.history.max_items_per_window}; "
                    "older or remaining messages may be truncated."
                )
        finally:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()

        source_snapshot = SourceSnapshot(
            platform="telegram",
            source_id=source_snapshot_id,
            source_name=self._entity_title(source_entity) or self.config.source.source_name,
            source_url=self._entity_url(source_entity) or self.config.source.url,
            source_type="channel",
            discussion_chat_id=discussion_chat_id or self.config.source.telegram.discussion_chat_id,
            discussion_chat_name=discussion_chat_name,
            discussion_linked=discussion_linked,
            filtered_service_message_count=filtered_service_message_count,
            source_collector=self.name,
            raw_path=str(source_raw_path),
        )

        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=warnings,
            source=source_snapshot,
            posts=posts,
        )

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

        client = self._open_client()
        try:
            discovered: dict[str, dict[str, str | None]] = {}
            for query in queries:
                for message in self._iter_person_monitor_search_messages(
                    client,
                    query=query,
                    max_items=max_items_per_query,
                ):
                    entity = self._message_container_entity(client, message)
                    if entity is None:
                        continue
                    if not self._search_result_matches_kind(
                        message,
                        entity,
                        include_posts=include_posts,
                        include_comments=include_comments,
                    ):
                        continue
                    payload = self._entity_surface_payload(entity)
                    identity = payload["source_id"] or payload["source_url"] or payload["source_name"]
                    if not identity:
                        continue
                    discovered[str(identity)] = payload
            return list(discovered.values())
        finally:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    def _open_client(self) -> Any:
        from telethon.sync import TelegramClient

        session_file = Path(self.settings.session_file or "").expanduser()
        client = TelegramClient(str(session_file), self.settings.api_id, self.settings.api_hash)
        client.connect()
        if not client.is_user_authorized():
            client.disconnect()
            raise CollectorUnavailableError(
                "Telegram MTProto session is not authorized. Log in once with the configured session file."
            )
        return client

    def oldest_source_datetime(self) -> str | None:
        client = self._open_client()
        try:
            source_entity = self._resolve_source_entity(client)
            messages = list(client.iter_messages(source_entity, limit=25, reverse=True))
            for message in messages:
                if self._is_service_message(message):
                    continue
                return self._iso_datetime(self._message_datetime(message))
            return None
        finally:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    def _iter_person_monitor_search_messages(
        self,
        client: Any,
        *,
        query: str,
        max_items: int,
    ) -> list[Any]:
        messages = list(
            client.iter_messages(
                None,
                search=query,
                limit=max(1, max_items),
                offset_date=self._end_datetime(),
                reverse=False,
            )
        )
        return [
            message
            for message in messages
            if not self._is_service_message(message) and self._within_range(self._message_datetime(message))
        ]

    def _message_container_entity(self, client: Any, message: Any) -> Any | None:
        entity = getattr(message, "chat", None)
        if entity is None and isinstance(message, dict):
            entity = message.get("chat")
        if entity is not None:
            return entity
        peer = getattr(message, "peer_id", None)
        if peer is None and isinstance(message, dict):
            peer = message.get("peer_id")
        if peer is None:
            return None
        try:
            return client.get_entity(peer)
        except Exception:
            return None

    def _entity_surface_payload(self, entity: Any) -> dict[str, str | None]:
        return {
            "source_id": self._entity_reference(entity),
            "source_name": self._entity_title(entity),
            "source_url": self._entity_url(entity),
            "source_type": self._entity_source_type(entity),
        }

    def _search_result_matches_kind(
        self,
        message: Any,
        entity: Any,
        *,
        include_posts: bool,
        include_comments: bool,
    ) -> bool:
        if include_posts and include_comments:
            return True
        source_type = self._entity_source_type(entity)
        is_post_like = source_type == "channel"
        if include_posts and is_post_like:
            return True
        if include_comments and not is_post_like:
            return True
        return False

    def _resolve_source_entity(self, client: Any) -> Any:
        reference = self._source_reference()
        try:
            return client.get_entity(reference)
        except Exception as exc:  # pragma: no cover - network surface
            raise CollectorUnavailableError(f"Unable to resolve Telegram source '{reference}': {exc}") from exc

    def _source_reference(self) -> str:
        if self.config.source.source_id:
            return self.config.source.source_id
        if self.config.source.source_name:
            return self.config.source.source_name
        if self.config.source.url:
            parsed = urlparse(self.config.source.url)
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return parts[-1]
        raise CollectorUnavailableError("Telegram source requires source.url, source.source_id, or source.source_name.")

    def _resolve_discussion_entity(self, client: Any, source_entity: Any) -> Any | None:
        explicit_id = self.config.source.telegram.discussion_chat_id
        try:
            if explicit_id:
                return client.get_entity(explicit_id)

            from telethon.tl import functions

            full_channel = client(functions.channels.GetFullChannelRequest(channel=source_entity))
            linked_chat_id = getattr(full_channel.full_chat, "linked_chat_id", None)
            if not linked_chat_id:
                return None
            return client.get_entity(linked_chat_id)
        except Exception:
            return None

    def _iter_source_messages(self, client: Any, source_entity: Any) -> list[Any]:
        reverse = False
        messages = list(
            client.iter_messages(
                source_entity,
                limit=self._source_message_limit(),
                offset_date=self._end_datetime(),
                reverse=reverse,
            )
        )
        filtered = [message for message in messages if self._within_range(self._message_datetime(message))]
        filtered.sort(key=lambda message: (self._message_datetime(message) or datetime.min.replace(tzinfo=UTC)), reverse=True)
        return filtered

    def _fetch_discussion_context(self, client: Any, source_entity: Any, message: Any) -> DiscussionContext | None:
        try:
            from telethon.tl import functions

            result = client(functions.messages.GetDiscussionMessageRequest(peer=source_entity, msg_id=self._message_id(message)))
            chat = None
            if getattr(result, "chats", None):
                chat = result.chats[0]
            if chat is None and self.config.source.telegram.discussion_chat_id:
                chat = client.get_entity(self.config.source.telegram.discussion_chat_id)
            if chat is None:
                return None
            root_message = next(
                (
                    item
                    for item in getattr(result, "messages", []) or []
                    if self._message_id(item) != self._message_id(message)
                ),
                None,
            )
            if root_message is None:
                root_message = next(iter(getattr(result, "messages", []) or []), None)
            if root_message is None:
                return None
            replies = getattr(root_message, "replies", None)
            expected_comment_count = safe_int(getattr(replies, "replies", None))
            return DiscussionContext(
                chat=chat,
                root_message_id=self._message_id(root_message),
                expected_comment_count=expected_comment_count or 0,
            )
        except Exception:
            return None

    def _collect_discussion_comments(
        self,
        *,
        client: Any,
        discussion_context: DiscussionContext,
        post_snapshot: PostSnapshot,
        raw_store: RawSnapshotStore,
    ) -> tuple[list[CommentSnapshot], int]:
        comments: list[CommentSnapshot] = []
        filtered_service_messages = 0
        message_to_comment_id: dict[str, str] = {}
        comment_depths: dict[str, int] = {}

        discussion_messages = self._order_discussion_messages(
            self._iter_discussion_messages(client, discussion_context),
            root_message_id=discussion_context.root_message_id,
        )
        for message in discussion_messages:
            if self._is_service_message(message):
                filtered_service_messages += 1
                continue
            comment_snapshot = self._build_comment_snapshot(
                message=message,
                post_snapshot=post_snapshot,
                raw_store=raw_store,
                message_to_comment_id=message_to_comment_id,
                comment_depths=comment_depths,
            )
            comments.append(comment_snapshot)
        comments.sort(key=lambda item: (item.depth, item.created_at or "", item.comment_id))
        return comments, filtered_service_messages

    def _iter_discussion_messages(self, client: Any, discussion_context: DiscussionContext) -> Iterable[Any]:
        direct_messages = list(
            client.iter_messages(
                discussion_context.chat,
                limit=self._discussion_direct_limit(),
                reply_to=discussion_context.root_message_id,
                reverse=True,
            )
        )
        messages_by_id = {self._message_id(message): message for message in direct_messages}
        scan_limit = max(
            self._discussion_direct_limit() * 3,
            min(max(discussion_context.expected_comment_count * 2, 0), self._discussion_scan_cap()),
            100,
        )
        broad_messages = list(
            client.iter_messages(
                discussion_context.chat,
                limit=scan_limit,
                offset_date=self._end_datetime(),
                reverse=True,
            )
        )
        for message in broad_messages:
            message_id = self._message_id(message)
            if message_id == discussion_context.root_message_id or message_id in messages_by_id:
                continue
            if self._belongs_to_discussion_thread(message, discussion_context.root_message_id):
                messages_by_id[message_id] = message
        ordered = sorted(
            messages_by_id.values(),
            key=lambda item: (
                self._message_datetime(item) or datetime.min.replace(tzinfo=UTC),
                self._message_id(item),
            ),
        )
        if self.config.history.active:
            return ordered[: self.config.history.max_comments_per_post]
        return ordered

    def _source_message_limit(self) -> int:
        if self.config.history.active:
            return max(1, self.config.history.max_items_per_window)
        return self.settings.page_size

    def _discussion_direct_limit(self) -> int:
        if self.config.history.active:
            return max(1, min(self.config.history.max_comments_per_post, self.settings.page_size))
        return self.settings.page_size

    def _discussion_scan_cap(self) -> int:
        if self.config.history.active:
            return max(1, self.config.history.max_comments_per_post)
        return 1000

    def _order_discussion_messages(
        self,
        messages: Iterable[Any],
        *,
        root_message_id: int,
    ) -> list[Any]:
        message_list = list(messages)
        messages_by_id = {self._message_id(message): message for message in message_list}
        ordered: list[Any] = []
        seen: set[int] = set()

        def visit(message: Any) -> None:
            message_id = self._message_id(message)
            if message_id in seen:
                return
            parent_message_id = self._reply_to_parent_message_id(message)
            if parent_message_id is not None and parent_message_id != root_message_id:
                parent_message = messages_by_id.get(parent_message_id)
                if parent_message is not None:
                    visit(parent_message)
            seen.add(message_id)
            ordered.append(message)

        for message in sorted(
            message_list,
            key=lambda item: (
                self._message_datetime(item) or datetime.min.replace(tzinfo=UTC),
                self._message_id(item),
            ),
        ):
            visit(message)
        return ordered

    @classmethod
    def _belongs_to_discussion_thread(cls, message: Any, root_message_id: int) -> bool:
        reply_to = getattr(message, "reply_to", None)
        if reply_to is not None:
            reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None)
            reply_to_top_id = getattr(reply_to, "reply_to_top_id", None)
            if reply_to_msg_id == root_message_id or reply_to_top_id == root_message_id:
                return True
        if isinstance(message, dict):
            nested = message.get("reply_to") or {}
            return nested.get("reply_to_msg_id") == root_message_id or nested.get("reply_to_top_id") == root_message_id
        return False

    def _build_post_snapshot(self, *, message: Any, source_entity: Any, raw_store: RawSnapshotStore) -> PostSnapshot:
        post_id = self._telegram_post_id(source_entity, message)
        payload = self._serialize_object(message)
        raw_path = raw_store.write_json("telegram_posts", slugify(post_id), payload)
        media_refs = self._extract_media_refs(post_id, message)
        reaction_breakdown = self._reaction_breakdown(message)
        source_id = self._stringify(self._entity_id(source_entity)) or self._source_reference()
        replies = getattr(message, "replies", None)
        reply_count = safe_int(getattr(replies, "replies", None))
        propagation_kind, origin_post_id, origin_external_id, origin_permalink = self._propagation_metadata(message)
        return PostSnapshot(
            post_id=post_id,
            platform="telegram",
            source_id=source_id,
            origin_post_id=origin_post_id,
            origin_external_id=origin_external_id,
            origin_permalink=origin_permalink,
            propagation_kind=propagation_kind,
            is_propagation=propagation_kind is not None,
            created_at=self._iso_datetime(self._message_datetime(message)),
            message=self._message_text(message),
            permalink=self._message_permalink(source_entity, message),
            reactions=sum(reaction_breakdown.values()),
            comments_count=reply_count or 0,
            views=safe_int(getattr(message, "views", None)),
            forwards=safe_int(getattr(message, "forwards", None)),
            reply_count=reply_count,
            has_media=bool(media_refs) or bool(getattr(message, "media", None)),
            media_type=self._media_type(message),
            reaction_breakdown_json=json.dumps(reaction_breakdown, ensure_ascii=False) if reaction_breakdown else None,
            source_collector=self.name,
            raw_path=str(raw_path),
            author=AuthorSnapshot(
                author_id=self._stringify(self._entity_id(source_entity)),
                name=self._entity_title(source_entity),
            ),
            media_refs=media_refs,
        )

    def _build_comment_snapshot(
        self,
        *,
        message: Any,
        post_snapshot: PostSnapshot,
        raw_store: RawSnapshotStore,
        message_to_comment_id: dict[str, str],
        comment_depths: dict[str, int],
    ) -> CommentSnapshot:
        comment_id = self._telegram_comment_id(post_snapshot.post_id, message)
        payload = self._serialize_object(message)
        raw_path = raw_store.write_json("telegram_comments", slugify(comment_id), payload)
        reply_to_message_id = self._reply_to_message_id(message)
        parent_message_id = self._reply_to_parent_message_id(message)
        parent_comment_id = (
            message_to_comment_id.get(str(parent_message_id))
            if parent_message_id is not None
            else None
        )
        depth = comment_depths.get(parent_comment_id, -1) + 1 if parent_comment_id else 0

        reaction_breakdown = self._reaction_breakdown(message)
        author = self._author_snapshot(message)
        snapshot = CommentSnapshot(
            comment_id=comment_id,
            platform="telegram",
            parent_post_id=post_snapshot.post_id,
            parent_comment_id=parent_comment_id,
            reply_to_message_id=self._stringify(reply_to_message_id),
            thread_root_post_id=post_snapshot.post_id,
            created_at=self._iso_datetime(self._message_datetime(message)),
            message=self._message_text(message),
            permalink=self._message_permalink_from_post(post_snapshot, message),
            reactions=sum(reaction_breakdown.values()),
            reaction_breakdown_json=json.dumps(reaction_breakdown, ensure_ascii=False) if reaction_breakdown else None,
            source_collector=self.name,
            depth=depth,
            raw_path=str(raw_path),
            author=author,
        )
        message_to_comment_id[str(self._message_id(message))] = snapshot.comment_id
        comment_depths[snapshot.comment_id] = snapshot.depth
        return snapshot

    def _author_snapshot(self, message: Any) -> AuthorSnapshot | None:
        sender = getattr(message, "sender", None)
        if sender is None:
            return None
        return AuthorSnapshot(
            author_id=self._stringify(self._entity_id(sender)),
            name=self._entity_title(sender),
            profile_url=self._entity_url(sender),
        )

    def _extract_media_refs(self, post_id: str, message: Any) -> list[MediaReference]:
        media_type = self._media_type(message)
        if media_type is None:
            return []
        return [
            MediaReference(
                media_id=f"{post_id}:media:1",
                owner_post_id=post_id,
                media_type=media_type,
            )
        ]

    def _propagation_metadata(self, message: Any) -> tuple[str | None, str | None, str | None, str | None]:
        forward_info = getattr(message, "fwd_from", None)
        if forward_info is None and isinstance(message, dict):
            forward_info = message.get("fwd_from")
        if forward_info is None:
            return None, None, None, None

        saved_from_msg_id = getattr(forward_info, "saved_from_msg_id", None)
        if saved_from_msg_id is None and isinstance(forward_info, dict):
            saved_from_msg_id = forward_info.get("saved_from_msg_id")
        peer_id = self._forward_peer_id(forward_info)
        from_name = getattr(forward_info, "from_name", None)
        if from_name is None and isinstance(forward_info, dict):
            from_name = forward_info.get("from_name")

        if peer_id is not None and saved_from_msg_id is not None:
            origin_post_id = f"telegram:{peer_id}:{saved_from_msg_id}"
            origin_external_id = str(saved_from_msg_id)
        else:
            origin_external_id = str(saved_from_msg_id or from_name or self._message_id(message))
            origin_post_id = f"telegram:origin:{origin_external_id}"
        return "forward", origin_post_id, origin_external_id, None

    @staticmethod
    def _forward_peer_id(forward_info: Any) -> str | None:
        for attr in ("saved_from_peer", "from_id"):
            peer = getattr(forward_info, attr, None)
            if peer is None and isinstance(forward_info, dict):
                peer = forward_info.get(attr)
            if peer is None:
                continue
            peer_id = TelegramMtprotoCollector._peer_identifier(peer)
            if peer_id:
                return peer_id
        return None

    @staticmethod
    def _peer_identifier(peer: Any) -> str | None:
        for attr in ("channel_id", "chat_id", "user_id"):
            value = getattr(peer, attr, None)
            if value is None and isinstance(peer, dict):
                value = peer.get(attr)
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _message_id(message: Any) -> int:
        value = getattr(message, "id", None)
        if value is None and isinstance(message, dict):
            value = message.get("id")
        if value is None:
            raise CollectorUnavailableError("Telegram message is missing id.")
        return int(value)

    @staticmethod
    def _message_datetime(message: Any) -> datetime | None:
        value = getattr(message, "date", None)
        if value is None and isinstance(message, dict):
            value = message.get("date")
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return value

    @staticmethod
    def _message_text(message: Any) -> str | None:
        for attr in ("message", "text", "raw_text"):
            value = getattr(message, attr, None)
            if value:
                return str(value)
        if isinstance(message, dict):
            for key in ("message", "text", "raw_text"):
                value = message.get(key)
                if value:
                    return str(value)
        return None

    @staticmethod
    def _reply_to_parent_message_id(message: Any) -> int | None:
        reply_to = getattr(message, "reply_to", None)
        if reply_to is not None:
            value = getattr(reply_to, "reply_to_msg_id", None)
            if value:
                return int(value)
        if isinstance(message, dict):
            nested = message.get("reply_to") or {}
            value = nested.get("reply_to_msg_id")
            if value:
                return int(value)
        return None

    @staticmethod
    def _reply_to_message_id(message: Any) -> int | None:
        parent_message_id = TelegramMtprotoCollector._reply_to_parent_message_id(message)
        if parent_message_id is not None:
            return parent_message_id
        reply_to = getattr(message, "reply_to", None)
        if reply_to is not None:
            value = getattr(reply_to, "reply_to_top_id", None)
            if value:
                return int(value)
        if isinstance(message, dict):
            nested = message.get("reply_to") or {}
            value = nested.get("reply_to_top_id")
            if value:
                return int(value)
        return None

    def _telegram_post_id(self, source_entity: Any, message: Any) -> str:
        return f"telegram:{self._entity_id(source_entity)}:{self._message_id(message)}"

    def _telegram_comment_id(self, post_id: str, message: Any) -> str:
        return f"{post_id}:comment:{self._message_id(message)}"

    def _message_permalink(self, source_entity: Any, message: Any) -> str | None:
        username = getattr(source_entity, "username", None)
        if username:
            return f"https://t.me/{username}/{self._message_id(message)}"
        return None

    def _message_permalink_from_post(self, post_snapshot: PostSnapshot, message: Any) -> str | None:
        if not post_snapshot.permalink:
            return None
        return f"{post_snapshot.permalink}?comment={self._message_id(message)}"

    @staticmethod
    def _media_type(message: Any) -> str | None:
        media = getattr(message, "media", None)
        if media is None and isinstance(message, dict):
            media = message.get("media")
        if media is None:
            return None
        media_type_name = media.__class__.__name__.lower()
        if "photo" in media_type_name:
            return "photo"
        if "document" in media_type_name:
            return "document"
        if "webpage" in media_type_name:
            return "link"
        return media.__class__.__name__

    def _reaction_breakdown(self, message: Any) -> dict[str, int]:
        breakdown: dict[str, int] = {}
        reactions = getattr(message, "reactions", None)
        if reactions is None and isinstance(message, dict):
            reactions = message.get("reactions")
        results = getattr(reactions, "results", None) if reactions is not None else None
        if results is None and isinstance(reactions, dict):
            results = reactions.get("results")
        for item in results or []:
            reaction = getattr(item, "reaction", None)
            emoticon = getattr(reaction, "emoticon", None) if reaction is not None else None
            if emoticon is None and isinstance(item, dict):
                reaction = item.get("reaction")
                if isinstance(reaction, dict):
                    emoticon = reaction.get("emoticon") or reaction.get("emoji")
                else:
                    emoticon = reaction
            key = str(emoticon or "unknown")
            count = getattr(item, "count", None)
            if count is None and isinstance(item, dict):
                count = item.get("count")
            breakdown[key] = int(count or 0)
        return breakdown

    @staticmethod
    def _is_service_message(message: Any) -> bool:
        if getattr(message, "action", None) is not None:
            return True
        if isinstance(message, dict) and message.get("action") is not None:
            return True
        return False

    def _serialize_object(self, value: Any) -> dict[str, Any]:
        if hasattr(value, "to_dict"):
            return value.to_dict()
        if isinstance(value, dict):
            return value
        serialized: dict[str, Any] = {}
        for attr in ("id", "username", "title", "date", "message", "views", "forwards"):
            if hasattr(value, attr):
                serialized[attr] = getattr(value, attr)
        return serialized

    @staticmethod
    def _entity_id(entity: Any) -> str | int | None:
        value = getattr(entity, "id", None)
        if value is None and isinstance(entity, dict):
            value = entity.get("id")
        return value

    @staticmethod
    def _entity_title(entity: Any) -> str | None:
        for attr in ("title", "first_name", "username"):
            value = getattr(entity, attr, None)
            if value:
                return str(value)
        if isinstance(entity, dict):
            for key in ("title", "first_name", "username"):
                value = entity.get(key)
                if value:
                    return str(value)
        return None

    @staticmethod
    def _entity_url(entity: Any) -> str | None:
        username = getattr(entity, "username", None)
        if username:
            return f"https://t.me/{username}"
        if isinstance(entity, dict) and entity.get("username"):
            return f"https://t.me/{entity['username']}"
        return None

    @classmethod
    def _entity_source_type(cls, entity: Any) -> str:
        if cls._entity_flag(entity, "broadcast"):
            return "channel"
        if cls._entity_flag(entity, "megagroup") or cls._entity_flag(entity, "gigagroup") or cls._entity_flag(entity, "forum"):
            return "group"
        if cls._entity_title(entity):
            return "chat"
        return "user"

    @classmethod
    def _entity_reference(cls, entity: Any) -> str | None:
        username = getattr(entity, "username", None)
        if username:
            return str(username)
        if isinstance(entity, dict) and entity.get("username"):
            return str(entity["username"])
        entity_id = cls._entity_id(entity)
        return cls._stringify(entity_id)

    @staticmethod
    def _entity_flag(entity: Any, name: str) -> bool:
        value = getattr(entity, name, None)
        if value is None and isinstance(entity, dict):
            value = entity.get(name)
        return bool(value)

    @staticmethod
    def _stringify(value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _iso_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).replace(microsecond=0).isoformat()

    def _within_range(self, value: datetime | None) -> bool:
        return self.range_filter.contains(value, allow_missing=False)

    def _start_datetime(self) -> datetime | None:
        if not self.config.date_range.start:
            return None
        return parse_configured_datetime(self.config.date_range.start, end_of_day=False)

    def _end_datetime(self) -> datetime | None:
        if not self.config.date_range.end:
            return None
        return parse_configured_datetime(self.config.date_range.end, end_of_day=True)
