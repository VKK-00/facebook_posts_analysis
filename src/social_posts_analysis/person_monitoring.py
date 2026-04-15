from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

from social_posts_analysis.collectors.base import BaseCollector, CollectorUnavailableError
from social_posts_analysis.config import ProjectConfig, SourceConfig, WatchlistSourceConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    DiscoveryKind,
    MatchHitSnapshot,
    MatchKind,
    ObservedSourceSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import slugify, stable_id, utc_now_iso


def build_request_signature(config: ProjectConfig) -> str:
    source = config.source
    payload: dict[str, Any] = {
        "kind": source.kind,
        "platform": source.platform,
        "source_id": source.source_id or "",
        "source_name": source.source_name or "",
        "url": source.url or "",
        "mode": config.collector.mode,
        "date_start": config.date_range.start or "",
        "date_end": config.date_range.end or "",
    }
    if source.kind == "person_monitor":
        payload["aliases"] = sorted(item.strip() for item in source.aliases if item.strip())
        payload["watchlist"] = [normalize_watchlist_item(item) for item in source.watchlist]
        payload["search"] = {
            "enabled": source.search.enabled,
            "include_posts": source.search.include_posts,
            "include_comments": source.search.include_comments,
            "max_items_per_query": source.search.max_items_per_query,
            "queries": auto_search_queries(source),
        }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def request_signature_from_manifest(manifest: CollectionManifest) -> str:
    if manifest.request_signature:
        return manifest.request_signature
    source = manifest.source
    payload = {
        "kind": source.source_kind,
        "platform": source.platform,
        "source_id": source.source_id or "",
        "source_name": source.source_name or "",
        "url": source.source_url or "",
        "mode": manifest.mode,
        "date_start": manifest.requested_date_start or "",
        "date_end": manifest.requested_date_end or "",
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def auto_search_queries(source: SourceConfig) -> list[str]:
    if source.search.queries is not None:
        return sorted(dict.fromkeys(item.strip() for item in source.search.queries if item and item.strip()))

    values: list[str] = []
    for candidate in (source.url, source.source_id, source.source_name, *source.aliases):
        if candidate:
            stripped = candidate.strip()
            if stripped:
                values.append(stripped)
    handle = infer_profile_handle(source)
    if handle:
        values.extend([f"@{handle}", handle])
    return sorted(dict.fromkeys(values))


def normalize_watchlist_item(item: WatchlistSourceConfig) -> dict[str, str]:
    return {
        "url": item.url or "",
        "source_id": item.source_id or "",
        "source_name": item.source_name or "",
        "source_type": item.source_type or "",
    }


def infer_profile_handle(source: SourceConfig) -> str | None:
    for candidate in (source.url, source.source_id):
        parsed = extract_handle_candidate(candidate)
        if parsed:
            return parsed
    return None


def extract_handle_candidate(value: str | None) -> str | None:
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.startswith("@"):
        stripped = stripped[1:]
    if stripped.startswith("http://") or stripped.startswith("https://"):
        parsed = urlparse(stripped)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            return None
        stripped = parts[0].lstrip("@")
    if re.fullmatch(r"[A-Za-z0-9_.-]{2,}", stripped):
        return stripped
    return None


def token_bounded_contains(text: str, candidate: str) -> bool:
    if not text or not candidate:
        return False
    pattern = re.compile(rf"(?<![\w]){re.escape(candidate)}(?![\w])", flags=re.IGNORECASE)
    return bool(pattern.search(text))


@dataclass(frozen=True)
class MatchSignals:
    profile_url: str | None
    profile_id: str | None
    source_name: str | None
    aliases: tuple[str, ...]
    handle: str | None


@dataclass(frozen=True)
class DiscoverySource:
    source_id: str
    source_name: str | None
    source_url: str | None
    source_type: str | None
    discovery_kind: DiscoveryKind


def build_match_signals(source: SourceConfig) -> MatchSignals:
    aliases = tuple(sorted(dict.fromkeys(item.strip() for item in source.aliases if item.strip())))
    return MatchSignals(
        profile_url=(source.url or "").strip() or None,
        profile_id=(source.source_id or "").strip() or None,
        source_name=(source.source_name or "").strip() or None,
        aliases=aliases,
        handle=infer_profile_handle(source),
    )


class PersonMonitorOrchestrator:
    def __init__(
        self,
        config: ProjectConfig,
        collector_builder: Callable[[ProjectConfig], list[BaseCollector]],
    ) -> None:
        self.config = config
        self.collector_builder = collector_builder
        self.signals = build_match_signals(config.source)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        warnings: list[str] = []
        cursors: dict[str, str] = {}
        observed_sources: list[ObservedSourceSnapshot] = []
        collected_posts: dict[str, PostSnapshot] = {}
        match_hits: dict[str, MatchHitSnapshot] = {}
        any_fallback_used = False

        discovery_sources = self._watchlist_sources()
        search_sources, search_rows, search_warnings = self._discover_search_sources()
        discovery_sources = self._merge_discovery_sources(discovery_sources, search_sources)
        observed_sources.extend(search_rows)
        warnings.extend(search_warnings)

        for discovery_source in discovery_sources:
            surface_manifest = self._collect_surface_manifest(run_id, raw_store, discovery_source)
            observed_sources.append(surface_manifest["observed_source"])
            warnings.extend(surface_manifest["warnings"])
            cursors.update(surface_manifest["cursors"])
            any_fallback_used = any_fallback_used or surface_manifest["fallback_used"]
            if surface_manifest["manifest"] is None:
                continue
            filtered_posts, filtered_hits = self._filter_manifest_items(
                surface_manifest["manifest"],
                discovery_source=discovery_source,
            )
            for post in filtered_posts:
                key = self._item_identity(post.post_id, post.permalink)
                existing = collected_posts.get(key)
                collected_posts[key] = self._merge_monitor_post(existing, post)
            for hit in filtered_hits:
                match_hits[hit.match_id] = hit

        status: Literal["success", "partial", "failed"] = "success"
        if warnings or any(row.status != "success" for row in observed_sources):
            status = "partial"
        if not observed_sources and not collected_posts:
            status = "failed"

        root_source = self._root_source_snapshot()
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            requested_date_start=self.config.date_range.start,
            requested_date_end=self.config.date_range.end,
            collector="person_monitor",
            mode=self.config.collector.mode,
            status=status,
            fallback_used=any_fallback_used,
            request_signature=build_request_signature(self.config),
            warnings=list(dict.fromkeys(warnings)),
            cursors=cursors,
            source=root_source,
            posts=self._sort_posts(list(collected_posts.values())),
            observed_sources=observed_sources,
            match_hits=list(match_hits.values()),
        )

    def _root_source_snapshot(self) -> SourceSnapshot:
        return SourceSnapshot(
            platform=self.config.source.platform,
            source_id=self._root_source_id(),
            source_kind="person_monitor",
            source_name=self.config.source.source_name,
            source_url=self.config.source.url,
            source_type="profile",
            source_collector="person_monitor",
        )

    def _root_source_id(self) -> str:
        if self.config.source.source_id:
            return self.config.source.source_id
        return f"{self.config.source.platform}:monitor:{stable_id(self.config.source.url or '', self.config.source.source_name or '')}"

    def _watchlist_sources(self) -> list[DiscoverySource]:
        rows: list[DiscoverySource] = []
        for item in self.config.source.watchlist:
            rows.append(
                DiscoverySource(
                    source_id=item.source_id
                    or item.url
                    or item.source_name
                    or f"{self.config.source.platform}:watch:{stable_id(json.dumps(normalize_watchlist_item(item), ensure_ascii=False))}",
                    source_name=item.source_name,
                    source_url=item.url,
                    source_type=item.source_type,
                    discovery_kind="watchlist",
                )
            )
        return rows

    def _discover_search_sources(self) -> tuple[list[DiscoverySource], list[ObservedSourceSnapshot], list[str]]:
        if not self.config.source.search.enabled:
            return [], [], []
        if self.config.source.platform == "x" and self.config.collector.mode == "x_api":
            return self._discover_x_api_sources()
        warnings: list[str] = []
        observed_rows: list[ObservedSourceSnapshot] = []
        search_warning = (
            f"Search discovery is not supported for {self.config.source.platform} "
            f"with collector.mode='{self.config.collector.mode}'; continuing with watchlist only."
        )
        warnings.append(search_warning)
        for query in auto_search_queries(self.config.source):
            observed_rows.append(
                ObservedSourceSnapshot(
                    container_source_id=f"search-query:{stable_id(query)}",
                    container_source_name=query,
                    container_source_type="search_query",
                    discovery_kind="search",
                    platform=self.config.source.platform,
                    status="unsupported",
                    warning_count=1,
                    source_collector="person_monitor",
                )
            )
        return [], observed_rows, warnings

    def _discover_x_api_sources(self) -> tuple[list[DiscoverySource], list[ObservedSourceSnapshot], list[str]]:
        from social_posts_analysis.collectors.x_api import XApiCollector

        warnings: list[str] = []
        try:
            collector = XApiCollector(self.config)
            payloads = collector.discover_person_monitor_sources(
                queries=auto_search_queries(self.config.source),
                include_posts=self.config.source.search.include_posts,
                include_comments=self.config.source.search.include_comments,
                max_items_per_query=self.config.source.search.max_items_per_query,
            )
        except CollectorUnavailableError as exc:
            warning = f"X API search discovery is unavailable: {exc}"
            return [], [], [warning]

        discovery_sources: list[DiscoverySource] = []
        for payload in payloads:
            if self._is_monitored_surface(payload):
                continue
            source_id = (payload.get("source_id") or "").strip()
            if not source_id:
                continue
            discovery_sources.append(
                DiscoverySource(
                    source_id=source_id,
                    source_name=(payload.get("source_name") or None),
                    source_url=(payload.get("source_url") or None),
                    source_type=(payload.get("source_type") or "account"),
                    discovery_kind="search",
                )
            )
        if not discovery_sources:
            warnings.append("X API search discovery completed but found no external surfaces for the configured queries.")
        return discovery_sources, [], warnings

    def _collect_surface_manifest(
        self,
        run_id: str,
        raw_store: RawSnapshotStore,
        discovery_source: DiscoverySource,
    ) -> dict[str, Any]:
        surface_key = slugify(f"{discovery_source.discovery_kind}-{discovery_source.source_id}")
        surface_store = RawSnapshotStore(raw_store.run_dir / "person_monitor_surfaces" / surface_key)
        surface_config = self.config.model_copy(deep=True)
        surface_config.source.kind = "feed"
        surface_config.source.url = discovery_source.source_url
        surface_config.source.source_id = discovery_source.source_id
        surface_config.source.source_name = discovery_source.source_name
        surface_config.source.aliases = []
        surface_config.source.watchlist = []
        surface_config.source.search.enabled = False
        surface_config.source.search.queries = None

        warnings: list[str] = []
        cursors: dict[str, str] = {}
        collectors = self.collector_builder(surface_config)
        if not collectors:
            warning = (
                f"No supported collectors are available for person_monitor surface "
                f"{discovery_source.source_id} on {self.config.source.platform}."
            )
            return {
                "manifest": None,
                "warnings": [warning],
                "cursors": {},
                "fallback_used": False,
                "observed_source": ObservedSourceSnapshot(
                    container_source_id=discovery_source.source_id,
                    container_source_name=discovery_source.source_name,
                    container_source_url=discovery_source.source_url,
                    container_source_type=discovery_source.source_type,
                    discovery_kind=discovery_source.discovery_kind,
                    platform=self.config.source.platform,
                    status="failed",
                    warning_count=1,
                    source_collector="person_monitor",
                    raw_path=str(surface_store.run_dir / "manifest.json"),
                ),
            }

        manifest: CollectionManifest | None = None
        fallback_used = False
        for index, collector in enumerate(collectors):
            try:
                current_manifest = collector.collect(run_id, surface_store)
                current_manifest = current_manifest.model_copy(
                    update={
                        "requested_date_start": self.config.date_range.start,
                        "requested_date_end": self.config.date_range.end,
                    }
                )
                current_manifest.warnings = [*warnings, *current_manifest.warnings]
                current_manifest.fallback_used = index > 0
                manifest = current_manifest
                fallback_used = current_manifest.fallback_used
                cursors = {
                    f"{surface_key}:{cursor_key}": cursor_value
                    for cursor_key, cursor_value in current_manifest.cursors.items()
                }
                (surface_store.run_dir / "manifest.json").write_text(
                    current_manifest.model_dump_json(indent=2),
                    encoding="utf-8",
                )
                break
            except CollectorUnavailableError as exc:
                warnings.append(f"{collector.__class__.__name__}: {exc}")
            except Exception as exc:  # pragma: no cover
                warnings.append(f"{collector.__class__.__name__}: {exc}")

        status = manifest.status if manifest is not None else "failed"
        surface_source = manifest.source if manifest is not None else None
        observed_source = ObservedSourceSnapshot(
            container_source_id=(surface_source.source_id if surface_source else discovery_source.source_id),
            container_source_name=(surface_source.source_name if surface_source else discovery_source.source_name),
            container_source_url=(surface_source.source_url if surface_source else discovery_source.source_url),
            container_source_type=(surface_source.source_type if surface_source else discovery_source.source_type),
            discovery_kind=discovery_source.discovery_kind,
            platform=self.config.source.platform,
            status=status if status in {"success", "partial", "failed"} else "failed",
            warning_count=len(manifest.warnings if manifest is not None else warnings),
            source_collector=(manifest.collector if manifest is not None else "person_monitor"),
            raw_path=str(surface_store.run_dir / "manifest.json"),
        )
        return {
            "manifest": manifest,
            "warnings": manifest.warnings if manifest is not None else warnings,
            "cursors": cursors,
            "fallback_used": fallback_used,
            "observed_source": observed_source,
        }

    def _filter_manifest_items(
        self,
        manifest: CollectionManifest,
        *,
        discovery_source: DiscoverySource,
    ) -> tuple[list[PostSnapshot], list[MatchHitSnapshot]]:
        filtered_posts: list[PostSnapshot] = []
        filtered_hits: list[MatchHitSnapshot] = []
        for post in manifest.posts:
            post_hits = self._match_post(post, manifest.source)
            matched_comments = []
            for comment in post.comments:
                comment_hits = self._match_comment(comment, manifest.source)
                if comment_hits:
                    matched_comments.append(self._stamp_comment(comment, manifest.source, discovery_source))
                    filtered_hits.extend(comment_hits)
            if not post_hits and not matched_comments:
                continue
            filtered_hits.extend(post_hits)
            filtered_posts.append(
                self._stamp_post(
                    post,
                    manifest.source,
                    discovery_source,
                    matched_comments,
                )
            )
        return filtered_posts, filtered_hits

    def _stamp_post(
        self,
        post: PostSnapshot,
        container_source: SourceSnapshot,
        discovery_source: DiscoverySource,
        matched_comments: list[CommentSnapshot],
    ) -> PostSnapshot:
        return post.model_copy(
            update={
                "source_id": self._root_source_id(),
                "source_kind": "person_monitor",
                "container_source_id": container_source.source_id,
                "container_source_name": container_source.source_name,
                "container_source_url": container_source.source_url,
                "container_source_type": container_source.source_type or discovery_source.source_type,
                "discovery_kind": discovery_source.discovery_kind,
                "comments": matched_comments,
            }
        )

    def _stamp_comment(
        self,
        comment: CommentSnapshot,
        container_source: SourceSnapshot,
        discovery_source: DiscoverySource,
    ) -> CommentSnapshot:
        return comment.model_copy(
            update={
                "source_kind": "person_monitor",
                "container_source_id": container_source.source_id,
                "container_source_name": container_source.source_name,
                "container_source_url": container_source.source_url,
                "container_source_type": container_source.source_type or discovery_source.source_type,
                "discovery_kind": discovery_source.discovery_kind,
            }
        )

    def _match_post(self, post: PostSnapshot, container_source: SourceSnapshot) -> list[MatchHitSnapshot]:
        item_type: Literal["post", "propagation"] = "propagation" if post.is_propagation else "post"
        return self._match_item(
            item_type=item_type,
            item_id=post.post_id,
            author=post.author,
            text_candidates=[post.message, post.raw_text, post.permalink, post.origin_permalink],
            container_source=container_source,
        )

    def _match_comment(self, comment: CommentSnapshot, container_source: SourceSnapshot) -> list[MatchHitSnapshot]:
        return self._match_item(
            item_type="comment",
            item_id=comment.comment_id,
            author=comment.author,
            text_candidates=[comment.message, comment.raw_text, comment.permalink],
            container_source=container_source,
        )

    def _match_item(
        self,
        *,
        item_type: Literal["post", "comment", "propagation"],
        item_id: str,
        author: AuthorSnapshot | None,
        text_candidates: list[str | None],
        container_source: SourceSnapshot,
    ) -> list[MatchHitSnapshot]:
        blob = "\n".join(candidate for candidate in text_candidates if candidate)
        hits: list[MatchHitSnapshot] = []
        authored_value = self._authored_match_value(author)
        if authored_value:
            hits.append(
                self._build_match_hit(
                    item_type=item_type,
                    item_id=item_id,
                    match_kind="authored_by_subject",
                    matched_value=authored_value,
                    container_source_id=container_source.source_id,
                )
            )
        if self.signals.profile_url and self._contains_profile_url(blob):
            hits.append(
                self._build_match_hit(
                    item_type=item_type,
                    item_id=item_id,
                    match_kind="profile_url_mention",
                    matched_value=self.signals.profile_url,
                    container_source_id=container_source.source_id,
                )
            )
        if self.signals.profile_id and token_bounded_contains(blob, self.signals.profile_id):
            hits.append(
                self._build_match_hit(
                    item_type=item_type,
                    item_id=item_id,
                    match_kind="profile_id_mention",
                    matched_value=self.signals.profile_id,
                    container_source_id=container_source.source_id,
                )
            )
        if self.signals.handle and (
            token_bounded_contains(blob, f"@{self.signals.handle}")
            or token_bounded_contains(blob, self.signals.handle)
        ):
            hits.append(
                self._build_match_hit(
                    item_type=item_type,
                    item_id=item_id,
                    match_kind="handle_mention",
                    matched_value=self.signals.handle,
                    container_source_id=container_source.source_id,
                )
            )
        for alias in self._alias_candidates():
            if token_bounded_contains(blob, alias):
                hits.append(
                    self._build_match_hit(
                        item_type=item_type,
                        item_id=item_id,
                        match_kind="alias_text_mention",
                        matched_value=alias,
                        container_source_id=container_source.source_id,
                    )
                )
        deduped: dict[str, MatchHitSnapshot] = {}
        for hit in hits:
            deduped[hit.match_id] = hit
        return list(deduped.values())

    def _build_match_hit(
        self,
        *,
        item_type: Literal["post", "comment", "propagation"],
        item_id: str,
        match_kind: MatchKind,
        matched_value: str,
        container_source_id: str,
    ) -> MatchHitSnapshot:
        return MatchHitSnapshot(
            match_id=stable_id(item_type, item_id, match_kind, matched_value, container_source_id),
            item_type=item_type,
            item_id=item_id,
            match_kind=match_kind,
            matched_value=matched_value,
            platform=self.config.source.platform,
            container_source_id=container_source_id,
        )

    def _authored_match_value(self, author: AuthorSnapshot | None) -> str | None:
        if author is None:
            return None
        if self.signals.profile_id and author.author_id and author.author_id == self.signals.profile_id:
            return author.author_id
        if self.signals.profile_url and normalize_profile_url(author.profile_url) == normalize_profile_url(self.signals.profile_url):
            return self.signals.profile_url
        author_name = (author.name or "").strip()
        if author_name and any(author_name.casefold() == candidate.casefold() for candidate in self._alias_candidates()):
            return author_name
        return None

    def _alias_candidates(self) -> tuple[str, ...]:
        values = [self.signals.source_name, *self.signals.aliases]
        return tuple(sorted(dict.fromkeys(value.strip() for value in values if value and value.strip())))

    def _contains_profile_url(self, blob: str) -> bool:
        normalized_profile_url = normalize_profile_url(self.signals.profile_url)
        if not normalized_profile_url:
            return False
        normalized_blob = blob.casefold()
        candidates = {normalized_profile_url, normalized_profile_url.rstrip("/")}
        return any(candidate and candidate in normalized_blob for candidate in candidates)

    @staticmethod
    def _item_identity(item_id: str | None, permalink: str | None) -> str:
        return item_id or permalink or stable_id(permalink or "")

    def _merge_discovery_sources(
        self,
        watchlist_sources: list[DiscoverySource],
        search_sources: list[DiscoverySource],
    ) -> list[DiscoverySource]:
        merged: dict[str, DiscoverySource] = {}
        for discovery_source in [*watchlist_sources, *search_sources]:
            key = self._surface_identity(discovery_source)
            existing = merged.get(key)
            if existing is None:
                merged[key] = discovery_source
                continue
            if existing.discovery_kind == "watchlist":
                continue
            if discovery_source.discovery_kind == "watchlist":
                merged[key] = discovery_source
        return list(merged.values())

    @staticmethod
    def _surface_identity(discovery_source: DiscoverySource) -> str:
        return (
            discovery_source.source_id
            or normalize_profile_url(discovery_source.source_url)
            or (discovery_source.source_name or "").casefold()
        )

    def _is_monitored_surface(self, payload: dict[str, str | None]) -> bool:
        source_id = (payload.get("source_id") or "").strip()
        source_url = normalize_profile_url(payload.get("source_url"))
        source_name = (payload.get("source_name") or "").strip().casefold()
        monitored_name_candidates = {candidate.casefold() for candidate in self._alias_candidates()}
        if self.signals.profile_id and source_id and source_id == self.signals.profile_id:
            return True
        if self.signals.profile_url and source_url and source_url == normalize_profile_url(self.signals.profile_url):
            return True
        return bool(source_name and source_name in monitored_name_candidates)

    def _merge_monitor_post(self, existing: PostSnapshot | None, incoming: PostSnapshot) -> PostSnapshot:
        if existing is None:
            return incoming
        merged_comments: dict[str, CommentSnapshot] = {comment.comment_id: comment for comment in existing.comments}
        for comment in incoming.comments:
            merged_comments[comment.comment_id] = self._merge_monitor_comment(merged_comments.get(comment.comment_id), comment)
        preferred_discovery_kind = existing.discovery_kind if existing.discovery_kind == "watchlist" else incoming.discovery_kind or existing.discovery_kind
        return existing.model_copy(
            update={
                "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
                "raw_text": incoming.raw_text if len(incoming.raw_text or "") > len(existing.raw_text or "") else existing.raw_text,
                "permalink": existing.permalink or incoming.permalink,
                "comments_count": max(existing.comments_count, incoming.comments_count, len(merged_comments)),
                "reactions": max(existing.reactions, incoming.reactions),
                "shares": max(existing.shares, incoming.shares),
                "views": _prefer_numeric_max(existing.views, incoming.views),
                "forwards": _prefer_numeric_max(existing.forwards, incoming.forwards),
                "reply_count": _prefer_numeric_max(existing.reply_count, incoming.reply_count),
                "has_media": existing.has_media or incoming.has_media,
                "media_type": existing.media_type or incoming.media_type,
                "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
                "author": existing.author or incoming.author,
                "discovery_kind": preferred_discovery_kind,
                "comments": sorted(
                    merged_comments.values(),
                    key=lambda comment: (comment.depth, comment.created_at or "", comment.comment_id),
                ),
            }
        )

    @staticmethod
    def _merge_monitor_comment(existing: CommentSnapshot | None, incoming: CommentSnapshot) -> CommentSnapshot:
        if existing is None:
            return incoming
        preferred_discovery_kind = existing.discovery_kind if existing.discovery_kind == "watchlist" else incoming.discovery_kind or existing.discovery_kind
        return existing.model_copy(
            update={
                "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
                "raw_text": incoming.raw_text if len(incoming.raw_text or "") > len(existing.raw_text or "") else existing.raw_text,
                "permalink": existing.permalink or incoming.permalink,
                "reactions": max(existing.reactions, incoming.reactions),
                "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
                "author": existing.author or incoming.author,
                "discovery_kind": preferred_discovery_kind,
            }
        )

    @staticmethod
    def _sort_posts(posts: list[PostSnapshot]) -> list[PostSnapshot]:
        return sorted(posts, key=lambda post: (post.created_at or "", post.post_id), reverse=True)


def normalize_profile_url(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.strip().casefold()
    if "://" not in normalized:
        return normalized
    parsed = urlparse(normalized)
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _prefer_numeric_max(existing: int | None, incoming: int | None) -> int | None:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    return max(existing, incoming)
