from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PlatformName = Literal["facebook", "telegram", "x", "threads", "instagram"]
SourceKind = Literal["feed", "person_monitor"]
DiscoveryKind = Literal["watchlist", "search"]
MatchKind = Literal[
    "authored_by_subject",
    "profile_url_mention",
    "profile_id_mention",
    "handle_mention",
    "alias_text_mention",
]
CollectorMode = Literal[
    "api",
    "web",
    "hybrid",
    "mtproto",
    "bot_api",
    "x_api",
    "threads_api",
    "instagram_graph_api",
]
ParentEntityType = Literal["post", "propagation"]


class AuthorSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    author_id: str | None = None
    name: str | None = None
    profile_url: str | None = None


class MediaReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    media_id: str
    owner_post_id: str
    media_type: str | None = None
    title: str | None = None
    url: str | None = None
    preview_url: str | None = None


class CommentSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    comment_id: str
    platform: PlatformName
    source_kind: SourceKind = "feed"
    parent_post_id: str
    parent_entity_type: ParentEntityType | None = None
    parent_entity_id: str | None = None
    parent_comment_id: str | None = None
    reply_to_message_id: str | None = None
    thread_root_post_id: str | None = None
    origin_post_id: str | None = None
    container_source_id: str | None = None
    container_source_name: str | None = None
    container_source_url: str | None = None
    container_source_type: str | None = None
    discovery_kind: DiscoveryKind | None = None
    created_at: str | None = None
    message: str | None = None
    raw_text: str | None = None
    permalink: str | None = None
    reactions: int = 0
    reaction_breakdown_json: str | None = None
    source_collector: str
    depth: int = 0
    raw_path: str | None = None
    author: AuthorSnapshot | None = None


class PostSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    post_id: str
    platform: PlatformName
    source_id: str
    source_kind: SourceKind = "feed"
    origin_post_id: str | None = None
    origin_external_id: str | None = None
    origin_permalink: str | None = None
    propagation_kind: str | None = None
    is_propagation: bool = False
    container_source_id: str | None = None
    container_source_name: str | None = None
    container_source_url: str | None = None
    container_source_type: str | None = None
    discovery_kind: DiscoveryKind | None = None
    created_at: str | None = None
    message: str | None = None
    raw_text: str | None = None
    permalink: str | None = None
    reactions: int = 0
    shares: int = 0
    comments_count: int = 0
    views: int | None = None
    forwards: int | None = None
    reply_count: int | None = None
    has_media: bool = False
    media_type: str | None = None
    reaction_breakdown_json: str | None = None
    source_collector: str
    raw_path: str | None = None
    author: AuthorSnapshot | None = None
    media_refs: list[MediaReference] = Field(default_factory=list)
    comments: list[CommentSnapshot] = Field(default_factory=list)


class PropagationSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    propagation_id: str
    platform: PlatformName
    source_id: str
    source_kind: SourceKind = "feed"
    origin_post_id: str | None = None
    origin_external_id: str | None = None
    origin_permalink: str | None = None
    propagation_kind: str
    container_source_id: str | None = None
    container_source_name: str | None = None
    container_source_url: str | None = None
    container_source_type: str | None = None
    discovery_kind: DiscoveryKind | None = None
    created_at: str | None = None
    message: str | None = None
    raw_text: str | None = None
    permalink: str | None = None
    reactions: int = 0
    shares: int = 0
    comments_count: int = 0
    views: int | None = None
    forwards: int | None = None
    reply_count: int | None = None
    has_media: bool = False
    media_type: str | None = None
    reaction_breakdown_json: str | None = None
    source_collector: str
    raw_path: str | None = None
    author: AuthorSnapshot | None = None


class SourceSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: PlatformName
    source_id: str
    source_kind: SourceKind = "feed"
    source_name: str | None = None
    source_url: str | None = None
    source_type: str | None = None
    about: str | None = None
    followers_count: int | None = None
    fan_count: int | None = None
    discussion_chat_id: str | None = None
    discussion_chat_name: str | None = None
    discussion_linked: bool | None = None
    filtered_service_message_count: int = 0
    source_collector: str
    raw_path: str | None = None


class ObservedSourceSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    container_source_id: str
    container_source_name: str | None = None
    container_source_url: str | None = None
    container_source_type: str | None = None
    discovery_kind: DiscoveryKind
    platform: PlatformName
    status: Literal["success", "partial", "failed", "unsupported"] = "success"
    warning_count: int = 0
    source_collector: str | None = None
    raw_path: str | None = None


class MatchHitSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match_id: str
    item_type: Literal["post", "comment", "propagation"]
    item_id: str
    match_kind: MatchKind
    matched_value: str
    platform: PlatformName
    container_source_id: str | None = None


class CollectionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    collected_at: str
    requested_date_start: str | None = None
    requested_date_end: str | None = None
    collector: str
    mode: CollectorMode
    status: Literal["success", "partial", "failed"] = "success"
    fallback_used: bool = False
    request_signature: str | None = None
    warnings: list[str] = Field(default_factory=list)
    cursors: dict[str, str] = Field(default_factory=dict)
    source: SourceSnapshot
    posts: list[PostSnapshot] = Field(default_factory=list)
    observed_sources: list[ObservedSourceSnapshot] = Field(default_factory=list)
    match_hits: list[MatchHitSnapshot] = Field(default_factory=list)


class ClusterSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_type: Literal["post", "comment", "propagation"]
    cluster_id: str
    label: str
    description: str
    top_keywords: list[str] = Field(default_factory=list)
    exemplar_ids: list[str] = Field(default_factory=list)
    run_id: str


class StanceLabel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_type: Literal["post", "comment", "propagation"]
    item_id: str
    side_id: str
    label: Literal["support", "oppose", "neutral", "unclear"]
    confidence: float
    model_name: str
    run_id: str
