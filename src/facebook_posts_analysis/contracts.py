from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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
    parent_post_id: str
    parent_comment_id: str | None = None
    created_at: str | None = None
    message: str | None = None
    permalink: str | None = None
    reactions: int = 0
    source_collector: str
    depth: int = 0
    raw_path: str | None = None
    author: AuthorSnapshot | None = None


class PostSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    post_id: str
    page_id: str
    created_at: str | None = None
    message: str | None = None
    permalink: str | None = None
    reactions: int = 0
    shares: int = 0
    comments_count: int = 0
    source_collector: str
    raw_path: str | None = None
    author: AuthorSnapshot | None = None
    media_refs: list[MediaReference] = Field(default_factory=list)
    comments: list[CommentSnapshot] = Field(default_factory=list)


class PageSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page_id: str
    page_name: str | None = None
    page_url: str | None = None
    about: str | None = None
    followers_count: int | None = None
    fan_count: int | None = None
    source_collector: str
    raw_path: str | None = None


class CollectionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    collected_at: str
    collector: str
    mode: Literal["api", "web", "hybrid"]
    status: Literal["success", "partial", "failed"] = "success"
    fallback_used: bool = False
    warnings: list[str] = Field(default_factory=list)
    cursors: dict[str, str] = Field(default_factory=dict)
    page: PageSnapshot
    posts: list[PostSnapshot] = Field(default_factory=list)


class ClusterSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_type: Literal["post", "comment"]
    cluster_id: str
    label: str
    description: str
    top_keywords: list[str] = Field(default_factory=list)
    exemplar_ids: list[str] = Field(default_factory=list)
    run_id: str


class StanceLabel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_type: Literal["post", "comment"]
    item_id: str
    side_id: str
    label: Literal["support", "oppose", "neutral", "unclear"]
    confidence: float
    model_name: str
    run_id: str

