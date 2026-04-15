from __future__ import annotations

from typing import Any, Literal

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    CollectionManifest,
    CommentSnapshot,
    MatchHitSnapshot,
    ObservedSourceSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.person_monitoring import request_signature_from_manifest
from social_posts_analysis.utils import read_json


def validate_source_run_ids(paths: ProjectPaths, run_ids: list[str]) -> list[str]:
    available = set(paths.list_run_ids())
    missing = [run_id for run_id in run_ids if run_id not in available]
    if missing:
        raise RuntimeError(f"Configured source_run_ids are missing raw snapshots: {', '.join(missing)}")
    return run_ids


def resolve_source_run_ids(config: ProjectConfig, paths: ProjectPaths, resolved_run_id: str) -> list[str]:
    configured = [run_id for run_id in config.normalization.source_run_ids if run_id]
    if configured:
        return validate_source_run_ids(paths, configured)

    available_run_ids = paths.list_run_ids()
    if resolved_run_id not in available_run_ids:
        raise RuntimeError(f"Run {resolved_run_id} does not exist in raw snapshots.")
    target_manifest = load_manifest(paths, resolved_run_id)
    target_key = manifest_merge_key(target_manifest)
    compatible_run_ids = [
        run_id
        for run_id in available_run_ids
        if manifest_merge_key(load_manifest(paths, run_id)) == target_key
    ]
    target_index = compatible_run_ids.index(resolved_run_id)
    merge_recent_runs = max(1, config.normalization.merge_recent_runs)
    return validate_source_run_ids(
        paths,
        compatible_run_ids[max(0, target_index - merge_recent_runs + 1) : target_index + 1],
    )


def load_manifests(paths: ProjectPaths, run_ids: list[str]) -> list[CollectionManifest]:
    return [load_manifest(paths, run_id) for run_id in run_ids]


def load_manifest(paths: ProjectPaths, run_id: str) -> CollectionManifest:
    manifest_path = paths.run_raw_dir(run_id) / "manifest.json"
    return CollectionManifest.model_validate(read_json(manifest_path))


def manifest_merge_key(manifest: CollectionManifest) -> tuple[str, str, str]:
    source = manifest.source
    return (
        source.platform,
        source.source_kind,
        request_signature_from_manifest(manifest),
    )


def merge_manifests(output_run_id: str, manifests: list[CollectionManifest]) -> CollectionManifest:
    merged_posts: dict[str, PostSnapshot] = {}
    for manifest in manifests:
        for post in manifest.posts:
            merged_posts[post.post_id] = merge_post_snapshots(merged_posts.get(post.post_id), post)

    latest_manifest = manifests[-1]
    source = merge_source_snapshots([manifest.source for manifest in manifests])
    warnings = list(dict.fromkeys(warning for manifest in manifests for warning in manifest.warnings))
    merged_observed_sources = merge_observed_sources(manifests)
    merged_match_hits = merge_match_hits(manifests)
    if len(manifests) > 1:
        warnings.append(f"Merged normalized snapshot from {len(manifests)} collection runs.")

    status: Literal["success", "partial", "failed"] = (
        "partial" if warnings or any(manifest.status != "success" for manifest in manifests) else "success"
    )
    posts = sorted(
        merged_posts.values(),
        key=lambda post: (post.created_at or "", post.post_id),
        reverse=True,
    )
    return CollectionManifest(
        run_id=output_run_id,
        collected_at=latest_manifest.collected_at,
        requested_date_start=latest_manifest.requested_date_start,
        requested_date_end=latest_manifest.requested_date_end,
        collector=latest_manifest.collector,
        mode=latest_manifest.mode,
        status=status,
        fallback_used=any(manifest.fallback_used for manifest in manifests),
        request_signature=request_signature_from_manifest(latest_manifest),
        warnings=warnings,
        cursors=latest_manifest.cursors,
        source=source,
        posts=posts,
        observed_sources=merged_observed_sources,
        match_hits=merged_match_hits,
    )


def merge_source_snapshots(sources: list[SourceSnapshot]) -> SourceSnapshot:
    latest_source = sources[-1]
    merged = latest_source.model_copy(deep=True)
    for source in reversed(sources[:-1]):
        merged = merged.model_copy(
            update={
                "source_kind": merged.source_kind or source.source_kind,
                "source_name": merged.source_name or source.source_name,
                "source_url": merged.source_url or source.source_url,
                "source_type": merged.source_type or source.source_type,
                "about": merged.about or source.about,
                "followers_count": merged.followers_count or source.followers_count,
                "fan_count": merged.fan_count or source.fan_count,
                "discussion_chat_id": merged.discussion_chat_id or source.discussion_chat_id,
                "discussion_chat_name": merged.discussion_chat_name or source.discussion_chat_name,
                "discussion_linked": merged.discussion_linked if merged.discussion_linked is not None else source.discussion_linked,
                "filtered_service_message_count": max(
                    merged.filtered_service_message_count,
                    source.filtered_service_message_count,
                ),
                "raw_path": merged.raw_path or source.raw_path,
            }
        )
    return merged


def merge_post_snapshots(existing: PostSnapshot | None, incoming: PostSnapshot) -> PostSnapshot:
    if existing is None:
        return incoming.model_copy(deep=True)

    merged_comments: dict[str, CommentSnapshot] = {comment.comment_id: comment for comment in existing.comments}
    merged_media_refs = {media.media_id: media for media in existing.media_refs}
    for media in incoming.media_refs:
        merged_media_refs[media.media_id] = media
    for comment in incoming.comments:
        merged_comments[comment.comment_id] = merge_comment_snapshots(merged_comments.get(comment.comment_id), comment)

    return existing.model_copy(
        update={
            "source_kind": existing.source_kind or incoming.source_kind,
            "created_at": existing.created_at or incoming.created_at,
            "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
            "raw_text": incoming.raw_text if len(incoming.raw_text or "") > len(existing.raw_text or "") else existing.raw_text,
            "permalink": existing.permalink or incoming.permalink,
            "origin_post_id": existing.origin_post_id or incoming.origin_post_id,
            "origin_external_id": existing.origin_external_id or incoming.origin_external_id,
            "origin_permalink": existing.origin_permalink or incoming.origin_permalink,
            "propagation_kind": existing.propagation_kind or incoming.propagation_kind,
            "is_propagation": existing.is_propagation or incoming.is_propagation,
            "container_source_id": existing.container_source_id or incoming.container_source_id,
            "container_source_name": existing.container_source_name or incoming.container_source_name,
            "container_source_url": existing.container_source_url or incoming.container_source_url,
            "container_source_type": existing.container_source_type or incoming.container_source_type,
            "discovery_kind": existing.discovery_kind or incoming.discovery_kind,
            "reactions": max(existing.reactions, incoming.reactions),
            "shares": max(existing.shares, incoming.shares),
            "comments_count": max(existing.comments_count, incoming.comments_count, len(merged_comments)),
            "views": prefer_numeric_max(existing.views, incoming.views),
            "forwards": prefer_numeric_max(existing.forwards, incoming.forwards),
            "reply_count": prefer_numeric_max(existing.reply_count, incoming.reply_count),
            "has_media": existing.has_media or incoming.has_media,
            "media_type": existing.media_type or incoming.media_type,
            "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
            "source_collector": incoming.source_collector or existing.source_collector,
            "raw_path": incoming.raw_path or existing.raw_path,
            "author": select_author(existing.author, incoming.author),
            "media_refs": list(merged_media_refs.values()),
            "comments": sort_comments(list(merged_comments.values())),
        }
    )


def merge_comment_snapshots(existing: CommentSnapshot | None, incoming: CommentSnapshot) -> CommentSnapshot:
    if existing is None:
        return incoming.model_copy(deep=True)
    return existing.model_copy(
        update={
            "source_kind": existing.source_kind or incoming.source_kind,
            "parent_entity_type": existing.parent_entity_type or incoming.parent_entity_type,
            "parent_entity_id": existing.parent_entity_id or incoming.parent_entity_id,
            "parent_comment_id": existing.parent_comment_id or incoming.parent_comment_id,
            "reply_to_message_id": existing.reply_to_message_id or incoming.reply_to_message_id,
            "thread_root_post_id": existing.thread_root_post_id or incoming.thread_root_post_id,
            "origin_post_id": existing.origin_post_id or incoming.origin_post_id,
            "container_source_id": existing.container_source_id or incoming.container_source_id,
            "container_source_name": existing.container_source_name or incoming.container_source_name,
            "container_source_url": existing.container_source_url or incoming.container_source_url,
            "container_source_type": existing.container_source_type or incoming.container_source_type,
            "discovery_kind": existing.discovery_kind or incoming.discovery_kind,
            "created_at": existing.created_at or incoming.created_at,
            "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
            "raw_text": incoming.raw_text if len(incoming.raw_text or "") > len(existing.raw_text or "") else existing.raw_text,
            "permalink": existing.permalink or incoming.permalink,
            "reactions": max(existing.reactions, incoming.reactions),
            "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
            "depth": max(existing.depth, incoming.depth),
            "source_collector": incoming.source_collector or existing.source_collector,
            "raw_path": incoming.raw_path or existing.raw_path,
            "author": select_author(existing.author, incoming.author),
        }
    )


def prefer_numeric_max(existing: int | None, incoming: int | None) -> int | None:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    return max(existing, incoming)


def select_author(existing: Any, incoming: Any) -> Any:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    existing_name = existing.name or ""
    incoming_name = incoming.name or ""
    if len(incoming_name) > len(existing_name):
        return incoming
    return existing


def sort_comments(comments: list[CommentSnapshot]) -> list[CommentSnapshot]:
    return sorted(
        comments,
        key=lambda comment: (
            comment.depth,
            comment.created_at or "",
            comment.comment_id,
        ),
    )


def merge_observed_sources(manifests: list[CollectionManifest]) -> list[ObservedSourceSnapshot]:
    merged: dict[tuple[str, str, str], ObservedSourceSnapshot] = {}
    for manifest in manifests:
        for observed_source in manifest.observed_sources:
            key = (
                observed_source.container_source_id,
                observed_source.discovery_kind,
                observed_source.platform,
            )
            merged[key] = observed_source
    return list(merged.values())


def merge_match_hits(manifests: list[CollectionManifest]) -> list[MatchHitSnapshot]:
    merged: dict[str, MatchHitSnapshot] = {}
    for manifest in manifests:
        for match_hit in manifest.match_hits:
            merged[match_hit.match_id] = match_hit
    return list(merged.values())
