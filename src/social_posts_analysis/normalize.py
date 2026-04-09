from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

import duckdb
import polars as pl

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import CollectionManifest, CommentSnapshot, PostSnapshot, SourceSnapshot
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.utils import read_json


class NormalizationService:
    TABLE_KEYS = {
        "posts": ["post_id"],
        "comments": ["comment_id"],
        "comment_edges": ["comment_id"],
        "authors": ["author_id"],
        "media_refs": ["media_id"],
        "collection_runs": ["run_id"],
    }
    TABLE_SCHEMAS = {
        "posts": {
            "post_id": pl.String,
            "platform": pl.String,
            "source_id": pl.String,
            "author_id": pl.String,
            "created_at": pl.String,
            "message": pl.String,
            "permalink": pl.String,
            "reactions": pl.Int64,
            "shares": pl.Int64,
            "comments_count": pl.Int64,
            "views": pl.Int64,
            "forwards": pl.Int64,
            "reply_count": pl.Int64,
            "has_media": pl.Boolean,
            "media_type": pl.String,
            "reaction_breakdown_json": pl.String,
            "source_collector": pl.String,
            "raw_path": pl.String,
            "run_id": pl.String,
        },
        "comments": {
            "comment_id": pl.String,
            "platform": pl.String,
            "parent_post_id": pl.String,
            "parent_comment_id": pl.String,
            "reply_to_message_id": pl.String,
            "thread_root_post_id": pl.String,
            "author_id": pl.String,
            "created_at": pl.String,
            "message": pl.String,
            "depth": pl.Int64,
            "permalink": pl.String,
            "reactions": pl.Int64,
            "reaction_breakdown_json": pl.String,
            "source_collector": pl.String,
            "raw_path": pl.String,
            "run_id": pl.String,
        },
        "comment_edges": {
            "comment_id": pl.String,
            "parent_post_id": pl.String,
            "parent_comment_id": pl.String,
            "reply_to_message_id": pl.String,
            "thread_root_post_id": pl.String,
            "depth": pl.Int64,
            "run_id": pl.String,
        },
        "authors": {
            "author_id": pl.String,
            "name": pl.String,
            "profile_url": pl.String,
            "source_collector": pl.String,
            "run_id": pl.String,
        },
        "media_refs": {
            "media_id": pl.String,
            "owner_post_id": pl.String,
            "media_type": pl.String,
            "title": pl.String,
            "url": pl.String,
            "preview_url": pl.String,
            "run_id": pl.String,
        },
        "collection_runs": {
            "run_id": pl.String,
            "collected_at": pl.String,
            "collector": pl.String,
            "mode": pl.String,
            "status": pl.String,
            "fallback_used": pl.Boolean,
            "warning_count": pl.Int64,
            "post_count": pl.Int64,
            "comment_count": pl.Int64,
            "platform": pl.String,
            "source_id": pl.String,
            "source_name": pl.String,
            "source_type": pl.String,
            "discussion_linked": pl.Boolean,
            "filtered_service_message_count": pl.Int64,
            "source_run_count": pl.Int64,
            "source_run_ids": pl.List(pl.String),
        },
    }

    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> dict[str, Any]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No collection runs found to normalize.")

        source_run_ids = self._resolve_source_run_ids(resolved_run_id)
        manifests = self._load_manifests(source_run_ids)
        manifest = self._merge_manifests(resolved_run_id, manifests)

        posts_records: list[dict[str, Any]] = []
        comments_records: list[dict[str, Any]] = []
        comment_edges: list[dict[str, Any]] = []
        authors: list[dict[str, Any]] = []
        media_refs: list[dict[str, Any]] = []

        if manifest.source.source_id:
            authors.append(
                {
                    "author_id": manifest.source.source_id,
                    "name": manifest.source.source_name,
                    "profile_url": manifest.source.source_url,
                    "source_collector": manifest.source.source_collector,
                    "run_id": manifest.run_id,
                }
            )

        for post in manifest.posts:
            posts_records.append(
                {
                    "post_id": post.post_id,
                    "platform": post.platform,
                    "source_id": post.source_id,
                    "author_id": post.author.author_id if post.author else None,
                    "created_at": post.created_at,
                    "message": post.message,
                    "permalink": post.permalink,
                    "reactions": post.reactions,
                    "shares": post.shares,
                    "comments_count": post.comments_count,
                    "views": post.views,
                    "forwards": post.forwards,
                    "reply_count": post.reply_count,
                    "has_media": post.has_media,
                    "media_type": post.media_type,
                    "reaction_breakdown_json": post.reaction_breakdown_json,
                    "source_collector": post.source_collector,
                    "raw_path": post.raw_path,
                    "run_id": manifest.run_id,
                }
            )
            if post.author and post.author.author_id:
                authors.append(
                    {
                        "author_id": post.author.author_id,
                        "name": post.author.name,
                        "profile_url": post.author.profile_url,
                        "source_collector": post.source_collector,
                        "run_id": manifest.run_id,
                    }
                )
            for media in post.media_refs:
                media_refs.append(
                    {
                        "media_id": media.media_id,
                        "owner_post_id": media.owner_post_id,
                        "media_type": media.media_type,
                        "title": media.title,
                        "url": media.url,
                        "preview_url": media.preview_url,
                        "run_id": manifest.run_id,
                    }
                )
            for comment in post.comments:
                comments_records.append(
                    {
                        "comment_id": comment.comment_id,
                        "platform": comment.platform,
                        "parent_post_id": comment.parent_post_id,
                        "parent_comment_id": comment.parent_comment_id,
                        "reply_to_message_id": comment.reply_to_message_id,
                        "thread_root_post_id": comment.thread_root_post_id,
                        "author_id": comment.author.author_id if comment.author else None,
                        "created_at": comment.created_at,
                        "message": comment.message,
                        "depth": comment.depth,
                        "permalink": comment.permalink,
                        "reactions": comment.reactions,
                        "reaction_breakdown_json": comment.reaction_breakdown_json,
                        "source_collector": comment.source_collector,
                        "raw_path": comment.raw_path,
                        "run_id": manifest.run_id,
                    }
                )
                comment_edges.append(
                    {
                        "comment_id": comment.comment_id,
                        "parent_post_id": comment.parent_post_id,
                        "parent_comment_id": comment.parent_comment_id,
                        "reply_to_message_id": comment.reply_to_message_id,
                        "thread_root_post_id": comment.thread_root_post_id,
                        "depth": comment.depth,
                        "run_id": manifest.run_id,
                    }
                )
                if comment.author and comment.author.author_id:
                    authors.append(
                        {
                            "author_id": comment.author.author_id,
                            "name": comment.author.name,
                            "profile_url": comment.author.profile_url,
                            "source_collector": comment.source_collector,
                            "run_id": manifest.run_id,
                        }
                    )

        collection_runs = [
            {
                "run_id": manifest.run_id,
                "collected_at": manifest.collected_at,
                "collector": manifest.collector,
                "mode": manifest.mode,
                "status": manifest.status,
                "fallback_used": manifest.fallback_used,
                "warning_count": len(manifest.warnings),
                "post_count": len(posts_records),
                "comment_count": len(comments_records),
                "platform": manifest.source.platform,
                "source_id": manifest.source.source_id,
                "source_name": manifest.source.source_name,
                "source_type": manifest.source.source_type,
                "discussion_linked": manifest.source.discussion_linked,
                "filtered_service_message_count": manifest.source.filtered_service_message_count,
                "source_run_count": len(source_run_ids),
                "source_run_ids": source_run_ids,
            }
        ]

        outputs = {
            "posts": self._persist_table("posts", posts_records),
            "comments": self._persist_table("comments", comments_records),
            "comment_edges": self._persist_table("comment_edges", comment_edges),
            "authors": self._persist_table("authors", authors),
            "media_refs": self._persist_table("media_refs", media_refs),
            "collection_runs": self._persist_table("collection_runs", collection_runs),
        }
        self._sync_duckdb(outputs)
        return {
            "run_id": manifest.run_id,
            "source_run_ids": source_run_ids,
            "tables": {name: str(path) for name, path in outputs.items()},
        }

    def _resolve_source_run_ids(self, resolved_run_id: str) -> list[str]:
        configured = [run_id for run_id in self.config.normalization.source_run_ids if run_id]
        if configured:
            available = set(self.paths.list_run_ids())
            missing = [run_id for run_id in configured if run_id not in available]
            if missing:
                raise RuntimeError(f"Configured source_run_ids are missing raw snapshots: {', '.join(missing)}")
            return configured

        available_run_ids = self.paths.list_run_ids()
        if resolved_run_id not in available_run_ids:
            raise RuntimeError(f"Run {resolved_run_id} does not exist in raw snapshots.")
        target_index = available_run_ids.index(resolved_run_id)
        merge_recent_runs = max(1, self.config.normalization.merge_recent_runs)
        return available_run_ids[max(0, target_index - merge_recent_runs + 1) : target_index + 1]

    def _load_manifests(self, run_ids: list[str]) -> list[CollectionManifest]:
        manifests: list[CollectionManifest] = []
        for run_id in run_ids:
            manifest_path = self.paths.run_raw_dir(run_id) / "manifest.json"
            manifests.append(CollectionManifest.model_validate(read_json(manifest_path)))
        return manifests

    def _merge_manifests(self, output_run_id: str, manifests: list[CollectionManifest]) -> CollectionManifest:
        merged_posts: dict[str, PostSnapshot] = {}
        for manifest in manifests:
            for post in manifest.posts:
                merged_posts[post.post_id] = self._merge_post_snapshots(merged_posts.get(post.post_id), post)

        latest_manifest = manifests[-1]
        source = self._merge_source_snapshots([manifest.source for manifest in manifests])
        warnings = list(dict.fromkeys(warning for manifest in manifests for warning in manifest.warnings))
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
            collector=latest_manifest.collector,
            mode=latest_manifest.mode,
            status=status,
            fallback_used=any(manifest.fallback_used for manifest in manifests),
            warnings=warnings,
            cursors=latest_manifest.cursors,
            source=source,
            posts=posts,
        )

    @staticmethod
    def _merge_source_snapshots(sources: list[SourceSnapshot]) -> SourceSnapshot:
        latest_source = sources[-1]
        merged = latest_source.model_copy(deep=True)
        for source in reversed(sources[:-1]):
            merged = merged.model_copy(
                update={
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

    def _merge_post_snapshots(self, existing: PostSnapshot | None, incoming: PostSnapshot) -> PostSnapshot:
        if existing is None:
            return incoming.model_copy(deep=True)

        merged_comments: dict[str, CommentSnapshot] = {comment.comment_id: comment for comment in existing.comments}
        merged_media_refs = {media.media_id: media for media in existing.media_refs}
        for media in incoming.media_refs:
            merged_media_refs[media.media_id] = media
        for comment in incoming.comments:
            merged_comments[comment.comment_id] = self._merge_comment_snapshots(merged_comments.get(comment.comment_id), comment)

        return existing.model_copy(
            update={
                "created_at": existing.created_at or incoming.created_at,
                "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
                "permalink": existing.permalink or incoming.permalink,
                "reactions": max(existing.reactions, incoming.reactions),
                "shares": max(existing.shares, incoming.shares),
                "comments_count": max(existing.comments_count, incoming.comments_count, len(merged_comments)),
                "views": self._prefer_numeric_max(existing.views, incoming.views),
                "forwards": self._prefer_numeric_max(existing.forwards, incoming.forwards),
                "reply_count": self._prefer_numeric_max(existing.reply_count, incoming.reply_count),
                "has_media": existing.has_media or incoming.has_media,
                "media_type": existing.media_type or incoming.media_type,
                "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
                "source_collector": incoming.source_collector or existing.source_collector,
                "raw_path": incoming.raw_path or existing.raw_path,
                "author": self._select_author(existing.author, incoming.author),
                "media_refs": list(merged_media_refs.values()),
                "comments": self._sort_comments(list(merged_comments.values())),
            }
        )

    def _merge_comment_snapshots(self, existing: CommentSnapshot | None, incoming: CommentSnapshot) -> CommentSnapshot:
        if existing is None:
            return incoming.model_copy(deep=True)
        return existing.model_copy(
            update={
                "parent_comment_id": existing.parent_comment_id or incoming.parent_comment_id,
                "reply_to_message_id": existing.reply_to_message_id or incoming.reply_to_message_id,
                "thread_root_post_id": existing.thread_root_post_id or incoming.thread_root_post_id,
                "created_at": existing.created_at or incoming.created_at,
                "message": incoming.message if len(incoming.message or "") > len(existing.message or "") else existing.message,
                "permalink": existing.permalink or incoming.permalink,
                "reactions": max(existing.reactions, incoming.reactions),
                "reaction_breakdown_json": existing.reaction_breakdown_json or incoming.reaction_breakdown_json,
                "depth": max(existing.depth, incoming.depth),
                "source_collector": incoming.source_collector or existing.source_collector,
                "raw_path": incoming.raw_path or existing.raw_path,
                "author": self._select_author(existing.author, incoming.author),
            }
        )

    @staticmethod
    def _prefer_numeric_max(existing: int | None, incoming: int | None) -> int | None:
        if existing is None:
            return incoming
        if incoming is None:
            return existing
        return max(existing, incoming)

    @staticmethod
    def _select_author(existing: Any, incoming: Any) -> Any:
        if existing is None:
            return incoming
        if incoming is None:
            return existing
        existing_name = existing.name or ""
        incoming_name = incoming.name or ""
        if len(incoming_name) > len(existing_name):
            return incoming
        return existing

    @staticmethod
    def _sort_comments(comments: list[CommentSnapshot]) -> list[CommentSnapshot]:
        return sorted(
            comments,
            key=lambda comment: (
                comment.depth,
                comment.created_at or "",
                comment.comment_id,
            ),
        )

    def _persist_table(self, table_name: str, records: list[dict[str, Any]]) -> Path:
        path = self.paths.processed_root / f"{table_name}.parquet"
        schema = cast(dict[str, Any], self.TABLE_SCHEMAS[table_name])
        new_df = pl.DataFrame(records, schema=schema) if records else pl.DataFrame(schema=schema)
        if path.exists():
            existing_df = pl.read_parquet(path)
            if new_df.is_empty():
                combined = existing_df
            elif existing_df.is_empty():
                combined = new_df
            else:
                combined = pl.concat([existing_df, new_df], how="diagonal_relaxed")
        else:
            combined = new_df

        key_columns = [column for column in self.TABLE_KEYS[table_name] if column in combined.columns]
        if key_columns and not combined.is_empty():
            combined = combined.unique(subset=key_columns, keep="last")
        combined.write_parquet(path)
        return path

    def _sync_duckdb(self, table_paths: dict[str, Path]) -> None:
        connection = duckdb.connect(str(self.paths.database_path))
        try:
            for table_name, path in table_paths.items():
                path_str = path.as_posix().replace("'", "''")
                connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')")
        finally:
            connection.close()
