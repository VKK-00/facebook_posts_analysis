from __future__ import annotations

import json
from typing import Any

import polars as pl


def post_overview(posts: pl.DataFrame, comments: pl.DataFrame) -> pl.DataFrame:
    if posts.is_empty():
        return pl.DataFrame()
    extracted_counts = (
        comments.group_by("parent_post_id").agg(pl.len().alias("extracted_comment_count"))
        if not comments.is_empty()
        else pl.DataFrame(schema={"parent_post_id": pl.String, "extracted_comment_count": pl.Int64})
    )
    return (
        posts.select(
            "post_id",
            "created_at",
            "permalink",
            "reactions",
            "shares",
            "comments_count",
            "views",
            "forwards",
            "reply_count",
            "has_media",
            "media_type",
            pl.col("message").fill_null("").str.slice(0, 220).alias("post_excerpt"),
        )
        .join(extracted_counts, left_on="post_id", right_on="parent_post_id", how="left")
        .with_columns(
            pl.col("extracted_comment_count").fill_null(0),
            (pl.col("comments_count") - pl.col("extracted_comment_count").fill_null(0)).alias("comment_gap"),
        )
        .sort("created_at", descending=True)
    )


def propagation_overview(propagations: pl.DataFrame, comments: pl.DataFrame) -> pl.DataFrame:
    if propagations.is_empty():
        return pl.DataFrame()
    extracted_counts = (
        comments.filter(pl.col("parent_entity_type") == "propagation")
        .group_by("parent_entity_id")
        .agg(pl.len().alias("extracted_comment_count"))
        if not comments.is_empty()
        else pl.DataFrame(schema={"parent_entity_id": pl.String, "extracted_comment_count": pl.Int64})
    )
    return (
        propagations.select(
            "propagation_id",
            "origin_post_id",
            "origin_external_id",
            "propagation_kind",
            "created_at",
            "permalink",
            "reactions",
            "shares",
            "comments_count",
            "views",
            "forwards",
            "reply_count",
            "has_media",
            "media_type",
            pl.col("message").fill_null("").str.slice(0, 220).alias("propagation_excerpt"),
        )
        .join(extracted_counts, left_on="propagation_id", right_on="parent_entity_id", how="left")
        .with_columns(
            pl.col("extracted_comment_count").fill_null(0),
            (pl.col("comments_count") - pl.col("extracted_comment_count").fill_null(0)).alias("comment_gap"),
        )
        .sort("created_at", descending=True)
    )


def propagation_comment_overview(comments: pl.DataFrame, propagations: pl.DataFrame) -> pl.DataFrame:
    if comments.is_empty() or "parent_entity_type" not in comments.columns:
        return pl.DataFrame()
    propagation_comments = comments.filter(pl.col("parent_entity_type") == "propagation")
    if propagation_comments.is_empty():
        return pl.DataFrame()
    propagation_lookup = (
        propagations.select(
            pl.col("propagation_id").alias("parent_entity_id"),
            "origin_post_id",
            "propagation_kind",
            pl.col("permalink").alias("propagation_permalink"),
        )
        if not propagations.is_empty()
        else pl.DataFrame(
            schema={
                "parent_entity_id": pl.String,
                "origin_post_id": pl.String,
                "propagation_kind": pl.String,
                "propagation_permalink": pl.String,
            }
        )
    )
    base_columns = [
        "comment_id",
        "parent_post_id",
        "parent_entity_id",
        "origin_post_id",
        "parent_comment_id",
        "thread_root_post_id",
        "reply_to_message_id",
        "created_at",
        "depth",
        "author_id",
        "permalink",
    ]
    available_columns = [column for column in base_columns if column in propagation_comments.columns]
    return (
        propagation_comments.select(
            *available_columns,
            pl.col("message").fill_null("").str.slice(0, 280).alias("comment_excerpt"),
        )
        .join(propagation_lookup, on="parent_entity_id", how="left", suffix="_propagation")
        .with_columns(
            pl.coalesce([pl.col("origin_post_id_propagation"), pl.col("origin_post_id")]).alias("origin_post_id"),
        )
        .drop("origin_post_id_propagation")
        .sort(["created_at", "comment_id"], descending=[True, False])
    )


def telegram_summary(posts: pl.DataFrame, comments: pl.DataFrame, collection_runs: pl.DataFrame) -> dict[str, Any]:
    discussion_linked = (
        bool(collection_runs["discussion_linked"][0])
        if collection_runs.height and "discussion_linked" in collection_runs.columns
        else False
    )
    filtered_service_message_count = (
        int(collection_runs["filtered_service_message_count"][0])
        if collection_runs.height and "filtered_service_message_count" in collection_runs.columns
        else 0
    )
    return {
        "discussion_linked": discussion_linked,
        "filtered_service_message_count": filtered_service_message_count,
        "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
        "total_forwards": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
        "total_reply_count": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
        "reaction_breakdown": reaction_breakdown_summary(posts, comments),
    }


def x_summary(posts: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any]:
    return {
        "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
        "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
        "total_reposts": int(posts["shares"].fill_null(0).sum()) if "shares" in posts.columns and posts.height else 0,
        "total_quotes": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
        "total_replies": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
        "reaction_breakdown": reaction_breakdown_summary(posts, comments),
    }


def threads_summary(posts: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any]:
    return {
        "total_views": int(posts["views"].fill_null(0).sum()) if "views" in posts.columns and posts.height else 0,
        "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
        "total_reposts": int(posts["shares"].fill_null(0).sum()) if "shares" in posts.columns and posts.height else 0,
        "total_quotes": int(posts["forwards"].fill_null(0).sum()) if "forwards" in posts.columns and posts.height else 0,
        "total_replies": int(posts["reply_count"].fill_null(0).sum()) if "reply_count" in posts.columns and posts.height else 0,
        "reaction_breakdown": reaction_breakdown_summary(posts, comments),
    }


def instagram_summary(posts: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any]:
    return {
        "total_likes": int(posts["reactions"].fill_null(0).sum()) if "reactions" in posts.columns and posts.height else 0,
        "total_comments_visible": int(posts["comments_count"].fill_null(0).sum())
        if "comments_count" in posts.columns and posts.height
        else 0,
        "total_comments_extracted": comments.height,
        "reels_count": int(posts.filter(pl.col("media_type").fill_null("").str.to_lowercase() == "reel").height)
        if "media_type" in posts.columns
        else 0,
        "reaction_breakdown": reaction_breakdown_summary(posts, comments),
    }


def propagation_summary(propagations: pl.DataFrame, comments: pl.DataFrame) -> dict[str, Any] | None:
    if propagations.is_empty():
        return None
    comment_counts = (
        comments.filter(pl.col("parent_entity_type") == "propagation").group_by("parent_entity_id").agg(pl.len().alias("count"))
        if not comments.is_empty()
        else pl.DataFrame(schema={"parent_entity_id": pl.String, "count": pl.Int64})
    )
    kind_counts = (
        propagations.group_by("propagation_kind").agg(pl.len().alias("count")).sort("count", descending=True).to_dicts()
        if "propagation_kind" in propagations.columns
        else []
    )
    return {
        "total_instances": propagations.height,
        "with_comments": int(comment_counts.height),
        "extracted_comments": int(comment_counts["count"].sum()) if comment_counts.height else 0,
        "visible_comment_total": int(propagations["comments_count"].fill_null(0).sum())
        if "comments_count" in propagations.columns
        else 0,
        "kinds": kind_counts,
    }


def top_propagated_items(posts: pl.DataFrame, propagations: pl.DataFrame) -> list[dict[str, Any]]:
    if propagations.is_empty():
        return []
    counts = (
        propagations.filter(pl.col("origin_post_id").is_not_null() & (pl.col("origin_post_id") != ""))
        .group_by("origin_post_id")
        .agg(
            pl.len().alias("propagation_count"),
            pl.col("comments_count").fill_null(0).sum().alias("propagation_comment_count"),
        )
        .sort("propagation_count", descending=True)
        .head(10)
    )
    post_lookup = {row["post_id"]: row for row in posts.to_dicts()}
    rows: list[dict[str, Any]] = []
    for row in counts.to_dicts():
        origin_post = post_lookup.get(row["origin_post_id"], {})
        rows.append(
            {
                "origin_post_id": row["origin_post_id"],
                "propagation_count": row["propagation_count"],
                "propagation_comment_count": row["propagation_comment_count"],
                "origin_excerpt": (origin_post.get("message") or "")[:220],
                "origin_permalink": origin_post.get("permalink"),
            }
        )
    return rows


def reply_depth_summary(comments: pl.DataFrame) -> list[dict[str, Any]]:
    if comments.is_empty():
        return []
    return comments.group_by("depth").agg(pl.len().alias("count")).sort("depth").to_dicts()


def reaction_breakdown_summary(posts: pl.DataFrame, comments: pl.DataFrame) -> list[dict[str, Any]]:
    totals: dict[str, int] = {}
    for frame in (posts, comments):
        if frame.is_empty() or "reaction_breakdown_json" not in frame.columns:
            continue
        for raw_value in frame["reaction_breakdown_json"].fill_null("").to_list():
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            for key, value in payload.items():
                totals[str(key)] = totals.get(str(key), 0) + int(value or 0)
    return [
        {"reaction": reaction, "count": count}
        for reaction, count in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]


def person_monitor_breakdown(match_hits: pl.DataFrame) -> dict[str, int]:
    if match_hits.is_empty():
        return {
            "authored_posts": 0,
            "authored_comments": 0,
            "mentioned_posts": 0,
            "mentioned_comments": 0,
        }

    post_like = {"post", "propagation"}
    authored_hits = match_hits.filter(pl.col("match_kind") == "authored_by_subject")
    mention_hits = match_hits.filter(pl.col("match_kind") != "authored_by_subject")
    return {
        "authored_posts": authored_hits.filter(pl.col("item_type").is_in(list(post_like))).select("item_id").unique().height,
        "authored_comments": authored_hits.filter(pl.col("item_type") == "comment").select("item_id").unique().height,
        "mentioned_posts": mention_hits.filter(pl.col("item_type").is_in(list(post_like))).select("item_id").unique().height,
        "mentioned_comments": mention_hits.filter(pl.col("item_type") == "comment").select("item_id").unique().height,
    }


def top_external_sources_by_hit_count(match_hits: pl.DataFrame, observed_sources: pl.DataFrame) -> list[dict[str, Any]]:
    if match_hits.is_empty():
        return []
    counts = (
        match_hits.group_by("container_source_id")
        .agg(
            pl.len().alias("hit_count"),
            pl.col("item_id").n_unique().alias("unique_items"),
        )
        .sort("hit_count", descending=True)
        .head(10)
    )
    if observed_sources.is_empty():
        return counts.to_dicts()
    joined = counts.join(
        observed_sources.select(
            "container_source_id",
            "container_source_name",
            "container_source_url",
            "container_source_type",
            "discovery_kind",
            "status",
        ).unique(subset=["container_source_id"], keep="first"),
        on="container_source_id",
        how="left",
    )
    return joined.to_dicts()
