from __future__ import annotations

import polars as pl


def compute_support_metrics(
    stance_labels: pl.DataFrame,
    comment_memberships: pl.DataFrame,
    comments: pl.DataFrame,
    run_id: str,
) -> pl.DataFrame:
    if "item_type" not in stance_labels.columns:
        return pl.DataFrame(
            schema={
                "scope_type": pl.String,
                "scope_id": pl.String,
                "side_id": pl.String,
                "support_count": pl.Int64,
                "oppose_count": pl.Int64,
                "neutral_count": pl.Int64,
                "unclear_count": pl.Int64,
                "support_ratio": pl.Float64,
                "net_support": pl.Int64,
                "run_id": pl.String,
            }
        )
    comment_stance = stance_labels.filter(pl.col("item_type") == "comment")
    if comment_stance.is_empty():
        return pl.DataFrame(
            schema={
                "scope_type": pl.String,
                "scope_id": pl.String,
                "side_id": pl.String,
                "support_count": pl.Int64,
                "oppose_count": pl.Int64,
                "neutral_count": pl.Int64,
                "unclear_count": pl.Int64,
                "support_ratio": pl.Float64,
                "net_support": pl.Int64,
                "run_id": pl.String,
            }
        )

    global_metrics = _aggregate_scope(comment_stance, ["side_id"], "global", "all")
    joined_comments = (
        comment_stance.join(
            comments.select(
                "comment_id",
                "parent_post_id",
                "parent_entity_type",
                "parent_entity_id",
                "origin_post_id",
            ),
            left_on="item_id",
            right_on="comment_id",
            how="left",
        )
        if not comments.is_empty()
        else comment_stance
    )

    scoped_frames = [global_metrics]
    if "parent_entity_type" in joined_comments.columns and "parent_entity_id" in joined_comments.columns:
        direct_origin_metrics = _aggregate_scope(
            joined_comments.filter(pl.col("parent_entity_type") == "post"),
            ["parent_entity_id", "side_id"],
            "origin_post",
            None,
        ).rename({"parent_entity_id": "scope_id"})
        propagation_metrics = _aggregate_scope(
            joined_comments.filter(pl.col("parent_entity_type") == "propagation"),
            ["parent_entity_id", "side_id"],
            "propagation",
            None,
        ).rename({"parent_entity_id": "scope_id"})
        origin_plus_metrics = _aggregate_scope(
            joined_comments.filter(pl.col("origin_post_id").is_not_null() & (pl.col("origin_post_id") != "")),
            ["origin_post_id", "side_id"],
            "origin_plus_propagations",
            None,
        ).rename({"origin_post_id": "scope_id"})
        for frame in (direct_origin_metrics, propagation_metrics, origin_plus_metrics):
            if not frame.is_empty():
                scoped_frames.append(frame)

    if not comment_memberships.is_empty() and "cluster_id" in comment_memberships.columns:
        cluster_metrics = _aggregate_scope(
            comment_stance.join(comment_memberships, left_on="item_id", right_on="item_id", how="left"),
            ["cluster_id", "side_id"],
            "narrative_cluster",
            None,
        ).rename({"cluster_id": "scope_id"})
        if not cluster_metrics.is_empty():
            scoped_frames.append(cluster_metrics)

    return pl.concat(scoped_frames, how="diagonal_relaxed").with_columns(pl.lit(run_id).alias("run_id"))


def _aggregate_scope(df: pl.DataFrame, group_columns: list[str], scope_type: str, static_scope_id: str | None) -> pl.DataFrame:
    grouped = (
        df.group_by(group_columns)
        .agg(
            (pl.col("label") == "support").sum().alias("support_count"),
            (pl.col("label") == "oppose").sum().alias("oppose_count"),
            (pl.col("label") == "neutral").sum().alias("neutral_count"),
            (pl.col("label") == "unclear").sum().alias("unclear_count"),
        )
        .with_columns(
            (
                pl.col("support_count")
                / (
                    pl.col("support_count")
                    + pl.col("oppose_count")
                    + pl.col("neutral_count")
                ).clip(lower_bound=1)
            ).alias("support_ratio"),
            (pl.col("support_count") - pl.col("oppose_count")).alias("net_support"),
            pl.lit(scope_type).alias("scope_type"),
        )
    )
    if static_scope_id is not None:
        grouped = grouped.with_columns(pl.lit(static_scope_id).alias("scope_id"))
    return grouped
