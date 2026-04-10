from __future__ import annotations

import polars as pl

from social_posts_analysis.analysis.language import LanguageDetector
from social_posts_analysis.analysis.metrics import compute_support_metrics
from social_posts_analysis.analysis.providers import HeuristicLLMProvider, OpenAICompatibleLLMProvider
from social_posts_analysis.config import LLMProviderConfig, SideConfig
from social_posts_analysis.reporting.service import ReportService


def test_language_detector_fallbacks() -> None:
    detector = LanguageDetector(["ru", "uk", "en"])

    assert detector.detect("Підтримую реформу").language == "uk"
    assert detector.detect("Поддерживаю реформу").language == "ru"
    assert detector.detect("This is a policy update").language == "en"


def test_support_metrics_aggregate_comment_scope() -> None:
    stance = pl.DataFrame(
        [
            {"item_type": "comment", "item_id": "c1", "side_id": "side_a", "label": "support", "confidence": 0.8, "model_name": "x", "run_id": "r1"},
            {"item_type": "comment", "item_id": "c2", "side_id": "side_a", "label": "oppose", "confidence": 0.8, "model_name": "x", "run_id": "r1"},
            {"item_type": "comment", "item_id": "c3", "side_id": "side_a", "label": "neutral", "confidence": 0.8, "model_name": "x", "run_id": "r1"},
        ]
    )
    memberships = pl.DataFrame(
        [
            {"item_type": "comment", "item_id": "c1", "cluster_id": "comment-0", "run_id": "r1"},
            {"item_type": "comment", "item_id": "c2", "cluster_id": "comment-0", "run_id": "r1"},
            {"item_type": "comment", "item_id": "c3", "cluster_id": "comment-1", "run_id": "r1"},
        ]
    )
    comments = pl.DataFrame(
        [
            {"comment_id": "c1", "parent_post_id": "p1", "parent_entity_type": "post", "parent_entity_id": "p1", "origin_post_id": "p1"},
            {"comment_id": "c2", "parent_post_id": "p2", "parent_entity_type": "propagation", "parent_entity_id": "prop1", "origin_post_id": "p1"},
            {"comment_id": "c3", "parent_post_id": "p1", "parent_entity_type": "post", "parent_entity_id": "p1", "origin_post_id": "p1"},
        ]
    )

    metrics = compute_support_metrics(stance, memberships, comments, "r1")
    global_row = metrics.filter((pl.col("scope_type") == "global") & (pl.col("side_id") == "side_a")).to_dicts()[0]
    origin_plus_row = metrics.filter(
        (pl.col("scope_type") == "origin_plus_propagations") & (pl.col("scope_id") == "p1") & (pl.col("side_id") == "side_a")
    ).to_dicts()[0]

    assert global_row["support_count"] == 1
    assert global_row["oppose_count"] == 1
    assert global_row["neutral_count"] == 1
    assert global_row["net_support"] == 0
    assert origin_plus_row["support_count"] == 1
    assert origin_plus_row["oppose_count"] == 1


def test_support_metrics_handles_schema_less_empty_input() -> None:
    metrics = compute_support_metrics(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), "r1")
    assert metrics.is_empty()


def test_heuristic_llm_provider_marks_support_and_opposition() -> None:
    provider = HeuristicLLMProvider()
    side = SideConfig(
        side_id="side_a",
        name="Actor A",
        aliases=["actor a"],
        support_keywords=["support actor a"],
        oppose_keywords=["oppose actor a"],
    )

    support = provider.classify_stance("I fully support Actor A today.", side)
    oppose = provider.classify_stance("I oppose Actor A on this topic.", side)

    assert support["label"] == "support"
    assert oppose["label"] == "oppose"


def test_openai_compatible_llm_provider_parses_json(monkeypatch) -> None:
    provider = OpenAICompatibleLLMProvider(
        LLMProviderConfig(
            kind="openai_compatible",
            base_url="https://example.test/v1",
            api_key="secret",
            model="test-model",
        )
    )

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):  # noqa: ANN201
            return {
                "choices": [
                    {
                        "message": {
                            "content": '{"label":"support","confidence":0.91}'
                        }
                    }
                ]
            }

    monkeypatch.setattr(provider.client, "post", lambda *args, **kwargs: Response())
    side = SideConfig(side_id="side_a", name="Actor A")

    prediction = provider.classify_stance("Actor A is right here.", side)

    assert prediction["label"] == "support"
    assert prediction["confidence"] == 0.91


def test_report_service_builds_x_summary(project_config, project_paths) -> None:
    service = ReportService(project_config, project_paths)
    posts = pl.DataFrame(
        [
            {
                "views": 100,
                "reactions": 5,
                "shares": 2,
                "forwards": 1,
                "reply_count": 3,
                "reaction_breakdown_json": '{"like_count": 5, "retweet_count": 2, "view_count": 100}',
            }
        ]
    )
    comments = pl.DataFrame(
        [
            {
                "reaction_breakdown_json": '{"like_count": 1}',
            }
        ]
    )

    summary = service._x_summary(posts, comments)

    assert summary["total_views"] == 100
    assert summary["total_likes"] == 5
    assert summary["total_reposts"] == 2
    assert summary["total_quotes"] == 1
    assert summary["total_replies"] == 3
    assert summary["reaction_breakdown"][0]["reaction"] == "view_count"


def test_report_service_builds_threads_and_instagram_summaries(project_config, project_paths) -> None:
    service = ReportService(project_config, project_paths)
    threads_posts = pl.DataFrame(
        [
            {
                "views": 100,
                "reactions": 5,
                "shares": 2,
                "forwards": 1,
                "reply_count": 3,
                "reaction_breakdown_json": '{"like": 5, "repost": 2}',
            }
        ]
    )
    instagram_posts = pl.DataFrame(
        [
            {
                "reactions": 7,
                "comments_count": 9,
                "media_type": "reel",
                "reaction_breakdown_json": '{"likes": 7}',
            }
        ]
    )
    comments = pl.DataFrame([{"reaction_breakdown_json": '{"likes": 1}'}])

    threads_summary = service._threads_summary(threads_posts, comments)
    instagram_summary = service._instagram_summary(instagram_posts, comments)

    assert threads_summary["total_views"] == 100
    assert threads_summary["total_reposts"] == 2
    assert instagram_summary["total_likes"] == 7
    assert instagram_summary["total_comments_visible"] == 9
    assert instagram_summary["reels_count"] == 1
