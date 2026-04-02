from __future__ import annotations

import polars as pl

from facebook_posts_analysis.analysis.language import LanguageDetector
from facebook_posts_analysis.analysis.metrics import compute_support_metrics
from facebook_posts_analysis.analysis.providers import HeuristicLLMProvider, OpenAICompatibleLLMProvider
from facebook_posts_analysis.config import LLMProviderConfig, SideConfig


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

    metrics = compute_support_metrics(stance, memberships, "r1")
    global_row = metrics.filter((pl.col("scope_type") == "global") & (pl.col("side_id") == "side_a")).to_dicts()[0]

    assert global_row["support_count"] == 1
    assert global_row["oppose_count"] == 1
    assert global_row["neutral_count"] == 1
    assert global_row["net_support"] == 0


def test_support_metrics_handles_schema_less_empty_input() -> None:
    metrics = compute_support_metrics(pl.DataFrame(), pl.DataFrame(), "r1")
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
