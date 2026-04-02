from __future__ import annotations

from typing import Any

from facebook_posts_analysis.config import SideConfig

from .providers import LLMProvider


class StanceAnalyzer:
    def __init__(self, llm_provider: LLMProvider, sides: list[SideConfig]) -> None:
        self.llm_provider = llm_provider
        self.sides = sides

    def label_items(self, item_type: str, items: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
        labels: list[dict[str, Any]] = []
        for item in items:
            for side in self.sides:
                prediction = self.llm_provider.classify_stance(item["text"], side)
                labels.append(
                    {
                        "item_type": item_type,
                        "item_id": item["item_id"],
                        "side_id": side.side_id,
                        "label": prediction["label"],
                        "confidence": float(prediction["confidence"]),
                        "model_name": self.llm_provider.name,
                        "run_id": run_id,
                    }
                )
        return labels

