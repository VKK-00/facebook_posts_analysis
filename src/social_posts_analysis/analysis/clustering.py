from __future__ import annotations

import re
from collections import Counter
from typing import Any

import numpy as np

from .providers import LLMProvider

STOPWORDS = {
    "the",
    "and",
    "that",
    "with",
    "this",
    "from",
    "\u0434\u043b\u044f",
    "\u044d\u0442\u043e",
    "\u043a\u0430\u043a",
    "\u0447\u0442\u043e",
    "\u0430\u043b\u0435",
    "\u043f\u0440\u043e",
    "\u0435\u0441\u043b\u0438",
    "they",
    "have",
}


def _tokenize(text: str) -> list[str]:
    return [
        token
        for token in re.findall(r"[\w']+", text.lower())
        if len(token) > 2 and token not in STOPWORDS
    ]


class NarrativeClusterer:
    def __init__(self, llm_provider: LLMProvider, exemplar_count: int, min_cluster_size: int, min_samples: int) -> None:
        self.llm_provider = llm_provider
        self.exemplar_count = exemplar_count
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples

    def cluster_items(
        self,
        item_type: str,
        items: list[dict[str, Any]],
        embeddings: np.ndarray,
        run_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not items:
            return [], []

        labels = self._cluster_labels(embeddings)
        if len(labels) != len(items):
            labels = [0] * len(items)

        memberships: list[dict[str, Any]] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item, label in zip(items, labels, strict=False):
            cluster_key = f"{item_type}-{'noise' if label == -1 else label}"
            memberships.append(
                {
                    "item_type": item_type,
                    "item_id": item["item_id"],
                    "cluster_id": cluster_key,
                    "run_id": run_id,
                }
            )
            grouped.setdefault(cluster_key, []).append(item)

        summaries: list[dict[str, Any]] = []
        for cluster_id, members in grouped.items():
            keywords = self._keywords(members)
            sorted_members = sorted(members, key=lambda entry: len(entry["text"]), reverse=True)
            exemplars = [item["item_id"] for item in sorted_members[: self.exemplar_count]]
            llm_summary = self.llm_provider.summarize_cluster(
                item_type=item_type,
                keywords=keywords,
                texts=[member["text"] for member in members[:3]],
            )
            summaries.append(
                {
                    "item_type": item_type,
                    "cluster_id": cluster_id,
                    "label": llm_summary.get("label") or cluster_id,
                    "description": llm_summary.get("description") or "",
                    "top_keywords": keywords,
                    "exemplar_ids": exemplars,
                    "run_id": run_id,
                }
            )
        return summaries, memberships

    def _cluster_labels(self, embeddings: np.ndarray) -> list[int]:
        if len(embeddings) == 0:
            return []
        if len(embeddings) < max(2, self.min_cluster_size):
            return [0] * len(embeddings)
        try:
            import hdbscan

            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min(self.min_cluster_size, len(embeddings)),
                min_samples=min(self.min_samples, max(len(embeddings) - 1, 1)),
            )
            labels = clusterer.fit_predict(embeddings)
            if len(set(labels)) == 1 and list(set(labels))[0] == -1:
                return [0] * len(embeddings)
            return list(map(int, labels))
        except Exception:
            return [0] * len(embeddings)

    @staticmethod
    def _keywords(members: list[dict[str, Any]]) -> list[str]:
        counter: Counter[str] = Counter()
        for member in members:
            counter.update(_tokenize(member["text"]))
        return [token for token, _ in counter.most_common(8)]
