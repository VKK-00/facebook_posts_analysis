from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
import numpy as np

from facebook_posts_analysis.config import EmbeddingProviderConfig, LLMProviderConfig, SideConfig


def _join_api_url(base_url: str, suffix: str) -> str:
    return base_url.rstrip("/") + suffix


class EmbeddingProvider(Protocol):
    name: str

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        raise NotImplementedError


class LLMProvider(Protocol):
    name: str

    def summarize_cluster(self, item_type: str, keywords: list[str], texts: list[str]) -> dict[str, str]:
        raise NotImplementedError

    def classify_stance(self, text: str, side: SideConfig) -> dict[str, Any]:
        raise NotImplementedError


class HashEmbeddingProvider:
    name = "hash_embedding"

    def __init__(self, dimension: int = 256) -> None:
        self.dimension = dimension

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        vectors = [self._embed_one(text) for text in texts]
        return np.vstack(vectors) if vectors else np.zeros((0, self.dimension))

    def _embed_one(self, text: str) -> np.ndarray:
        vector = np.zeros(self.dimension, dtype=float)
        for token in re.findall(r"[\w']+", text.lower()):
            index = hash(token) % self.dimension
            sign = 1.0 if hash(token + "::sign") % 2 else -1.0
            vector[index] += sign
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector
        return vector / norm


class OpenAICompatibleEmbeddingProvider:
    name = "openai_compatible_embedding"

    def __init__(self, config: EmbeddingProviderConfig) -> None:
        if not config.base_url or not config.api_key:
            raise ValueError("OpenAI-compatible embedding provider requires base_url and api_key.")
        self.config = config
        self.client = httpx.Client(timeout=config.timeout_seconds)

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.config.dimension))
        response = self.client.post(
            _join_api_url(self.config.base_url, "/embeddings"),
            headers={"Authorization": f"Bearer {self.config.api_key}"},
            json={"model": self.config.model, "input": texts},
        )
        response.raise_for_status()
        payload = response.json()
        embeddings = [item["embedding"] for item in payload.get("data", [])]
        return np.array(embeddings, dtype=float)


POSITIVE_MARKERS = {
    "good",
    "great",
    "support",
    "agree",
    "important",
    "справедливо",
    "підтримую",
    "поддерживаю",
    "молодцы",
    "дякую",
}
NEGATIVE_MARKERS = {
    "bad",
    "wrong",
    "oppose",
    "against",
    "hate",
    "не поддерживаю",
    "против",
    "брехня",
    "ложь",
    "коррупция",
}


class HeuristicLLMProvider:
    name = "heuristic_llm"

    def summarize_cluster(self, item_type: str, keywords: list[str], texts: list[str]) -> dict[str, str]:
        label = ", ".join(keywords[:3]) if keywords else f"{item_type.title()} cluster"
        description = (
            f"{item_type.title()} discussion centered on {', '.join(keywords[:5])}."
            if keywords
            else f"Automatically discovered {item_type} discussion cluster."
        )
        return {"label": label, "description": description}

    def classify_stance(self, text: str, side: SideConfig) -> dict[str, Any]:
        normalized = (text or "").lower()
        if not normalized.strip():
            return {"label": "unclear", "confidence": 0.0}

        mentions_side = any(alias in normalized for alias in side.all_names)
        side_positive = sum(1 for token in side.support_keywords if token.lower() in normalized)
        side_negative = sum(1 for token in side.oppose_keywords if token.lower() in normalized)
        global_positive = sum(1 for token in POSITIVE_MARKERS if token in normalized)
        global_negative = sum(1 for token in NEGATIVE_MARKERS if token in normalized)
        positive_score = side_positive + global_positive
        negative_score = side_negative + global_negative

        if positive_score > negative_score and (mentions_side or side_positive > 0):
            return {"label": "support", "confidence": min(0.55 + 0.1 * positive_score, 0.95)}
        if negative_score > positive_score and (mentions_side or side_negative > 0):
            return {"label": "oppose", "confidence": min(0.55 + 0.1 * negative_score, 0.95)}
        if mentions_side:
            return {"label": "neutral", "confidence": 0.45}
        return {"label": "unclear", "confidence": 0.35}


class OpenAICompatibleLLMProvider:
    name = "openai_compatible_llm"

    def __init__(self, config: LLMProviderConfig) -> None:
        if not config.base_url or not config.api_key:
            raise ValueError("OpenAI-compatible LLM provider requires base_url and api_key.")
        self.config = config
        self.client = httpx.Client(timeout=config.timeout_seconds)

    def summarize_cluster(self, item_type: str, keywords: list[str], texts: list[str]) -> dict[str, str]:
        prompt = {
            "role": "user",
            "content": (
                "Return compact JSON with keys label and description. "
                f"Item type: {item_type}. Keywords: {keywords}. "
                f"Sample texts: {texts[:3]}."
            ),
        }
        return self._chat_json([prompt], {"label": f"{item_type.title()} cluster", "description": ""})

    def classify_stance(self, text: str, side: SideConfig) -> dict[str, Any]:
        prompt = {
            "role": "user",
            "content": (
                "Return compact JSON with keys label and confidence. "
                "Allowed labels: support, oppose, neutral, unclear. "
                f"Side: {side.name}. Aliases: {side.aliases}. Text: {text}"
            ),
        }
        payload = self._chat_json([prompt], {"label": "unclear", "confidence": 0.35})
        confidence = float(payload.get("confidence", 0.35))
        confidence = max(0.0, min(confidence, 1.0))
        return {"label": payload.get("label", "unclear"), "confidence": confidence}

    def _chat_json(self, messages: list[dict[str, str]], fallback: dict[str, Any]) -> dict[str, Any]:
        response = self.client.post(
            _join_api_url(self.config.base_url, "/chat/completions"),
            headers={"Authorization": f"Bearer {self.config.api_key}"},
            json={
                "model": self.config.model,
                "temperature": self.config.temperature,
                "messages": messages,
            },
        )
        response.raise_for_status()
        payload = response.json()
        content = payload["choices"][0]["message"]["content"]
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return fallback


@dataclass(slots=True)
class ProvidersBundle:
    embeddings: EmbeddingProvider
    llm: LLMProvider

    @property
    def summary(self) -> dict[str, str]:
        return {"embeddings": self.embeddings.name, "llm": self.llm.name}


def build_providers(
    embedding_config: EmbeddingProviderConfig,
    llm_config: LLMProviderConfig,
) -> ProvidersBundle:
    if embedding_config.kind == "openai_compatible":
        embedding_provider: EmbeddingProvider = OpenAICompatibleEmbeddingProvider(embedding_config)
    elif embedding_config.kind == "hash":
        embedding_provider = HashEmbeddingProvider(dimension=embedding_config.dimension)
    else:
        embedding_provider = (
            OpenAICompatibleEmbeddingProvider(embedding_config)
            if embedding_config.base_url and embedding_config.api_key
            else HashEmbeddingProvider(dimension=embedding_config.dimension)
        )

    if llm_config.kind == "openai_compatible":
        llm_provider: LLMProvider = OpenAICompatibleLLMProvider(llm_config)
    elif llm_config.kind == "heuristic":
        llm_provider = HeuristicLLMProvider()
    else:
        llm_provider = (
            OpenAICompatibleLLMProvider(llm_config)
            if llm_config.base_url and llm_config.api_key
            else HeuristicLLMProvider()
        )

    return ProvidersBundle(embeddings=embedding_provider, llm=llm_provider)

