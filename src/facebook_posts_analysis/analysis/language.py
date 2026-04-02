from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class LanguagePrediction:
    language: str
    confidence: float
    method: str


class LanguageDetector:
    def __init__(self, allowed_languages: list[str]) -> None:
        self.allowed_languages = allowed_languages

    def detect(self, text: str | None) -> LanguagePrediction:
        normalized = (text or "").strip()
        if not normalized:
            return LanguagePrediction(language="unknown", confidence=0.0, method="empty")

        try:
            from langdetect import DetectorFactory, detect_langs

            DetectorFactory.seed = 0
            predictions = detect_langs(normalized)
            if predictions:
                best = max(predictions, key=lambda item: item.prob)
                language = best.lang
                if language in self.allowed_languages:
                    return LanguagePrediction(language=language, confidence=float(best.prob), method="langdetect")
        except Exception:
            pass

        lowered = normalized.lower()
        if re.search(r"[іїєґ]", lowered):
            return LanguagePrediction(language="uk", confidence=0.72, method="heuristic-script")
        if re.search(r"[ыэёъ]", lowered):
            return LanguagePrediction(language="ru", confidence=0.72, method="heuristic-script")
        if re.search(r"[а-я]", lowered):
            return LanguagePrediction(language="ru", confidence=0.58, method="heuristic-cyrillic")

        english_markers = {"the", "and", "with", "that", "this", "from", "will"}
        tokens = set(re.findall(r"[a-zA-Z']+", lowered))
        if tokens & english_markers:
            return LanguagePrediction(language="en", confidence=0.6, method="heuristic-lexicon")
        if re.search(r"[a-z]", lowered):
            return LanguagePrediction(language="en", confidence=0.45, method="heuristic-latin")
        return LanguagePrediction(language="unknown", confidence=0.2, method="heuristic-fallback")

