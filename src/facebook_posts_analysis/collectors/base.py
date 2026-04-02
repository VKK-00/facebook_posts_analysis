from __future__ import annotations

from abc import ABC, abstractmethod

from facebook_posts_analysis.contracts import CollectionManifest
from facebook_posts_analysis.raw_store import RawSnapshotStore


class CollectorUnavailableError(RuntimeError):
    """Raised when a collector cannot be used in the current environment."""


class BaseCollector(ABC):
    name: str

    @abstractmethod
    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        raise NotImplementedError
