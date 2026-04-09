from __future__ import annotations

from social_posts_analysis.collectors.base import BaseCollector, CollectorUnavailableError
from social_posts_analysis.contracts import CollectionManifest, SourceSnapshot
from social_posts_analysis.pipeline import CollectionService
from social_posts_analysis.utils import utc_now_iso


class FailingCollector(BaseCollector):
    name = "failing"

    def collect(self, run_id, raw_store):  # noqa: ANN001, ANN201
        raise CollectorUnavailableError("primary collector unavailable")


class SuccessfulCollector(BaseCollector):
    name = "success"

    def collect(self, run_id, raw_store):  # noqa: ANN001, ANN201
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode="hybrid",
            source=SourceSnapshot(
                platform="facebook",
                source_id="page_1",
                source_name="Example",
                source_type="page",
                source_collector=self.name,
            ),
            posts=[],
        )


def test_collection_service_uses_fallback(project_config, project_paths, monkeypatch) -> None:
    service = CollectionService(project_config, project_paths)
    monkeypatch.setattr(service, "_build_collectors", lambda: [FailingCollector(), SuccessfulCollector()])

    manifest = service.run(run_id="fallback-run")

    assert manifest.collector == "success"
    assert manifest.fallback_used is True
    assert any("primary collector unavailable" in warning for warning in manifest.warnings)


def test_collection_service_run_many_suffixes_run_ids(project_config, project_paths, monkeypatch) -> None:
    project_config.collector.multi_pass_runs = 2
    project_config.collector.wait_between_passes_seconds = 0
    service = CollectionService(project_config, project_paths)
    monkeypatch.setattr(service, "_build_collectors", lambda: [SuccessfulCollector()])

    manifests = service.run_many(run_id="batch-run", passes=2)

    assert [manifest.run_id for manifest in manifests] == ["batch-run-p01", "batch-run-p02"]
