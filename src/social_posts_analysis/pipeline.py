from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from social_posts_analysis.collectors.base import BaseCollector, CollectorUnavailableError
from social_posts_analysis.collectors.instagram_graph_api import InstagramGraphApiCollector
from social_posts_analysis.collectors.instagram_web import InstagramWebCollector
from social_posts_analysis.collectors.meta_api import MetaApiCollector
from social_posts_analysis.collectors.public_web import PublicWebCollector
from social_posts_analysis.collectors.telegram_bot_api import TelegramBotApiCollector
from social_posts_analysis.collectors.telegram_mtproto import TelegramMtprotoCollector
from social_posts_analysis.collectors.telegram_web import TelegramWebCollector
from social_posts_analysis.collectors.threads_api import ThreadsApiCollector
from social_posts_analysis.collectors.threads_web import ThreadsWebCollector
from social_posts_analysis.collectors.x_api import XApiCollector
from social_posts_analysis.collectors.x_web import XWebCollector
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import CollectionManifest
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import make_run_id


class CollectionService:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> CollectionManifest:
        resolved_run_id = run_id or make_run_id()
        return self._run_single(resolved_run_id)

    def run_many(self, run_id: str | None = None, passes: int | None = None) -> list[CollectionManifest]:
        resolved_passes = max(1, passes or self.config.collector.multi_pass_runs)
        if resolved_passes == 1:
            return [self.run(run_id=run_id)]

        base_run_id = run_id or make_run_id()
        manifests: list[CollectionManifest] = []
        for pass_index in range(resolved_passes):
            current_run_id = f"{base_run_id}-p{pass_index + 1:02d}"
            manifests.append(self._run_single(current_run_id))
            if pass_index < resolved_passes - 1 and self.config.collector.wait_between_passes_seconds > 0:
                time.sleep(self.config.collector.wait_between_passes_seconds)
        return manifests

    def _run_single(self, resolved_run_id: str) -> CollectionManifest:
        run_dir = self.paths.run_raw_dir(resolved_run_id)
        raw_store = RawSnapshotStore(run_dir)

        warnings: list[str] = []
        collectors = self._build_collectors()
        if not collectors:
            raise RuntimeError("No collectors are available for the configured platform and mode.")

        for index, collector in enumerate(collectors):
            try:
                manifest = collector.collect(resolved_run_id, raw_store)
                manifest.warnings = [*warnings, *manifest.warnings]
                manifest.fallback_used = index > 0
                self._write_manifest(run_dir, manifest)
                return manifest
            except CollectorUnavailableError as exc:
                warnings.append(f"{collector.__class__.__name__}: {exc}")
            except Exception as exc:  # pragma: no cover
                warnings.append(f"{collector.__class__.__name__}: {exc}")

        raise RuntimeError("All configured collectors failed: " + "; ".join(warnings))

    def _build_collectors(self) -> list[BaseCollector]:
        if self.config.source.platform == "telegram":
            if self.config.collector.mode == "mtproto":
                return [TelegramMtprotoCollector(self.config)]
            if self.config.collector.mode == "bot_api":
                return [TelegramBotApiCollector(self.config)]
            return [TelegramWebCollector(self.config)]
        if self.config.source.platform == "x":
            if self.config.collector.mode == "x_api":
                return [XApiCollector(self.config)]
            return [XWebCollector(self.config)]
        if self.config.source.platform == "threads":
            if self.config.collector.mode == "threads_api":
                return [ThreadsApiCollector(self.config)]
            return [ThreadsWebCollector(self.config)]
        if self.config.source.platform == "instagram":
            if self.config.collector.mode == "instagram_graph_api":
                return [InstagramGraphApiCollector(self.config)]
            return [InstagramWebCollector(self.config)]

        mode = self.config.collector.mode
        collector_classes: list[type[Any]]
        if mode == "api":
            collector_classes = [MetaApiCollector]
        elif mode == "web":
            collector_classes = [PublicWebCollector]
        else:
            collector_classes = [MetaApiCollector, PublicWebCollector]

        collectors: list[BaseCollector] = []
        for collector_class in collector_classes:
            try:
                collectors.append(collector_class(self.config))
            except CollectorUnavailableError:
                if mode != "hybrid":
                    raise
        return collectors

    @staticmethod
    def _write_manifest(run_dir: Path, manifest: CollectionManifest) -> None:
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")


class PipelineRunner:
    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> dict[str, Any]:
        collection_service = CollectionService(self.config, self.paths)
        collection_manifests = collection_service.run_many(run_id=run_id, passes=self.config.collector.multi_pass_runs)
        collection_manifest = collection_manifests[-1]

        from social_posts_analysis.analysis.service import AnalysisService
        from social_posts_analysis.normalize import NormalizationService
        from social_posts_analysis.reporting.service import ReportService, ReviewExportService

        NormalizationService(self.config, self.paths).run(run_id=collection_manifest.run_id)
        AnalysisService(self.config, self.paths).run(run_id=collection_manifest.run_id)
        ReviewExportService(self.config, self.paths).run(run_id=collection_manifest.run_id)
        report_files = ReportService(self.config, self.paths).run(run_id=collection_manifest.run_id)
        return {
            "run_id": collection_manifest.run_id,
            "report_markdown": str(report_files[0]),
            "report_html": str(report_files[1]),
        }
