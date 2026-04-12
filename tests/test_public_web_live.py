from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from social_posts_analysis.collectors.public_web import PublicWebCollector
from social_posts_analysis.config_models import load_config
from social_posts_analysis.raw_store import RawSnapshotStore


@pytest.mark.skipif(os.getenv("SPA_RUN_PUBLIC_WEB_LIVE") != "1", reason="manual authenticated public_web live test")
def test_public_web_authenticated_collect_live(tmp_path: Path) -> None:
    config = load_config(Path("config/project.local.yaml"))
    collector = PublicWebCollector(config)
    run_id = f"pytest-live-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    raw_store = RawSnapshotStore(tmp_path / "raw" / run_id)

    manifest = collector.collect(run_id, raw_store)

    assert manifest.collector == "public_web"
    assert manifest.source.source_url
    assert len(manifest.posts) > 0
