from __future__ import annotations

import pytest

from social_posts_analysis.collectors.telegram_mtproto import TelegramMtprotoCollector
from social_posts_analysis.collectors.telegram_web import TelegramWebCollector
from social_posts_analysis.collectors.threads_api import ThreadsApiCollector
from social_posts_analysis.collectors.threads_web import ThreadsWebCollector
from social_posts_analysis.collectors.x_api import XApiCollector
from social_posts_analysis.collectors.x_web import XWebCollector
from social_posts_analysis.config import ProjectConfig, WatchlistSourceConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    MatchHitSnapshot,
    ObservedSourceSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.person_monitoring import (
    DiscoverySource,
    PersonMonitorOrchestrator,
    build_request_signature,
    normalize_profile_url,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.reporting.service import ReportService


def test_person_monitor_config_validates_watchlist_only(project_config) -> None:
    payload = project_config.model_dump()
    payload["source"] = {
        "kind": "person_monitor",
        "platform": "x",
        "source_name": "Subject Name",
        "aliases": ["Subject Name"],
        "watchlist": [{"source_name": "external_a"}],
        "search": {"enabled": False},
        "telegram": {"discussion_chat_id": None},
    }
    payload["collector"]["mode"] = "web"
    payload["collector"]["x_web"]["enabled"] = True

    config = ProjectConfig.model_validate(payload)

    assert config.source.kind == "person_monitor"
    assert config.source.watchlist[0].source_name == "external_a"


def test_person_monitor_config_validates_search_only(project_config) -> None:
    payload = project_config.model_dump()
    payload["source"] = {
        "kind": "person_monitor",
        "platform": "x",
        "source_name": "Subject Name",
        "aliases": ["Subject Name"],
        "watchlist": [],
        "search": {"enabled": True, "queries": ["subject_handle"]},
        "telegram": {"discussion_chat_id": None},
    }
    payload["collector"]["mode"] = "web"
    payload["collector"]["x_web"]["enabled"] = True

    config = ProjectConfig.model_validate(payload)

    assert config.source.search.enabled is True
    assert config.source.search.queries == ["subject_handle"]


def test_person_monitor_config_rejects_missing_discovery_path(project_config) -> None:
    payload = project_config.model_dump()
    payload["source"] = {
        "kind": "person_monitor",
        "platform": "x",
        "source_name": "Subject Name",
        "aliases": ["Subject Name"],
        "watchlist": [],
        "search": {"enabled": False},
        "telegram": {"discussion_chat_id": None},
    }
    payload["collector"]["mode"] = "web"
    payload["collector"]["x_web"]["enabled"] = True

    with pytest.raises(ValueError, match="person_monitor source requires source.watchlist or source.search.enabled=true"):
        ProjectConfig.model_validate(payload)


def test_person_monitor_x_api_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "x"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://x.com/subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.collector.mode = "x_api"
    project_config.collector.x_api.enabled = True
    project_config.collector.x_api.bearer_token = "token"

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        assert endpoint == "/tweets/search/recent"
        return {
            "data": [
                {
                    "id": "1",
                    "text": "Mention @subject_handle",
                    "author_id": "external_a",
                    "conversation_id": "1",
                },
                {
                    "id": "2",
                    "text": "Own post by monitored account",
                    "author_id": "subject_handle",
                    "conversation_id": "2",
                },
            ],
            "includes": {
                "users": [
                    {"id": "external_a", "username": "external_a", "name": "External A"},
                    {"id": "subject_handle", "username": "subject_handle", "name": "Subject Name"},
                ]
            },
            "meta": {},
        }

    monkeypatch.setattr(XApiCollector, "_get_json", fake_get_json)
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_a", "External A", "search")
    ]


def test_person_monitor_telegram_mtproto_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "telegram"
    project_config.source.source_id = "subject_channel"
    project_config.source.source_name = "Subject Channel"
    project_config.source.url = "https://t.me/subject_channel"
    project_config.source.aliases = ["Subject Channel"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_channel"]
    project_config.collector.mode = "mtproto"
    project_config.collector.telegram_mtproto.enabled = True
    project_config.collector.telegram_mtproto.session_file = ".sessions/example"
    project_config.collector.telegram_mtproto.api_id = 12345
    project_config.collector.telegram_mtproto.api_hash = "hash"

    monkeypatch.setattr(
        TelegramMtprotoCollector,
        "discover_person_monitor_sources",
        lambda self, **kwargs: [
            {
                "source_id": "subject_channel",
                "source_name": "Subject Channel",
                "source_url": "https://t.me/subject_channel",
                "source_type": "channel",
            },
            {
                "source_id": "external_group",
                "source_name": "External Group",
                "source_url": "https://t.me/external_group",
                "source_type": "group",
            },
        ],
    )
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_group", "External Group", "search")
    ]


def test_person_monitor_telegram_web_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "telegram"
    project_config.source.source_id = "subject_channel"
    project_config.source.source_name = "Subject Channel"
    project_config.source.url = "https://t.me/subject_channel"
    project_config.source.aliases = ["Subject Channel"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["https://t.me/s/external_channel?q=subject_channel"]
    project_config.collector.mode = "web"
    project_config.collector.telegram_web.enabled = True

    monkeypatch.setattr(
        TelegramWebCollector,
        "discover_person_monitor_sources",
        lambda self, **kwargs: [
            {
                "source_id": "subject_channel",
                "source_name": "Subject Channel",
                "source_url": "https://t.me/s/subject_channel",
                "source_type": "channel",
            },
            {
                "source_id": "external_channel",
                "source_name": "External Channel",
                "source_url": "https://t.me/s/external_channel?q=subject_channel",
                "source_type": "channel",
            },
        ],
    )
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_channel", "External Channel", "search")
    ]


def test_person_monitor_threads_api_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "threads"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://www.threads.net/@subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.collector.mode = "threads_api"
    project_config.collector.threads_api.enabled = True
    project_config.collector.threads_api.access_token = "token"

    monkeypatch.setattr(
        ThreadsApiCollector,
        "discover_person_monitor_sources",
        lambda self, **kwargs: [
            {
                "source_id": "subject_handle",
                "source_name": "Subject Name",
                "source_url": "https://www.threads.net/@subject_handle",
                "source_type": "account",
            },
            {
                "source_id": "external_threads",
                "source_name": "external_threads",
                "source_url": "https://www.threads.net/@external_threads",
                "source_type": "account",
            },
        ],
    )
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_threads", "external_threads", "search")
    ]


def test_person_monitor_threads_web_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "threads"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://www.threads.net/@subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.source.search.include_comments = False
    project_config.collector.mode = "web"
    project_config.collector.threads_web.enabled = True

    monkeypatch.setattr(
        ThreadsWebCollector,
        "discover_person_monitor_sources",
        lambda self, **kwargs: [
            {
                "source_id": "subject_handle",
                "source_name": "Subject Name",
                "source_url": "https://www.threads.net/@subject_handle",
                "source_type": "account",
            },
            {
                "source_id": "external_threads_web",
                "source_name": "external_threads_web",
                "source_url": "https://www.threads.net/@external_threads_web",
                "source_type": "account",
            },
        ],
    )
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_threads_web", "external_threads_web", "search")
    ]


def test_person_monitor_threads_web_search_warns_for_comment_only_mode(project_config) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "threads"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://www.threads.net/@subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.source.search.include_posts = False
    project_config.source.search.include_comments = True
    project_config.collector.mode = "web"
    project_config.collector.threads_web.enabled = True

    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert discovery_sources == []
    assert observed_rows == []
    assert warnings == [
        "Threads web search discovery currently derives external surfaces from public search result posts only; reply/comment-only discovery is not supported.",
        "Threads web search discovery completed but found no external surfaces for the configured queries.",
    ]


def test_person_monitor_x_web_search_discovers_external_sources(project_config, monkeypatch) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "x"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://x.com/subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.collector.mode = "web"
    project_config.collector.x_web.enabled = True

    monkeypatch.setattr(
        XWebCollector,
        "discover_person_monitor_sources",
        lambda self, **kwargs: [
            {
                "source_id": "subject_handle",
                "source_name": "Subject Name",
                "source_url": "https://x.com/subject_handle",
                "source_type": "account",
            },
            {
                "source_id": "external_b",
                "source_name": "External B",
                "source_url": "https://x.com/external_b",
                "source_type": "account",
            },
        ],
    )
    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])

    discovery_sources, observed_rows, warnings = orchestrator._discover_search_sources()

    assert observed_rows == []
    assert warnings == []
    assert [(item.source_id, item.source_name, item.discovery_kind) for item in discovery_sources] == [
        ("external_b", "External B", "search")
    ]


def test_person_monitor_orchestrator_dedupes_items_and_preserves_match_kinds(project_config, monkeypatch, tmp_path) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "x"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://x.com/subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.watchlist = [
        WatchlistSourceConfig(source_id="external_a", source_name="External A", source_type="account")
    ]
    project_config.source.search.enabled = True
    project_config.source.search.queries = ["subject_handle"]
    project_config.collector.mode = "web"
    project_config.collector.x_web.enabled = True

    class FakeCollector:
        name = "fake_x"

        def __init__(self, config) -> None:
            self.config = config

        def collect(self, run_id, raw_store):  # noqa: ANN001, ANN201
            return CollectionManifest(
                run_id=run_id,
                collected_at="2026-04-15T10:00:00+00:00",
                collector=self.name,
                mode="web",
                request_signature=build_request_signature(self.config),
                source=SourceSnapshot(
                    platform="x",
                    source_id="external_a",
                    source_name="External A",
                    source_url="https://x.com/external_a",
                    source_type="account",
                    source_collector=self.name,
                ),
                posts=[
                    PostSnapshot(
                        post_id="x:external_a:1",
                        platform="x",
                        source_id="external_a",
                        message="Subject Name mentioned @subject_handle https://x.com/subject_handle",
                        permalink="https://x.com/external_a/status/1",
                        source_collector=self.name,
                        author=AuthorSnapshot(
                            author_id="external_a",
                            name="External A",
                            profile_url="https://x.com/external_a",
                        ),
                        comments=[
                            CommentSnapshot(
                                comment_id="x:external_a:1:comment:1",
                                platform="x",
                                parent_post_id="x:external_a:1",
                                message="Comment by subject",
                                permalink="https://x.com/external_a/status/1#comment-1",
                                source_collector=self.name,
                                author=AuthorSnapshot(
                                    author_id="subject_handle",
                                    name="Subject Name",
                                    profile_url="https://x.com/subject_handle",
                                ),
                            )
                        ],
                    )
                ],
            )

    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [FakeCollector(cfg)])
    monkeypatch.setattr(
        orchestrator,
        "_discover_search_sources",
        lambda: (
            [
                DiscoverySource(
                    source_id="external_a",
                    source_name="External A",
                    source_url="https://x.com/external_a",
                    source_type="account",
                    discovery_kind="search",
                )
            ],
            [],
            [],
        ),
    )

    manifest = orchestrator.collect("pm-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.source_kind == "person_monitor"
    assert len(manifest.posts) == 1
    assert manifest.posts[0].source_id == "subject_handle"
    assert manifest.posts[0].container_source_id == "external_a"
    assert manifest.posts[0].discovery_kind == "watchlist"
    assert len(manifest.posts[0].comments) == 1
    assert len(manifest.observed_sources) == 1
    assert {hit.match_kind for hit in manifest.match_hits} >= {
        "alias_text_mention",
        "handle_mention",
        "profile_url_mention",
        "authored_by_subject",
    }


def test_person_monitor_threads_profile_url_aliases_match_threads_com_and_net(project_config) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "threads"
    project_config.source.source_id = "openai"
    project_config.source.source_name = "OpenAI"
    project_config.source.url = "https://www.threads.net/@openai"
    project_config.source.aliases = []
    project_config.collector.mode = "web"

    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [])
    container_source = SourceSnapshot(
        platform="threads",
        source_id="external_threads",
        source_name="External Threads",
        source_url="https://www.threads.net/@external_threads",
        source_collector="threads_web",
    )

    hits = orchestrator._match_item(
        item_type="post",
        item_id="threads:external_threads:1",
        author=AuthorSnapshot(
            author_id="different_display_id",
            name="Different Display",
            profile_url="https://www.threads.com/@openai",
        ),
        text_candidates=["Subject profile link: https://www.threads.com/@openai"],
        container_source=container_source,
    )

    assert normalize_profile_url("https://www.threads.com/@OpenAI/") == "https://threads.net/@openai"
    assert {hit.match_kind for hit in hits} >= {"authored_by_subject", "profile_url_mention"}


def test_person_monitor_orchestrator_dedupes_watchlist_and_search_surfaces(project_config, monkeypatch, tmp_path) -> None:
    project_config.source.kind = "person_monitor"
    project_config.source.platform = "x"
    project_config.source.source_id = "subject_handle"
    project_config.source.source_name = "Subject Name"
    project_config.source.url = "https://x.com/subject_handle"
    project_config.source.aliases = ["Subject Name"]
    project_config.source.watchlist = [
        WatchlistSourceConfig(source_id="external_a", source_name="External A", source_type="account")
    ]
    project_config.source.search.enabled = True
    project_config.collector.mode = "web"
    project_config.collector.x_web.enabled = True

    calls: list[str] = []

    class FakeCollector:
        name = "fake_x"

        def __init__(self, config) -> None:
            self.config = config

        def collect(self, run_id, raw_store):  # noqa: ANN001, ANN201
            calls.append(self.config.source.source_id or "")
            return CollectionManifest(
                run_id=run_id,
                collected_at="2026-04-15T10:00:00+00:00",
                collector=self.name,
                mode="web",
                request_signature=build_request_signature(self.config),
                source=SourceSnapshot(
                    platform="x",
                    source_id="external_a",
                    source_name="External A",
                    source_url="https://x.com/external_a",
                    source_type="account",
                    source_collector=self.name,
                ),
                posts=[],
            )

    orchestrator = PersonMonitorOrchestrator(project_config, collector_builder=lambda cfg: [FakeCollector(cfg)])
    monkeypatch.setattr(
        orchestrator,
        "_discover_search_sources",
        lambda: (
            [
                DiscoverySource(
                    source_id="external_a",
                    source_name="External A",
                    source_url="https://x.com/external_a",
                    source_type="account",
                    discovery_kind="search",
                )
            ],
            [],
            [],
        ),
    )

    manifest = orchestrator.collect("pm-run-dedupe", RawSnapshotStore(tmp_path / "raw"))

    assert calls == ["external_a"]
    assert len(manifest.observed_sources) == 1


def test_person_monitor_report_context_exposes_observed_sources_and_match_exports(
    project_root,
    project_config,
    project_paths,
) -> None:
    run_id = "20260415T101500Z"
    root_source = SourceSnapshot(
        platform="x",
        source_id="subject_handle",
        source_kind="person_monitor",
        source_name="Subject Name",
        source_url="https://x.com/subject_handle",
        source_type="profile",
        source_collector="person_monitor",
    )
    manifest = CollectionManifest(
        run_id=run_id,
        collected_at="2026-04-15T10:15:00+00:00",
        collector="person_monitor",
        mode="web",
        status="partial",
        request_signature="sig-1",
        warnings=["X web extraction is best-effort and public replies may be limited without an authenticated browser session."],
        source=root_source,
        posts=[
            PostSnapshot(
                post_id="x:external_a:1",
                platform="x",
                source_id="subject_handle",
                source_kind="person_monitor",
                container_source_id="external_a",
                container_source_name="External A",
                container_source_url="https://x.com/external_a",
                container_source_type="account",
                discovery_kind="watchlist",
                created_at="2026-04-15T09:00:00+00:00",
                message="Mentioned Subject Name",
                permalink="https://x.com/external_a/status/1",
                source_collector="person_monitor",
                comments=[
                    CommentSnapshot(
                        comment_id="x:external_a:1:comment:1",
                        platform="x",
                        source_kind="person_monitor",
                        parent_post_id="x:external_a:1",
                        container_source_id="external_a",
                        container_source_name="External A",
                        container_source_url="https://x.com/external_a",
                        container_source_type="account",
                        discovery_kind="watchlist",
                        created_at="2026-04-15T09:05:00+00:00",
                        message="Comment by subject",
                        permalink="https://x.com/external_a/status/1#comment-1",
                        source_collector="person_monitor",
                        author=AuthorSnapshot(
                            author_id="subject_handle",
                            name="Subject Name",
                            profile_url="https://x.com/subject_handle",
                        ),
                    )
                ],
            )
        ],
        observed_sources=[
            ObservedSourceSnapshot(
                container_source_id="external_a",
                container_source_name="External A",
                container_source_url="https://x.com/external_a",
                container_source_type="account",
                discovery_kind="watchlist",
                platform="x",
                status="success",
                warning_count=0,
                source_collector="x_web",
            ),
        ],
        match_hits=[
            MatchHitSnapshot(
                match_id="hit-post",
                item_type="post",
                item_id="x:external_a:1",
                match_kind="alias_text_mention",
                matched_value="Subject Name",
                platform="x",
                container_source_id="external_a",
            ),
            MatchHitSnapshot(
                match_id="hit-comment",
                item_type="comment",
                item_id="x:external_a:1:comment:1",
                match_kind="authored_by_subject",
                matched_value="subject_handle",
                platform="x",
                container_source_id="external_a",
            ),
        ],
    )
    run_dir = project_root / "data" / "raw" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

    NormalizationService(project_config, project_paths).run(run_id=run_id)
    context = ReportService(project_config, project_paths)._build_context(run_id)

    assert context["is_person_monitor"] is True
    assert context["person_monitor_stats"]["authored_comments"] == 1
    assert context["person_monitor_stats"]["mentioned_posts"] == 1
    assert "observed_sources" in context["export_tables"]
    assert "match_hits" in context["export_tables"]
    assert "matched_posts" in context["export_tables"]
    assert "matched_comments" in context["export_tables"]

    ReportService(project_config, project_paths).run(run_id=run_id)
    markdown_path = project_paths.reports_root / f"report_{run_id}.md"
    markdown_text = markdown_path.read_text(encoding="utf-8")

    assert "Person Monitor Summary" in markdown_text
    assert "Observed Surfaces" in markdown_text
