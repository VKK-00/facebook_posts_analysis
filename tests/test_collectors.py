from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from social_posts_analysis.collectors.meta_api import MetaApiCollector
from social_posts_analysis.collectors.public_web import PublicWebCollector
from social_posts_analysis.collectors.telegram_mtproto import DiscussionContext, TelegramMtprotoCollector
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import PostSnapshot
from social_posts_analysis.raw_store import RawSnapshotStore


def test_meta_api_collector_paginates_and_recurses_comments(project_config, tmp_path: Path, monkeypatch) -> None:
    project_config.collector.meta_api.enabled = True
    project_config.collector.meta_api.access_token = "token"
    project_config.source.url = "https://www.facebook.com/example-page"
    collector = MetaApiCollector(project_config)

    def fake_get_json(self, endpoint, params=None, full_url=None):  # noqa: ANN001, ANN202
        if endpoint == "/example-page":
            return {
                "id": "page_1",
                "name": "Example Page",
                "link": "https://www.facebook.com/example-page",
                "fan_count": 10,
                "followers_count": 12,
            }
        if endpoint == "/page_1/feed" and not full_url:
            return {
                "data": [
                    {
                        "id": "page_1_post_1",
                        "message": "Support Actor A now.",
                        "created_time": "2026-04-01T10:00:00+00:00",
                        "permalink_url": "https://facebook.com/posts/1",
                        "shares": {"count": 2},
                        "reactions": {"summary": {"total_count": 4}},
                        "comments": {"summary": {"total_count": 1}},
                        "attachments": {"data": []},
                        "from": {"id": "page_1", "name": "Example Page"},
                    }
                ],
                "paging": {"next": "https://next-feed", "cursors": {"after": "cursor-1"}},
            }
        if full_url == "https://next-feed":
            return {"data": [], "paging": {}}
        if endpoint == "/page_1_post_1/comments":
            return {
                "data": [
                    {
                        "id": "comment_1",
                        "message": "I support Actor A.",
                        "created_time": "2026-04-01T11:00:00+00:00",
                        "permalink_url": "https://facebook.com/posts/1?comment_id=1",
                        "comment_count": 1,
                        "like_count": 2,
                        "from": {"id": "user_1", "name": "User 1"},
                    }
                ],
                "paging": {},
            }
        if endpoint == "/comment_1/comments":
            return {
                "data": [
                    {
                        "id": "comment_1_reply",
                        "message": "I oppose Actor A.",
                        "created_time": "2026-04-01T11:05:00+00:00",
                        "permalink_url": "https://facebook.com/posts/1?comment_id=2",
                        "comment_count": 0,
                        "like_count": 1,
                        "from": {"id": "user_2", "name": "User 2"},
                    }
                ],
                "paging": {},
            }
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, full_url={full_url}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, MetaApiCollector))
    manifest = collector.collect("run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.source_id == "page_1"
    assert len(manifest.posts) == 1
    assert manifest.posts[0].source_id == "page_1"
    assert [comment.depth for comment in manifest.posts[0].comments] == [0, 1]


def test_public_web_time_parser_handles_relative_and_calendar_values() -> None:
    one_hour = PublicWebCollector._parse_post_timestamp("1h")
    one_day = PublicWebCollector._parse_post_timestamp("1d")
    calendar = PublicWebCollector._parse_post_timestamp("March 15")
    localized = PublicWebCollector._parse_post_timestamp("24 березня 2026 року")
    embedded = PublicWebCollector._parse_post_timestamp("Рівно п’ять років тому, 24 березня 2026 року, сторінка опублікувала допис")

    assert one_hour is not None
    assert one_day is not None
    assert calendar is not None
    assert localized == "2026-03-24T00:00:00+00:00"
    assert embedded == "2026-03-24T00:00:00+00:00"


def test_public_web_date_only_end_boundary_is_inclusive(project_config) -> None:
    collector = PublicWebCollector(project_config)
    project_config.date_range.end = "2026-04-02"
    assert collector._within_configured_range("2026-04-02T10:00:00+00:00") is True


def test_public_web_permalink_normalization_removes_tracking_query() -> None:
    raw = (
        "https://www.facebook.com/example-page/posts/pfbid02BnkaQDGdXiiNCL3twBRPuQMMYMomqLe2hQM7vjUNjVWXAcqS2tM6H6VKeuiBhvq4l"
        "?__cft__[0]=token&__tn__=%2CO%2CP-R&locale=en_US"
    )
    normalized = PublicWebCollector._normalize_permalink(raw)

    assert normalized == (
        "https://www.facebook.com/example-page/posts/pfbid02BnkaQDGdXiiNCL3twBRPuQMMYMomqLe2hQM7vjUNjVWXAcqS2tM6H6VKeuiBhvq4l"
    )


def test_public_web_post_permalink_normalization_drops_comment_id() -> None:
    raw = "https://www.facebook.com/reel/1178401624169743?comment_id=1504852981166697&locale=en_US"

    normalized = PublicWebCollector._normalize_post_permalink(raw)

    assert normalized == "https://www.facebook.com/reel/1178401624169743"


def test_public_web_select_post_permalink_prefers_specific_candidate_over_generic_payload() -> None:
    permalink = PublicWebCollector._select_post_permalink(
        payload_post_permalink="https://www.facebook.com/reel",
        candidate_permalink="https://www.facebook.com/reel/1178401624169743?comment_id=1504852981166697",
        detail_url="https://www.facebook.com/reel/1178401624169743?locale=en_US",
    )

    assert permalink == "https://www.facebook.com/reel/1178401624169743"


def test_public_web_metric_parser_reads_numeric_counts() -> None:
    assert PublicWebCollector._extract_metric_count("44") == 44
    assert PublicWebCollector._extract_metric_count("87 comments") == 87
    assert PublicWebCollector._extract_metric_count("Share") == 0


def test_public_web_comment_article_limit_grows_for_larger_threads() -> None:
    assert PublicWebCollector._comment_article_limit(0, aggressive=False) == 220
    assert PublicWebCollector._comment_article_limit(35, aggressive=True) == 320
    assert PublicWebCollector._comment_article_limit(120, aggressive=True) == 420


def test_public_web_expansion_patterns_include_localized_variants() -> None:
    comment_patterns = PublicWebCollector._comment_expansion_patterns()
    reply_patterns = PublicWebCollector._reply_expansion_patterns()

    assert any("Show more comments" in pattern for pattern in comment_patterns)
    assert any("\\u041f\\u043e\\u043a\\u0430\\u0437\\u0430\\u0442\\u0438" in pattern for pattern in comment_patterns)
    assert any("See previous replies" in pattern for pattern in reply_patterns)
    assert any("\\u0412\\u0456\\u0434\\u043f\\u043e\\u0432\\u0456\\u0434\\u0456" in pattern for pattern in reply_patterns)


def test_public_web_extracts_embedded_publish_time_from_html() -> None:
    html = """
    <script type="application/json">
    {"post_id":"34286935720920970","creation_time":1772118660,"unpublished_content_type":"PUBLISHED",
     "attachments":[{"media":{"id":"1178401624169743"}}],
     "page_insights":{"100000939396702":{"post_context":{"publish_time":1772118625,"story_fbid":["1178401624169743"]}}}}
    </script>
    """

    published_at = PublicWebCollector._extract_embedded_published_at(
        html,
        detail_url="https://www.facebook.com/reel/1178401624169743?locale=en_US",
        post_permalink="https://www.facebook.com/example/videos/1178401624169743/?locale=en_US",
    )

    assert published_at == "2026-02-26T15:11:00+00:00"


def test_public_web_clean_comment_text_removes_reply_controls() -> None:
    raw = "Volodymyr Ksienich replied\n \n·\n1 Reply\n1h"

    cleaned = PublicWebCollector._clean_comment_text(raw, "Volodymyr Ksienich replied", "1h")

    assert cleaned == ""


def test_public_web_clean_comment_text_keeps_reply_body_text() -> None:
    raw = "Volodymyr Ksienich\nOlga Kotenko 100% переможе))\n19h\n3"

    cleaned = PublicWebCollector._clean_comment_text(raw, "Volodymyr Ksienich", "19h")

    assert cleaned == "Olga Kotenko 100% переможе))"


def test_public_web_derives_author_from_glued_comment_prefix() -> None:
    raw = "Таня ДонійСтудентство завжди було бунтівним, і як першокурсниця 1990 р."

    author = PublicWebCollector._derive_comment_author(raw)

    assert author == "Таня Доній"


def test_public_web_builds_comment_hierarchy_from_nesting_offset(project_config) -> None:
    collector = PublicWebCollector(project_config)

    comments = collector._build_comment_snapshots(
        post_id="post_1",
        raw_path="raw.json",
        payload_comments=[
            {
                "text": "Parent Author\nTop-level comment\n3h",
                "author_name": "Parent Author",
                "published_hint": "3h",
                "permalink": "https://facebook.com/post?comment_id=1",
                "nesting_x": 350,
            },
            {
                "text": "Reply Author\nNested reply\n2h",
                "author_name": "Reply Author",
                "published_hint": "2h",
                "permalink": "https://facebook.com/post?comment_id=2",
                "nesting_x": 404,
            },
            {
                "text": "Second Parent\nAnother top-level comment\n1h",
                "author_name": "Second Parent",
                "published_hint": "1h",
                "permalink": "https://facebook.com/post?comment_id=3",
                "nesting_x": 350,
            },
        ],
    )

    assert [comment.depth for comment in comments] == [0, 1, 0]
    assert comments[1].parent_comment_id == comments[0].comment_id
    assert comments[2].parent_comment_id is None
    assert comments[0].platform == "facebook"
    assert comments[0].thread_root_post_id == "post_1"


def test_public_web_mobile_timeline_parser_extracts_posts() -> None:
    sample = [
        {"action_id": "1", "text": "See all"},
        {"action_id": "2", "text": "Page Name is with Friend Name\nand 4 others.\n3h"},
        {"action_id": "3", "text": "First public update from the page. See more"},
        {"action_id": "4", "text": "Alice and 52 others"},
        {"action_id": "5", "text": "53"},
        {"action_id": "6", "text": "1"},
        {"action_id": "7", "text": "Commenter Name"},
        {"action_id": "8", "text": "3h\n1"},
        {"action_id": "9", "text": "Page Name\n1d"},
        {"action_id": "10", "text": "Second update from the page feed."},
        {"action_id": "11", "text": "Bob and 9 others"},
        {"action_id": "12", "text": "10"},
        {"action_id": "13", "text": "There's more to see"},
    ]

    candidates = PublicWebCollector._parse_mobile_timeline_candidates(sample, "Page Name")

    assert len(candidates) == 2
    assert candidates[0]["published_hint"] == "3h"
    assert candidates[0]["message"] == "First public update from the page."
    assert candidates[0]["reactions"] == 53
    assert candidates[0]["comments_count"] == 1
    assert candidates[1]["published_hint"] == "1d"
    assert candidates[1]["reactions"] == 10
    assert candidates[1]["comments_count"] == 0


def test_public_web_posts_match_for_mobile_and_desktop_variants() -> None:
    desktop = PostSnapshot(
        post_id="desktop-1",
        platform="facebook",
        source_id="page-1",
        created_at="2026-04-02T10:00:00+00:00",
        message="First public update from the page feed.",
        permalink="https://facebook.com/post/1",
        source_collector="public_web",
    )
    mobile = PostSnapshot(
        post_id="mobile-1",
        platform="facebook",
        source_id="page-1",
        created_at="2026-04-02T10:30:00+00:00",
        message="First public update from the page feed. See more",
        permalink=None,
        source_collector="public_web",
    )

    assert PublicWebCollector._posts_match(desktop, mobile) is True


@dataclass
class FakeReplies:
    replies: int


@dataclass
class FakeReaction:
    emoticon: str


@dataclass
class FakeReactionResult:
    reaction: FakeReaction
    count: int


@dataclass
class FakeReactions:
    results: list[FakeReactionResult]


@dataclass
class FakeReplyTo:
    reply_to_msg_id: int | None = None
    reply_to_top_id: int | None = None


@dataclass
class FakeChat:
    id: int
    title: str
    username: str | None = None


@dataclass
class FakeSender:
    id: int
    first_name: str
    username: str | None = None


@dataclass
class FakeMessage:
    id: int
    date: datetime
    message: str | None = None
    sender: FakeSender | None = None
    replies: FakeReplies | None = None
    views: int | None = None
    forwards: int | None = None
    reactions: FakeReactions | None = None
    media: Any = None
    reply_to: FakeReplyTo | None = None
    action: Any = None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "date": self.date.isoformat(),
            "message": self.message,
        }


class MessageMediaPhoto:
    pass


def _telegram_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "telegram", "source_name": "example_channel"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "mtproto",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": True,
                    "session_file": ".sessions/example",
                    "api_id": 12345,
                    "api_hash": "hash",
                    "page_size": 50,
                },
            },
        }
    )


def test_telegram_collector_collects_posts_without_discussion(tmp_path: Path, monkeypatch) -> None:
    config = _telegram_config()
    collector = TelegramMtprotoCollector(config)

    source_entity = FakeChat(id=1001, title="Example Channel", username="example_channel")
    post_message = FakeMessage(
        id=7,
        date=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        message="Channel post",
        replies=FakeReplies(replies=0),
        views=120,
        forwards=5,
        reactions=FakeReactions(results=[FakeReactionResult(FakeReaction("👍"), 3)]),
    )
    fake_client = SimpleNamespace(disconnect=lambda: None)

    monkeypatch.setattr(collector, "_open_client", lambda: fake_client)
    monkeypatch.setattr(collector, "_resolve_source_entity", lambda client: source_entity)
    monkeypatch.setattr(collector, "_resolve_discussion_entity", lambda client, source: None)
    monkeypatch.setattr(collector, "_iter_source_messages", lambda client, source: [post_message])

    manifest = collector.collect("tg-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.platform == "telegram"
    assert manifest.source.discussion_linked is False
    assert len(manifest.posts) == 1
    assert "no linked discussion chat" in manifest.warnings[0].lower()
    assert manifest.posts[0].views == 120


def test_telegram_collector_builds_nested_discussion_tree(tmp_path: Path, monkeypatch) -> None:
    config = _telegram_config()
    collector = TelegramMtprotoCollector(config)

    source_entity = FakeChat(id=1001, title="Example Channel", username="example_channel")
    discussion_entity = FakeChat(id=2002, title="Example Discussion")
    post_message = FakeMessage(
        id=7,
        date=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        message="Channel post",
        replies=FakeReplies(replies=2),
    )
    parent_comment = FakeMessage(
        id=71,
        date=datetime(2026, 4, 1, 10, 5, tzinfo=UTC),
        message="Top-level reply",
        sender=FakeSender(id=501, first_name="Alice"),
        reply_to=FakeReplyTo(reply_to_msg_id=700),
    )
    nested_reply = FakeMessage(
        id=72,
        date=datetime(2026, 4, 1, 10, 6, tzinfo=UTC),
        message="Nested reply",
        sender=FakeSender(id=502, first_name="Bob"),
        reply_to=FakeReplyTo(reply_to_msg_id=71),
        reactions=FakeReactions(results=[FakeReactionResult(FakeReaction("🔥"), 2)]),
    )
    service_message = FakeMessage(
        id=73,
        date=datetime(2026, 4, 1, 10, 7, tzinfo=UTC),
        action="join",
    )
    fake_client = SimpleNamespace(disconnect=lambda: None)

    monkeypatch.setattr(collector, "_open_client", lambda: fake_client)
    monkeypatch.setattr(collector, "_resolve_source_entity", lambda client: source_entity)
    monkeypatch.setattr(collector, "_resolve_discussion_entity", lambda client, source: discussion_entity)
    monkeypatch.setattr(collector, "_iter_source_messages", lambda client, source: [post_message])
    monkeypatch.setattr(
        collector,
        "_fetch_discussion_context",
        lambda client, source, message: DiscussionContext(chat=discussion_entity, root_message_id=700),
    )
    monkeypatch.setattr(
        collector,
        "_iter_discussion_messages",
        lambda client, context: [parent_comment, nested_reply, service_message],
    )

    manifest = collector.collect("tg-run-2", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.discussion_linked is True
    assert manifest.source.filtered_service_message_count == 1
    assert len(manifest.posts[0].comments) == 2
    assert manifest.posts[0].comments[0].depth == 0
    assert manifest.posts[0].comments[1].depth == 1
    assert manifest.posts[0].comments[1].parent_comment_id == manifest.posts[0].comments[0].comment_id
    assert manifest.posts[0].comments[1].reaction_breakdown_json == '{"🔥": 2}'


def test_telegram_collector_reaction_breakdown_and_media_type() -> None:
    config = _telegram_config()
    collector = TelegramMtprotoCollector(config)
    message = FakeMessage(
        id=7,
        date=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        reactions=FakeReactions(results=[FakeReactionResult(FakeReaction("👍"), 3)]),
        media=MessageMediaPhoto(),
    )

    breakdown = collector._reaction_breakdown(message)
    media_type = collector._media_type(message)

    assert breakdown == {"👍": 3}
    assert media_type == "photo"
