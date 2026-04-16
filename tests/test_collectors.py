from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import social_posts_analysis.collectors.facebook_web_interactions as facebook_web_interactions
from social_posts_analysis.collectors.facebook_web_content import (
    author_exclusion_literals_lower,
    merge_extracted_comments,
)
from social_posts_analysis.collectors.facebook_web_extraction import extract_post_page
from social_posts_analysis.collectors.instagram_graph_api import InstagramGraphApiCollector
from social_posts_analysis.collectors.instagram_web import InstagramWebCollector
from social_posts_analysis.collectors.meta_api import MetaApiCollector
from social_posts_analysis.collectors.public_web import PublicWebCollector
from social_posts_analysis.collectors.telegram_bot_api import TelegramBotApiCollector
from social_posts_analysis.collectors.telegram_mtproto import DiscussionContext, TelegramMtprotoCollector
from social_posts_analysis.collectors.telegram_web import TelegramWebCollector
from social_posts_analysis.collectors.threads_api import ThreadsApiCollector
from social_posts_analysis.collectors.threads_web import ThreadsWebCollector
from social_posts_analysis.collectors.x_api import XApiCollector
from social_posts_analysis.collectors.x_web import XWebCollector
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import PostSnapshot, SourceSnapshot
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import stable_id


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
    yesterday_ua = PublicWebCollector._parse_post_timestamp("Вчора")
    yesterday_ru = PublicWebCollector._parse_post_timestamp("Вчера в 14:03")

    assert one_hour is not None
    assert one_day is not None
    assert calendar is not None
    assert localized == "2026-03-24T00:00:00+00:00"
    assert embedded == "2026-03-24T00:00:00+00:00"
    assert yesterday_ua is not None
    assert yesterday_ru is not None


def test_public_web_time_parser_avoids_deprecated_yearless_strptime_path() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        parsed = PublicWebCollector._parse_post_timestamp("March 15")

    assert parsed is not None


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


def test_public_web_resolves_visible_share_origin_post_id_for_same_page(project_config) -> None:
    project_config.source.url = "https://www.facebook.com/example-page/"
    collector = PublicWebCollector(project_config)

    origin_post_id = collector._resolve_visible_share_origin_post_id(
        page_id="page-token",
        origin_post_id="facebook:origin:99",
        origin_permalink="https://www.facebook.com/example-page/posts/99",
    )

    assert origin_post_id == stable_id("page-token", "https://www.facebook.com/example-page/posts/99")


def test_public_web_propagation_metadata_prefers_shared_permalink() -> None:
    propagation_kind, origin_post_id, origin_external_id, origin_permalink = PublicWebCollector._propagation_metadata(
        payload={
            "shared_permalink": "https://www.facebook.com/example-page/posts/99",
            "body_text": "Example Page shared a post.",
        },
        post_text="Example Page shared a post.",
        post_permalink="https://www.facebook.com/example-page/posts/100",
    )

    assert propagation_kind == "share"
    assert origin_post_id is None
    assert origin_external_id is None
    assert origin_permalink == "https://www.facebook.com/example-page/posts/99"


def test_public_web_metric_parser_reads_numeric_counts() -> None:
    assert PublicWebCollector._extract_metric_count("44") == 44
    assert PublicWebCollector._extract_metric_count("87 comments") == 87
    assert PublicWebCollector._extract_metric_count("Share") == 0


def test_public_web_extract_comment_count_reads_localized_surface_text() -> None:
    assert PublicWebCollector._extract_comment_count({"body_text": "99 comments"}) == 99
    assert PublicWebCollector._extract_comment_count({"body_text": "99 коментарів"}) == 99
    assert PublicWebCollector._extract_comment_count({"body_text": "Комментарии 42"}) == 42
    assert PublicWebCollector._extract_comment_count({"meta_description": "Коментарі: 12"}) == 12


def test_public_web_comment_article_limit_grows_for_larger_threads() -> None:
    assert PublicWebCollector._comment_article_limit(0, aggressive=False) == 220
    assert PublicWebCollector._comment_article_limit(35, aggressive=True) == 320
    assert PublicWebCollector._comment_article_limit(120, aggressive=True) == 420


def test_public_web_merge_extracted_comments_prefers_richer_reel_fallback_payload() -> None:
    merged = merge_extracted_comments(
        [
            {
                "text": "Comment body",
                "author_name": None,
                "permalink": "https://www.facebook.com/reel/1?comment_id=42&locale=en_US",
                "published_hint": "",
                "nesting_x": 320,
            }
        ],
        [
            {
                "text": "Author Name\\nComment body\\n1h\\n9",
                "author_name": "Author Name",
                "permalink": "https://www.facebook.com/reel/1?comment_id=42&__tn__=R",
                "published_hint": "1h",
                "nesting_x": 320,
            },
            {
                "text": "Second Author\\nSecond comment\\n2h",
                "author_name": "Second Author",
                "permalink": "https://www.facebook.com/reel/1?comment_id=99",
                "published_hint": "2h",
                "nesting_x": 320,
            },
        ],
        limit=10,
    )

    assert len(merged) == 2
    assert merged[0]["author_name"] == "Author Name"
    assert merged[0]["published_hint"] == "1h"
    assert merged[0]["permalink"] == "https://www.facebook.com/reel/1?comment_id=42"
    assert merged[1]["permalink"] == "https://www.facebook.com/reel/1?comment_id=99"


def test_public_web_prepare_post_detail_page_opens_reel_comment_surface(monkeypatch) -> None:
    calls: list[list[str]] = []

    class FakePage:
        url = "https://www.facebook.com/reel/1178401624169743"

    monkeypatch.setattr(facebook_web_interactions, "accept_desktop_cookies", lambda page: None)
    monkeypatch.setattr(
        facebook_web_interactions,
        "click_buttonish_text",
        lambda page, *, patterns, max_clicks, wait_ms: calls.append(patterns) or 0,
    )
    monkeypatch.setattr(facebook_web_interactions, "expand_comment_threads", lambda *args, **kwargs: None)

    facebook_web_interactions.prepare_post_detail_page(
        FakePage(),
        target_comment_count=12,
        aggressive=True,
    )

    assert any(any("All comments" in pattern or "Comments?" in pattern for pattern in group) for group in calls)


def test_public_web_payload_login_wall_detection_requires_multiple_markers() -> None:
    assert (
        PublicWebCollector._payload_looks_login_walled(
            {"body_text": "Log In\nForgot password?\nCreate new account\nSee more on Facebook"}
        )
        is True
    )
    assert PublicWebCollector._payload_looks_login_walled({"body_text": "Log In"}) is False


def test_public_web_expand_comment_threads_respects_zero_time_budget(monkeypatch) -> None:
    class FakeMouse:
        def __init__(self) -> None:
            self.wheels = 0

        def wheel(self, x: int, y: int) -> None:
            self.wheels += 1

    class FakePage:
        def __init__(self) -> None:
            self.mouse = FakeMouse()

        def wait_for_timeout(self, timeout_ms: int) -> None:
            return None

    monkeypatch.setattr(facebook_web_interactions, "count_article_nodes", lambda page: 0)
    monkeypatch.setattr(facebook_web_interactions, "scroll_primary_comment_container", lambda page: False)
    monkeypatch.setattr(facebook_web_interactions, "click_buttonish_text", lambda *args, **kwargs: 0)

    page = FakePage()
    facebook_web_interactions.expand_comment_threads(
        page,
        target_comment_count=100,
        aggressive=True,
        max_seconds=0.0,
    )

    assert page.mouse.wheels == 0


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


def test_public_web_clean_comment_text_removes_localized_reply_controls() -> None:
    raw = "Ольга Котенко відповіла\nВідповісти\n1 відповідь\n19h"

    cleaned = PublicWebCollector._clean_comment_text(raw, "Ольга Котенко відповіла", "19h")

    assert cleaned == ""


def test_public_web_select_comment_author_rejects_localized_control_values() -> None:
    author = PublicWebCollector._select_comment_author("Відповісти", "Відповісти\n1 відповідь\n19h")

    assert author is None


def test_public_web_derives_author_from_glued_comment_prefix() -> None:
    raw = "Таня ДонійСтудентство завжди було бунтівним, і як першокурсниця 1990 р."

    author = PublicWebCollector._derive_comment_author(raw)

    assert author == "Таня Доній"


def test_public_web_author_exclusion_literals_cover_localized_ui_controls() -> None:
    terms = author_exclusion_literals_lower()

    assert "\u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0441\u0442\u0438" in terms
    assert "\u043f\u043e\u0434\u043e\u0431\u0430\u0454\u0442\u044c\u0441\u044f" in terms
    assert "\u043e\u0442\u0432\u0435\u0442\u0438\u0442\u044c" in terms


def test_public_web_extract_post_page_uses_shared_author_filters_in_script() -> None:
    captured: dict[str, Any] = {}

    class FakePage:
        def evaluate(self, script: str, comment_limit: int) -> dict[str, Any]:
            captured["script"] = script
            captured["comment_limit"] = comment_limit
            return {"comments": [], "reel_fallback_comments": []}

    payload = extract_post_page(FakePage(), comment_limit=7)

    assert payload == {"comments": [], "reel_fallback_comments": []}
    assert captured["comment_limit"] == 7
    assert "authorControlTerms" in captured["script"]
    assert "authorTimePatterns" in captured["script"]
    assert "looksLikeTimeHint" in captured["script"]
    assert "extractCommentBodyText" in captured["script"]
    assert "timestampNode" in captured["script"]
    assert "raw_text:" in captured["script"]
    assert "\\u0432\\u0456\\u0434\\u043f\\u043e\\u0432\\u0456\\u0441\\u0442\\u0438" in captured["script"]


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


def test_public_web_build_comment_snapshots_uses_raw_text_for_fallbacks(project_config) -> None:
    collector = PublicWebCollector(project_config)

    comments = collector._build_comment_snapshots(
        post_id="post_2",
        raw_path="raw.json",
        payload_comments=[
            {
                "raw_text": "Марина Коваль\nВідповісти\nВчора о 14:03\nЩиро дякую",
                "text": "Щиро дякую",
                "author_name": None,
                "published_hint": "",
                "permalink": "https://facebook.com/post?comment_id=10",
                "nesting_x": 350,
            }
        ],
    )

    assert len(comments) == 1
    assert comments[0].author.name == "Марина Коваль"
    assert comments[0].message == "Щиро дякую"
    assert comments[0].created_at is not None


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
class FakeForwardInfo:
    saved_from_msg_id: int | None = None
    from_name: str | None = None
    saved_from_peer: object | None = None
    from_id: object | None = None


@dataclass
class FakePeerRef:
    channel_id: int | None = None
    chat_id: int | None = None
    user_id: int | None = None


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
    fwd_from: FakeForwardInfo | None = None
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


def _x_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "x", "source_name": "example_account"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "x_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "x_api": {
                    "enabled": True,
                    "bearer_token": "token",
                    "page_size": 100,
                    "search_scope": "recent",
                },
            },
        }
    )


def _telegram_web_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {
                "platform": "telegram",
                "source_name": "example_channel",
                "telegram": {"discussion_chat_id": "example_discussion"},
            },
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_web": {
                    "enabled": True,
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {"enabled": False},
            },
        }
    )


def _telegram_bot_api_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {
                "platform": "telegram",
                "source_name": "example_channel",
                "telegram": {"discussion_chat_id": "-100200"},
            },
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "bot_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_bot_api": {
                    "enabled": True,
                    "bot_token": "123:token",
                },
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {"enabled": False},
            },
        }
    )


def _x_web_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "x", "source_name": "example_account"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": False,
                    "session_file": None,
                    "api_id": None,
                    "api_hash": None,
                },
                "telegram_web": {"enabled": False},
                "x_api": {"enabled": False, "bearer_token": None},
                "x_web": {"enabled": True},
            },
        }
    )


def _threads_api_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "threads", "source_name": "example_account"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "threads_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {"enabled": False, "session_file": None, "api_id": None, "api_hash": None},
                "x_api": {"enabled": False, "bearer_token": None},
                "threads_api": {"enabled": True, "access_token": "token"},
                "threads_web": {"enabled": False},
            },
        }
    )


def _threads_web_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "threads", "source_name": "example_account"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {"enabled": False, "session_file": None, "api_id": None, "api_hash": None},
                "x_api": {"enabled": False, "bearer_token": None},
                "threads_api": {"enabled": False, "access_token": None},
                "threads_web": {"enabled": True},
            },
        }
    )


def _instagram_graph_api_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "instagram", "source_id": "17841400000000000"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "instagram_graph_api",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {"enabled": False, "session_file": None, "api_id": None, "api_hash": None},
                "x_api": {"enabled": False, "bearer_token": None},
                "instagram_graph_api": {"enabled": True, "access_token": "token"},
                "instagram_web": {"enabled": False},
            },
        }
    )


def _instagram_web_config() -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "instagram", "source_name": "example_account"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-09"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "web",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {"enabled": False, "session_file": None, "api_id": None, "api_hash": None},
                "x_api": {"enabled": False, "bearer_token": None},
                "instagram_graph_api": {"enabled": False, "access_token": None},
                "instagram_web": {"enabled": True},
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


def test_telegram_mtproto_search_discovery_filters_posts_and_comments(monkeypatch) -> None:
    collector = TelegramMtprotoCollector(_telegram_config())
    fake_client = SimpleNamespace(disconnect=lambda: None)
    external_channel = SimpleNamespace(
        id=2001,
        title="External Channel",
        username="external_channel",
        broadcast=True,
    )
    external_group = SimpleNamespace(
        id=2002,
        title="External Group",
        username="external_group",
        megagroup=True,
    )
    post_message = SimpleNamespace(
        id=501,
        date=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
        message="Mention in channel post",
        chat=external_channel,
    )
    comment_message = SimpleNamespace(
        id=502,
        date=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        message="Mention in group comment",
        chat=external_group,
        reply_to=FakeReplyTo(reply_to_msg_id=500),
    )

    monkeypatch.setattr(collector, "_open_client", lambda: fake_client)
    monkeypatch.setattr(
        collector,
        "_iter_person_monitor_search_messages",
        lambda client, *, query, max_items: [post_message, comment_message, post_message],
    )

    posts_only = collector.discover_person_monitor_sources(
        queries=["subject_handle"],
        include_posts=True,
        include_comments=False,
        max_items_per_query=25,
    )
    comments_only = collector.discover_person_monitor_sources(
        queries=["subject_handle"],
        include_posts=False,
        include_comments=True,
        max_items_per_query=25,
    )

    assert posts_only == [
        {
            "source_id": "external_channel",
            "source_name": "External Channel",
            "source_url": "https://t.me/external_channel",
            "source_type": "channel",
        }
    ]
    assert comments_only == [
        {
            "source_id": "external_group",
            "source_name": "External Group",
            "source_url": "https://t.me/external_group",
            "source_type": "group",
        }
    ]


def test_telegram_web_search_discovery_resolves_public_handles_and_search_urls() -> None:
    collector = TelegramWebCollector(_telegram_web_config())

    assert collector._resolve_search_discovery_url("@external_channel") == "https://t.me/s/external_channel"
    assert collector._resolve_search_discovery_url("https://t.me/external_channel") == "https://t.me/s/external_channel"
    assert (
        collector._resolve_search_discovery_url("https://t.me/s/external_channel?q=subject_handle")
        == "https://t.me/s/external_channel?q=subject_handle"
    )
    assert collector._resolve_search_discovery_url("subject handle mention") is None


def test_telegram_web_search_discovery_requires_hits_for_query_urls() -> None:
    collector = TelegramWebCollector(_telegram_web_config())

    assert (
        collector._discovery_payload_from_feed_payload(
            {
                "source_id": "external_channel",
                "source_name": "External Channel",
                "messages": [],
            },
            feed_url="https://t.me/s/external_channel?q=subject_handle",
        )
        is None
    )
    assert collector._discovery_payload_from_feed_payload(
        {
            "source_id": "external_channel",
            "source_name": "External Channel",
            "messages": [{"message_id": "1"}],
        },
        feed_url="https://t.me/s/external_channel?q=subject_handle",
    ) == {
        "source_id": "external_channel",
        "source_name": "External Channel",
        "source_url": "https://t.me/s/external_channel?q=subject_handle",
        "source_type": "channel",
    }


def test_telegram_mtproto_discussion_fallback_scan_includes_nested_replies() -> None:
    collector = TelegramMtprotoCollector(_telegram_config())
    discussion_entity = FakeChat(id=2002, title="Example Discussion")
    parent_comment = FakeMessage(
        id=71,
        date=datetime(2026, 4, 1, 10, 5, tzinfo=UTC),
        message="Top-level reply",
        reply_to=FakeReplyTo(reply_to_msg_id=700),
    )
    nested_reply = FakeMessage(
        id=72,
        date=datetime(2026, 4, 1, 10, 6, tzinfo=UTC),
        message="Nested reply",
        reply_to=FakeReplyTo(reply_to_msg_id=71, reply_to_top_id=700),
    )
    unrelated_reply = FakeMessage(
        id=73,
        date=datetime(2026, 4, 1, 10, 7, tzinfo=UTC),
        message="Other thread reply",
        reply_to=FakeReplyTo(reply_to_msg_id=999, reply_to_top_id=999),
    )

    class FakeClient:
        def iter_messages(self, chat, limit=None, reply_to=None, offset_date=None, reverse=None):  # noqa: ANN001, ANN002, ANN003, ANN202
            if chat != discussion_entity:
                return []
            if reply_to == 700:
                return [parent_comment]
            return [parent_comment, nested_reply, unrelated_reply]

    messages = list(
        collector._iter_discussion_messages(
            FakeClient(),
            DiscussionContext(chat=discussion_entity, root_message_id=700),
        )
    )

    assert [message.id for message in messages] == [71, 72]


def test_telegram_mtproto_discussion_scan_limit_grows_with_expected_comment_count() -> None:
    collector = TelegramMtprotoCollector(_telegram_config())
    discussion_entity = FakeChat(id=2002, title="Example Discussion")
    calls: list[dict[str, int | None]] = []

    class FakeClient:
        def iter_messages(self, chat, limit=None, reply_to=None, offset_date=None, reverse=None):  # noqa: ANN001, ANN002, ANN003, ANN202
            if chat == discussion_entity:
                calls.append({"limit": limit, "reply_to": reply_to})
            return []

    list(
        collector._iter_discussion_messages(
            FakeClient(),
            DiscussionContext(chat=discussion_entity, root_message_id=700, expected_comment_count=240),
        )
    )

    assert calls[0] == {"limit": collector.settings.page_size, "reply_to": 700}
    assert calls[1] == {"limit": 480, "reply_to": None}


def test_telegram_mtproto_collect_reorders_discussion_messages_to_preserve_parent_chain(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
        date=datetime(2026, 4, 1, 10, 6, tzinfo=UTC),
        message="Top-level reply",
        sender=FakeSender(id=501, first_name="Alice"),
        reply_to=FakeReplyTo(reply_to_msg_id=700),
    )
    nested_reply = FakeMessage(
        id=72,
        date=datetime(2026, 4, 1, 10, 5, tzinfo=UTC),
        message="Nested reply",
        sender=FakeSender(id=502, first_name="Bob"),
        reply_to=FakeReplyTo(reply_to_msg_id=71, reply_to_top_id=700),
    )
    fake_client = SimpleNamespace(disconnect=lambda: None)

    monkeypatch.setattr(collector, "_open_client", lambda: fake_client)
    monkeypatch.setattr(collector, "_resolve_source_entity", lambda client: source_entity)
    monkeypatch.setattr(collector, "_resolve_discussion_entity", lambda client, source: discussion_entity)
    monkeypatch.setattr(collector, "_iter_source_messages", lambda client, source: [post_message])
    monkeypatch.setattr(
        collector,
        "_fetch_discussion_context",
        lambda client, source, message: DiscussionContext(
            chat=discussion_entity,
            root_message_id=700,
            expected_comment_count=2,
        ),
    )
    monkeypatch.setattr(
        collector,
        "_iter_discussion_messages",
        lambda client, context: [nested_reply, parent_comment],
    )

    manifest = collector.collect("tg-run-reordered", RawSnapshotStore(tmp_path / "raw"))

    assert len(manifest.posts[0].comments) == 2
    assert manifest.posts[0].comments[0].depth == 0
    assert manifest.posts[0].comments[1].depth == 1
    assert manifest.posts[0].comments[1].parent_comment_id == manifest.posts[0].comments[0].comment_id


def test_x_api_collector_collects_posts_and_nested_replies(tmp_path: Path, monkeypatch) -> None:
    config = _x_config()
    collector = XApiCollector(config)

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        if endpoint == "/users/by/username/example_account":
            return {
                "data": {
                    "id": "42",
                    "name": "Example Account",
                    "username": "example_account",
                    "description": "Example source",
                    "public_metrics": {"followers_count": 321},
                }
            }
        if endpoint == "/users/42/tweets":
            return {
                "data": [
                    {
                        "id": "100",
                        "text": "Root X post about Actor A",
                        "created_at": "2026-04-08T10:00:00Z",
                        "conversation_id": "100",
                        "author_id": "42",
                        "attachments": {"media_keys": ["m1"]},
                        "public_metrics": {
                            "like_count": 7,
                            "retweet_count": 3,
                            "reply_count": 2,
                            "quote_count": 1,
                            "bookmark_count": 0,
                            "impression_count": 250,
                        },
                    }
                ],
                "includes": {
                    "users": [
                        {"id": "42", "name": "Example Account", "username": "example_account"},
                    ],
                    "media": [
                        {"media_key": "m1", "type": "photo", "url": "https://img.example/m1.jpg"},
                    ],
                },
                "meta": {"result_count": 1},
            }
        if endpoint == "/tweets/search/recent":
            return {
                "data": [
                    {
                        "id": "101",
                        "text": "I support Actor A",
                        "created_at": "2026-04-08T10:05:00Z",
                        "conversation_id": "100",
                        "author_id": "501",
                        "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                        "public_metrics": {"like_count": 2, "reply_count": 1, "retweet_count": 0, "quote_count": 0},
                    },
                    {
                        "id": "102",
                        "text": "I oppose that reply",
                        "created_at": "2026-04-08T10:06:00Z",
                        "conversation_id": "100",
                        "author_id": "502",
                        "referenced_tweets": [{"type": "replied_to", "id": "101"}],
                        "public_metrics": {"like_count": 1, "reply_count": 0, "retweet_count": 0, "quote_count": 0},
                    },
                ],
                "includes": {
                    "users": [
                        {"id": "501", "name": "Alice", "username": "alice"},
                        {"id": "502", "name": "Bob", "username": "bob"},
                    ]
                },
                "meta": {"result_count": 2},
            }
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, XApiCollector))
    manifest = collector.collect("x-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.platform == "x"
    assert manifest.source.source_type == "account"
    assert manifest.source.followers_count == 321
    assert len(manifest.posts) == 1
    assert manifest.posts[0].permalink == "https://x.com/example_account/status/100"
    assert manifest.posts[0].views == 250
    assert manifest.posts[0].shares == 3
    assert manifest.posts[0].forwards == 1
    assert manifest.posts[0].has_media is True
    assert len(manifest.posts[0].comments) == 2
    assert [comment.depth for comment in manifest.posts[0].comments] == [0, 1]
    assert manifest.posts[0].comments[1].parent_comment_id == manifest.posts[0].comments[0].comment_id


def test_x_api_collector_warns_when_quote_thread_has_reply_count_but_search_returns_no_replies(tmp_path: Path, monkeypatch) -> None:
    collector = XApiCollector(_x_config())

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        if endpoint == "/users/by/username/example_account":
            return {
                "data": {
                    "id": "42",
                    "name": "Example Account",
                    "username": "example_account",
                }
            }
        if endpoint == "/users/42/tweets":
            return {
                "data": [
                    {
                        "id": "100",
                        "text": "Quoted post",
                        "created_at": "2026-04-08T10:00:00Z",
                        "conversation_id": "100",
                        "author_id": "42",
                        "public_metrics": {
                            "like_count": 7,
                            "retweet_count": 3,
                            "reply_count": 2,
                            "quote_count": 1,
                        },
                        "referenced_tweets": [{"type": "quoted", "id": "555"}],
                    }
                ],
                "includes": {
                    "users": [
                        {"id": "42", "name": "Example Account", "username": "example_account"},
                        {"id": "77", "name": "Origin Author", "username": "origin_author"},
                    ],
                    "tweets": [
                        {"id": "555", "author_id": "77"},
                    ],
                },
                "meta": {"result_count": 1},
            }
        if endpoint == "/tweets/search/recent":
            return {"data": [], "includes": {"users": []}, "meta": {"result_count": 0}}
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, XApiCollector))
    manifest = collector.collect("x-quote-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert len(manifest.posts) == 1
    assert manifest.posts[0].is_propagation is True
    assert manifest.posts[0].propagation_kind == "quote"
    assert manifest.posts[0].comments == []
    assert any("quote thread" in warning and "reply_count 2" in warning for warning in manifest.warnings)


def test_x_api_recent_search_warning_for_old_start_date() -> None:
    config = _x_config()
    config.date_range.start = (datetime.now(tz=UTC) - timedelta(days=10)).date().isoformat()
    collector = XApiCollector(config)

    warnings = collector._search_window_warnings()

    assert warnings
    assert "search_scope='recent'" in warnings[0]


def test_telegram_bot_api_collector_maps_channel_posts_and_discussion_replies(tmp_path: Path, monkeypatch) -> None:
    collector = TelegramBotApiCollector(_telegram_bot_api_config())

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        assert endpoint == "/getUpdates"
        return {
            "ok": True,
            "result": [
                {
                    "update_id": 1,
                    "channel_post": {
                        "message_id": 10,
                        "date": 1775632800,
                        "chat": {"id": -100100, "type": "channel", "title": "Example Channel", "username": "example_channel"},
                        "text": "Channel post from bot updates",
                    },
                },
                {
                    "update_id": 2,
                    "message": {
                        "message_id": 100,
                        "message_thread_id": 10,
                        "date": 1775633100,
                        "chat": {"id": -100200, "type": "supergroup", "title": "Example Discussion"},
                        "from": {"id": 501, "first_name": "Alice", "username": "alice"},
                        "text": "Top-level discussion comment",
                    },
                },
                {
                    "update_id": 3,
                    "message": {
                        "message_id": 101,
                        "message_thread_id": 10,
                        "date": 1775633160,
                        "chat": {"id": -100200, "type": "supergroup", "title": "Example Discussion"},
                        "from": {"id": 502, "first_name": "Bob", "username": "bob"},
                        "text": "Nested discussion comment",
                        "reply_to_message": {"message_id": 100},
                    },
                },
            ],
        }

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, TelegramBotApiCollector))
    manifest = collector.collect("tg-bot-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.platform == "telegram"
    assert manifest.source.discussion_linked is True
    assert len(manifest.posts) == 1
    assert manifest.posts[0].message == "Channel post from bot updates"
    assert len(manifest.posts[0].comments) == 2
    assert manifest.posts[0].comments[0].depth == 0
    assert manifest.posts[0].comments[1].depth == 1
    assert manifest.posts[0].comments[1].parent_comment_id == manifest.posts[0].comments[0].comment_id


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


def test_telegram_web_collector_maps_public_discussion_comments(tmp_path: Path) -> None:
    collector = TelegramWebCollector(_telegram_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    source_payload = {
        "source_id": "example_channel",
        "source_name": "Example Channel",
        "source_url": "https://t.me/s/example_channel",
        "messages": [
            {
                "message_token": "example_channel/10",
                "message_id": "10",
                "permalink": "https://t.me/example_channel/10",
                "created_at": "2026-04-08T10:00:00+00:00",
                "text": "Root channel post",
                "views": "1.2K",
                "has_media": False,
                "media_type": None,
                "reply_text": "5 comments",
                "reaction_breakdown": {"emoji-1": 5},
            }
        ],
    }
    posts = collector._build_posts_from_payload(source_payload, raw_store)
    discussion_payload = {
        "source_id": "example_discussion",
        "messages": [
            {
                "message_token": "example_discussion/100",
                "message_id": "100",
                "permalink": "https://t.me/example_discussion/100",
                "created_at": "2026-04-08T10:05:00+00:00",
                "text": "Top-level discussion reply",
                "author_id": "alice",
                "author_name": "Alice",
                "reply_permalink": "https://t.me/example_channel/10",
                "reaction_breakdown": {"emoji-1": 2},
            },
            {
                "message_token": "example_discussion/101",
                "message_id": "101",
                "permalink": "https://t.me/example_discussion/101",
                "created_at": "2026-04-08T10:06:00+00:00",
                "text": "Nested discussion reply",
                "author_id": "bob",
                "author_name": "Bob",
                "reply_permalink": "https://t.me/example_discussion/100",
                "reaction_breakdown": {},
            },
        ],
    }

    merged = collector._attach_discussion_comments(
        posts=posts,
        posts_by_permalink={posts[0].permalink: posts[0]},
        discussion_payload=discussion_payload,
        raw_store=raw_store,
    )

    assert len(merged[0].comments) == 2
    assert posts[0].comments_count == 5
    assert merged[0].comments_count == 5
    assert merged[0].comments[0].depth == 0
    assert merged[0].comments[0].reply_to_message_id == "10"
    assert merged[0].comments[1].depth == 1
    assert merged[0].comments[1].parent_comment_id == merged[0].comments[0].comment_id
    assert merged[0].comments[1].reply_to_message_id == "100"


def test_telegram_web_collector_uses_forwarded_permalink_for_origin_post_id(tmp_path: Path) -> None:
    collector = TelegramWebCollector(_telegram_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    source_payload = {
        "source_id": "example_channel",
        "source_name": "Example Channel",
        "source_url": "https://t.me/s/example_channel",
        "messages": [
            {
                "message_token": "example_channel/11",
                "message_id": "11",
                "permalink": "https://t.me/example_channel/11",
                "created_at": "2026-04-08T10:00:00+00:00",
                "text": "Forwarded visible post",
                "views": "123",
                "has_media": False,
                "media_type": None,
                "author_name": "Example Channel",
                "forwarded_from_name": "Origin Channel",
                "forwarded_permalink": "https://t.me/origin_channel/99",
                "forwarded_message_id": "99",
                "reaction_breakdown": {},
            }
        ],
    }

    posts = collector._build_posts_from_payload(source_payload, raw_store)

    assert posts[0].is_propagation is True
    assert posts[0].origin_post_id == "telegram:origin_channel:99"
    assert posts[0].origin_external_id == "99"
    assert posts[0].origin_permalink == "https://t.me/origin_channel/99"


def test_x_web_collector_builds_posts_and_reply_snapshots(tmp_path: Path, monkeypatch) -> None:
    collector = XWebCollector(_x_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "200",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Visible X post",
                    "permalink": "https://x.com/example_account/status/200",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "reply_count": "12",
                    "retweet_count": "5",
                    "like_count": "33",
                    "view_count": "4.5K",
                    "has_media": True,
                    "media_type": "photo",
                }
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    class FakePage:
        def goto(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return None

        def close(self) -> None:
            return None

    class FakeContext:
        def new_page(self) -> FakePage:
            return FakePage()

    monkeypatch.setattr(collector, "_dismiss_cookie_banner", lambda page: None)
    monkeypatch.setattr(collector, "_scroll_timeline", lambda page, passes=None: None)
    monkeypatch.setattr(
        collector,
        "_extract_status_payload",
        lambda page: {
            "main_status_id": "200",
            "replies": [
                {
                    "status_id": "201",
                    "created_at": "2026-04-08T10:05:00Z",
                    "text": "Visible reply",
                    "permalink": "https://x.com/alice/status/201",
                    "author_name": "Alice",
                    "author_username": "alice",
                    "reply_to_status_id": "200",
                    "reply_count": "0",
                    "retweet_count": "1",
                    "like_count": "4",
                    "view_count": "120",
                },
                {
                    "status_id": "202",
                    "created_at": "2026-04-08T10:06:00Z",
                    "text": "Nested reply",
                    "permalink": "https://x.com/bob/status/202",
                    "author_name": "Bob",
                    "author_username": "bob",
                    "reply_to_status_id": "201",
                    "reply_count": "0",
                    "retweet_count": "0",
                    "like_count": "2",
                    "view_count": "80",
                }
            ],
        },
    )

    replies = collector._collect_replies_for_post(context=FakeContext(), post=posts[0], raw_store=raw_store)

    assert posts[0].comments_count == 12
    assert posts[0].views == 4500
    assert posts[0].shares == 5
    assert replies[0].parent_post_id == posts[0].post_id
    assert replies[0].reply_to_message_id == "200"
    assert replies[0].reactions == 4
    assert replies[0].depth == 0
    assert replies[1].reply_to_message_id == "201"
    assert replies[1].parent_comment_id == replies[0].comment_id
    assert replies[1].depth == 1


def test_x_web_search_surface_payloads_filter_posts_and_comments() -> None:
    collector = XWebCollector(_x_web_config())

    payload = {
        "results": [
            {
                "status_id": "101",
                "author_username": "external_a",
                "author_name": "External A",
                "created_at": "2026-04-08T10:00:00Z",
                "is_reply": False,
            },
            {
                "status_id": "102",
                "author_username": "external_b",
                "author_name": "External B",
                "created_at": "2026-04-08T10:05:00Z",
                "is_reply": True,
            },
            {
                "status_id": "103",
                "author_username": "external_a",
                "author_name": "External A",
                "created_at": "2026-04-08T10:10:00Z",
                "is_reply": False,
            },
        ]
    }

    posts_only = collector._search_surface_payloads(
        payload,
        include_posts=True,
        include_comments=False,
    )
    comments_only = collector._search_surface_payloads(
        payload,
        include_posts=False,
        include_comments=True,
    )

    assert posts_only == [
        {
            "source_id": "external_a",
            "source_name": "External A",
            "source_url": "https://x.com/external_a",
            "source_type": "account",
        }
    ]
    assert comments_only == [
        {
            "source_id": "external_b",
            "source_name": "External B",
            "source_url": "https://x.com/external_b",
            "source_type": "account",
        }
    ]


def test_x_web_collector_ignores_embedded_origin_status_in_quote_detail(tmp_path: Path, monkeypatch) -> None:
    collector = XWebCollector(_x_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "201",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Quoted X post",
                    "permalink": "https://x.com/example_account/status/201",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "reply_count": "2",
                    "retweet_count": "0",
                    "like_count": "3",
                    "view_count": "100",
                    "has_media": False,
                    "media_type": None,
                    "propagation_kind": "quote",
                    "origin_status_id": "555",
                    "origin_permalink": "https://x.com/origin_author/status/555",
                }
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    class FakePage:
        def goto(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return None

        def close(self) -> None:
            return None

    class FakeContext:
        def new_page(self) -> FakePage:
            return FakePage()

    monkeypatch.setattr(collector, "_dismiss_cookie_banner", lambda page: None)
    monkeypatch.setattr(collector, "_scroll_timeline", lambda page, passes=None: None)
    monkeypatch.setattr(
        collector,
        "_extract_status_payload",
        lambda page: {
            "main_status_id": "201",
            "replies": [
                {
                    "status_id": "555",
                    "created_at": "2026-04-08T10:00:30Z",
                    "text": "Embedded quoted origin",
                    "permalink": "https://x.com/origin_author/status/555",
                    "author_name": "Origin Author",
                    "author_username": "origin_author",
                    "reply_to_status_id": "",
                    "origin_status_id": "",
                    "propagation_kind": "",
                    "reply_count": "0",
                    "retweet_count": "0",
                    "like_count": "10",
                    "view_count": "300",
                },
                {
                    "status_id": "202",
                    "created_at": "2026-04-08T10:05:00Z",
                    "text": "Actual visible reply",
                    "permalink": "https://x.com/alice/status/202",
                    "author_name": "Alice",
                    "author_username": "alice",
                    "reply_to_status_id": "201",
                    "origin_status_id": "",
                    "propagation_kind": "",
                    "reply_count": "0",
                    "retweet_count": "0",
                    "like_count": "4",
                    "view_count": "120",
                },
            ],
        },
    )

    replies = collector._collect_replies_for_post(context=FakeContext(), post=posts[0], raw_store=raw_store)

    assert len(replies) == 1
    assert replies[0].comment_id.endswith(":202")
    assert replies[0].reply_to_message_id == "201"


def test_x_web_collector_builds_origin_post_id_from_origin_permalink(tmp_path: Path) -> None:
    collector = XWebCollector(_x_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")

    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "201",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Quoted X post",
                    "permalink": "https://x.com/example_account/status/201",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "reply_count": "1",
                    "retweet_count": "0",
                    "like_count": "3",
                    "view_count": "100",
                    "has_media": False,
                    "media_type": None,
                    "propagation_kind": "quote",
                    "origin_status_id": "555",
                    "origin_permalink": "https://x.com/origin_author/status/555",
                }
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    assert posts[0].is_propagation is True
    assert posts[0].origin_post_id == "x:origin_author:555"
    assert posts[0].origin_permalink == "https://x.com/origin_author/status/555"


def test_x_web_collector_filters_profile_feed_to_source_author(tmp_path: Path) -> None:
    collector = XWebCollector(_x_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")

    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "200",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Own post",
                    "permalink": "https://x.com/example_account/status/200",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "reply_count": "12",
                    "retweet_count": "5",
                    "like_count": "33",
                    "view_count": "4.5K",
                    "has_media": False,
                    "media_type": None,
                },
                {
                    "status_id": "201",
                    "created_at": "2026-04-08T10:10:00Z",
                    "text": "Affiliate post",
                    "permalink": "https://x.com/other_author/status/201",
                    "author_name": "Other Author",
                    "author_username": "other_author",
                    "reply_count": "1",
                    "retweet_count": "1",
                    "like_count": "1",
                    "view_count": "10",
                    "has_media": False,
                    "media_type": None,
                },
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    assert len(posts) == 1
    assert posts[0].permalink == "https://x.com/example_account/status/200"


def test_meta_api_collector_marks_shared_posts_as_propagation(project_config, tmp_path: Path, monkeypatch) -> None:
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
            }
        if endpoint == "/page_1/feed":
            return {
                "data": [
                    {
                        "id": "page_1_post_2",
                        "message": "Shared example",
                        "created_time": "2026-04-01T10:00:00+00:00",
                        "permalink_url": "https://facebook.com/posts/2",
                        "status_type": "shared_story",
                        "parent_id": "page_1_post_99",
                        "link": "https://facebook.com/origin/99",
                        "shares": {"count": 1},
                        "comments": {"summary": {"total_count": 0}},
                        "attachments": {"data": []},
                        "from": {"id": "page_1", "name": "Example Page"},
                    }
                ],
                "paging": {},
            }
        if endpoint == "/page_1_post_2/comments":
            return {"data": [], "paging": {}}
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, full_url={full_url}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, MetaApiCollector))
    manifest = collector.collect("run-share", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.posts[0].is_propagation is True
    assert manifest.posts[0].propagation_kind == "share"
    assert manifest.posts[0].origin_post_id == "page_1_post_99"
    assert manifest.posts[0].origin_permalink == "https://facebook.com/origin/99"


def test_telegram_collector_marks_forwarded_posts_as_propagation(tmp_path: Path) -> None:
    collector = TelegramMtprotoCollector(_telegram_config())
    source_entity = FakeChat(id=1001, title="Example Channel", username="example_channel")
    raw_store = RawSnapshotStore(tmp_path / "raw")
    message = FakeMessage(
        id=7,
        date=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        message="Forwarded channel post",
        fwd_from=FakeForwardInfo(saved_from_msg_id=99, from_name="Origin Channel"),
    )

    snapshot = collector._build_post_snapshot(message=message, source_entity=source_entity, raw_store=raw_store)

    assert snapshot.is_propagation is True
    assert snapshot.propagation_kind == "forward"
    assert snapshot.origin_post_id == "telegram:origin:99"
    assert snapshot.origin_external_id == "99"


def test_telegram_collector_uses_forward_peer_for_origin_post_id(tmp_path: Path) -> None:
    collector = TelegramMtprotoCollector(_telegram_config())
    source_entity = FakeChat(id=1001, title="Example Channel", username="example_channel")
    raw_store = RawSnapshotStore(tmp_path / "raw")
    message = FakeMessage(
        id=8,
        date=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        message="Forwarded channel post",
        fwd_from=FakeForwardInfo(saved_from_msg_id=99, saved_from_peer=FakePeerRef(channel_id=777001)),
    )

    snapshot = collector._build_post_snapshot(message=message, source_entity=source_entity, raw_store=raw_store)

    assert snapshot.is_propagation is True
    assert snapshot.origin_post_id == "telegram:777001:99"
    assert snapshot.origin_external_id == "99"


def test_x_api_collector_marks_quote_posts_as_propagation(tmp_path: Path) -> None:
    collector = XApiCollector(_x_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    source_snapshot = SourceSnapshot(
        platform="x",
        source_id="42",
        source_name="Example Account",
        source_url="https://x.com/example_account",
        source_type="account",
        source_collector="x_api",
        raw_path="source.json",
    )

    post = collector._build_post_snapshot(
        tweet_payload={
            "id": "100",
            "text": "Quoted post",
            "created_at": "2026-04-08T10:00:00Z",
            "conversation_id": "100",
            "public_metrics": {
                "like_count": 7,
                "retweet_count": 3,
                "reply_count": 2,
                "quote_count": 1,
            },
            "referenced_tweets": [{"type": "quoted", "id": "555"}],
        },
        includes={
            "users": {"77": {"id": "77", "username": "origin_author", "name": "Origin Author"}},
            "tweets": {"555": {"id": "555", "author_id": "77"}},
            "media": {},
        },
        source_snapshot=source_snapshot,
        raw_store=raw_store,
    )

    assert post.is_propagation is True
    assert post.propagation_kind == "quote"
    assert post.origin_post_id == "x:77:555"
    assert post.origin_permalink == "https://x.com/origin_author/status/555"


def test_threads_api_collector_collects_posts_and_replies(tmp_path: Path, monkeypatch) -> None:
    collector = ThreadsApiCollector(_threads_api_config())

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        if endpoint == "/profile_lookup":
            return {
                "id": "314",
                "username": "example_account",
                "name": "Example Account",
                "threads_biography": "Bio",
            }
        if endpoint == "/314/threads":
            return {
                "data": [
                    {
                        "id": "t100",
                        "text": "Quoted thread",
                        "timestamp": "2026-04-08T10:00:00+00:00",
                        "permalink": "https://www.threads.net/@example_account/post/t100",
                        "is_quote_post": True,
                        "quoted_post": "orig_55",
                        "has_replies": True,
                        "media_type": "TEXT_POST",
                    }
                ],
                "paging": {},
            }
        if endpoint == "/t100/conversation":
            return {
                "data": [
                    {
                        "id": "r1",
                        "text": "Top reply",
                        "username": "alice",
                        "timestamp": "2026-04-08T10:05:00+00:00",
                        "permalink": "https://www.threads.net/@alice/post/r1",
                        "replied_to": "t100",
                    },
                    {
                        "id": "r2",
                        "text": "Nested reply",
                        "username": "bob",
                        "timestamp": "2026-04-08T10:06:00+00:00",
                        "permalink": "https://www.threads.net/@bob/post/r2",
                        "replied_to": "r1",
                    },
                ]
            }
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, ThreadsApiCollector))
    manifest = collector.collect("threads-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.platform == "threads"
    assert len(manifest.posts) == 1
    assert manifest.posts[0].is_propagation is True
    assert manifest.posts[0].propagation_kind == "quote"
    assert manifest.posts[0].origin_post_id == "threads:origin:orig_55"
    assert [comment.depth for comment in manifest.posts[0].comments] == [0, 1]


def test_threads_web_collector_builds_posts_and_reply_snapshots(tmp_path: Path) -> None:
    collector = ThreadsWebCollector(_threads_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "200",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Visible threads post",
                    "raw_text": "Visible threads post\n12\n5",
                    "permalink": "https://www.threads.net/@example_account/post/200",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "reply_count": "12",
                    "repost_count": "5",
                    "like_count": "33",
                    "view_count": "4.5K",
                    "has_media": True,
                    "media_type": "photo",
                    "propagation_kind": "quote",
                    "origin_status_id": "150",
                    "origin_permalink": "https://www.threads.net/@origin/post/150",
                }
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    assert posts[0].is_propagation is True
    assert posts[0].origin_post_id == "threads:origin:150"
    assert posts[0].raw_text == "Visible threads post\n12\n5"
    assert posts[0].comments_count == 12
    assert posts[0].views == 4500


def test_threads_api_search_discovery_filters_posts_and_comments(monkeypatch) -> None:
    collector = ThreadsApiCollector(_threads_api_config())

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        assert endpoint == "/keyword_search"
        return {
            "data": [
                {
                    "id": "p1",
                    "username": "post_author",
                    "timestamp": "2026-04-08T10:00:00+00:00",
                    "permalink": "https://www.threads.net/@post_author/post/p1",
                    "is_reply": False,
                },
                {
                    "id": "c1",
                    "username": "reply_author",
                    "timestamp": "2026-04-08T11:00:00+00:00",
                    "permalink": "https://www.threads.net/@reply_author/post/c1",
                    "is_reply": True,
                },
            ],
            "paging": {},
        }

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, ThreadsApiCollector))

    post_surfaces = collector.discover_person_monitor_sources(
        queries=["openai"],
        include_posts=True,
        include_comments=False,
        max_items_per_query=20,
    )
    comment_surfaces = collector.discover_person_monitor_sources(
        queries=["openai"],
        include_posts=False,
        include_comments=True,
        max_items_per_query=20,
    )

    assert post_surfaces == [
        {
            "source_id": "post_author",
            "source_name": "post_author",
            "source_url": "https://www.threads.net/@post_author",
            "source_type": "account",
        }
    ]
    assert comment_surfaces == [
        {
            "source_id": "reply_author",
            "source_name": "reply_author",
            "source_url": "https://www.threads.net/@reply_author",
            "source_type": "account",
        }
    ]


def test_threads_web_search_surface_payloads_are_post_only_and_canonicalize_profiles() -> None:
    collector = ThreadsWebCollector(_threads_web_config())
    payload = {
        "results": [
            {
                "author_username": "ExternalA",
                "author_name": "ExternalA",
                "created_at": "2026-04-08T10:00:00Z",
                "text": "Result one",
                "permalink": "https://www.threads.com/@ExternalA/post/1",
                "is_reply": False,
            },
            {
                "author_username": "externala",
                "author_name": "ExternalA",
                "created_at": "2026-04-08T10:10:00Z",
                "text": "Duplicate author",
                "permalink": "https://www.threads.com/@ExternalA/post/2",
                "is_reply": False,
            },
        ]
    }

    post_surfaces = collector._search_surface_payloads(
        payload,
        include_posts=True,
        include_comments=False,
    )
    comment_only_surfaces = collector._search_surface_payloads(
        payload,
        include_posts=False,
        include_comments=True,
    )

    assert post_surfaces == [
        {
            "source_id": "externala",
            "source_name": "ExternalA",
            "source_url": "https://www.threads.net/@externala",
            "source_type": "account",
        }
    ]
    assert comment_only_surfaces == []


def test_threads_web_extract_visible_post_text_from_pressable_profile_card() -> None:
    raw_text = (
        "arianvzn\n"
        "1d\n"
        "CLAUDE CODE V2.1.100 SILENTLY ADDS 20,000 INVISIBLE TOKENS TO EVERY\n"
        "SINGLE REQUEST AND NO ONE TOLD YOU.\n"
        "Someone set up an HTTP\n"
        "proxy to catch it.\n"
        "Same project.\n"
        "Same prompt.\n"
        "Same account.\n"
        "v2.1.98: 49,726 tokens\n"
        "v2.1.100: 69,922 tokens\n"
        "The fix: npx claude-code@2.1.98\n"
        "120\n"
        "12\n"
        "5\n"
        "40"
    )

    extracted = ThreadsWebCollector._extract_visible_post_text(
        raw_text,
        author_username="arianvzn",
        author_name="arianvzn",
    )

    assert extracted == (
        "CLAUDE CODE V2.1.100 SILENTLY ADDS 20,000 INVISIBLE TOKENS TO EVERY\n"
        "SINGLE REQUEST AND NO ONE TOLD YOU.\n"
        "Someone set up an HTTP\n"
        "proxy to catch it.\n"
        "Same project.\n"
        "Same prompt.\n"
        "Same account.\n"
        "v2.1.98: 49,726 tokens\n"
        "v2.1.100: 69,922 tokens\n"
        "The fix: npx claude-code@2.1.98"
    )


def test_threads_web_merge_profile_post_candidates_uses_pressable_fallback_when_article_extract_is_empty() -> None:
    merged = ThreadsWebCollector._merge_profile_post_candidates(
        [
            {
                "status_id": "DXGnV_UDC9L",
                "created_at": "2026-04-14T10:00:00Z",
                "permalink": "https://www.threads.net/@arianvzn/post/DXGnV_UDC9L",
                "text": "",
                "author_name": "arianvzn",
                "author_username": "arianvzn",
                "reply_count": "",
                "repost_count": "",
                "like_count": "",
                "view_count": "",
                "has_media": False,
                "media_type": None,
            }
        ],
        [
            {
                "status_id": "DXGnV_UDC9L",
                "created_at": "2026-04-14T10:00:00Z",
                "permalink": "https://www.threads.net/@arianvzn/post/DXGnV_UDC9L",
                "text": "",
                "raw_text": "arianvzn\n1d\nImportant external post body\n120\n12\n5",
                "author_name": "arianvzn",
                "author_username": "arianvzn",
                "reply_count": "",
                "repost_count": "",
                "like_count": "",
                "view_count": "",
                "has_media": False,
                "media_type": None,
            }
        ],
    )

    assert len(merged) == 1
    assert merged[0]["text"] == "Important external post body"


def test_threads_web_extract_profile_payload_merges_pressable_profile_posts() -> None:
    collector = ThreadsWebCollector(_threads_web_config())

    class FakePage:
        def evaluate(self, script: str) -> dict[str, Any]:
            assert "pressablePosts" in script
            return {
                "source_name": "arianvzn",
                "source_id": "arianvzn",
                "source_url": "https://www.threads.com/@arianvzn",
                "posts": [],
                "pressable_posts": [
                    {
                        "status_id": "DXGnV_UDC9L",
                        "created_at": "2026-04-14T10:00:00Z",
                        "permalink": "https://www.threads.com/@arianvzn/post/DXGnV_UDC9L",
                        "origin_permalink": "",
                        "origin_status_id": "",
                        "propagation_kind": "",
                        "text": "",
                        "raw_text": "arianvzn\n1d\nImportant external post body\n120\n12\n5",
                        "author_name": "arianvzn",
                        "author_username": "arianvzn",
                        "reply_count": "",
                        "repost_count": "",
                        "like_count": "",
                        "view_count": "",
                        "has_media": False,
                        "media_type": None,
                    }
                ],
            }

    payload = collector._extract_profile_payload(FakePage())

    assert payload["source_id"] == "arianvzn"
    assert payload["posts"] == [
        {
            "status_id": "DXGnV_UDC9L",
            "created_at": "2026-04-14T10:00:00Z",
            "permalink": "https://www.threads.com/@arianvzn/post/DXGnV_UDC9L",
            "origin_permalink": "",
            "origin_status_id": "",
            "propagation_kind": "",
            "text": "Important external post body",
            "raw_text": "arianvzn\n1d\nImportant external post body\n120\n12\n5",
            "author_name": "arianvzn",
            "author_username": "arianvzn",
            "reply_count": "",
            "repost_count": "",
            "like_count": "",
            "view_count": "",
            "has_media": False,
            "media_type": None,
        }
    ]


def test_threads_web_extract_detail_payload_uses_pressable_rows_when_articles_are_missing() -> None:
    collector = ThreadsWebCollector(_threads_web_config())

    class FakePage:
        def evaluate(self, script: str) -> dict[str, Any]:
            assert "pressableRows" in script
            return {
                "main_status_id": "",
                "rows": [],
                "pressable_rows": [
                    {
                        "status_id": "DXGnV_UDC9L",
                        "reply_to_status_id": "",
                        "created_at": "2026-04-14T07:16:00Z",
                        "permalink": "https://www.threads.com/@arianvzn/post/DXGnV_UDC9L",
                        "text": "",
                        "raw_text": "arianvzn\n1d\nMain thread body\n122\n12\n5\n41",
                        "author_name": "arianvzn",
                        "author_username": "arianvzn",
                        "like_count": "",
                    },
                    {
                        "status_id": "DXGnp1fiCZk",
                        "reply_to_status_id": "DXG2amQDONO",
                        "created_at": "2026-04-14T07:18:42Z",
                        "permalink": "https://www.threads.com/@dinohensen/post/DXGnp1fiCZk",
                        "text": "",
                        "raw_text": "dinohensen\n1d\nStuff like this is why the harness exists\n11\n2",
                        "author_name": "dinohensen",
                        "author_username": "dinohensen",
                        "like_count": "",
                    },
                    {
                        "status_id": "DXG2amQDONO",
                        "reply_to_status_id": "",
                        "created_at": "2026-04-14T09:27:42Z",
                        "permalink": "https://www.threads.com/@mktpavlenko/post/DXG2amQDONO",
                        "text": "",
                        "raw_text": (
                            "mktpavlenko\n1d\n"
                            "20k invisible tokens is basically a system prompt that keeps growing every release.\n"
                            "classic case of the DX team not talking to the billing team\n40"
                        ),
                        "author_name": "mktpavlenko",
                        "author_username": "mktpavlenko",
                        "like_count": "",
                    },
                ],
            }

    payload = collector._extract_detail_payload(FakePage())

    assert payload["main_status_id"] == "DXGnV_UDC9L"
    assert payload["replies"] == [
        {
            "status_id": "DXG2amQDONO",
            "reply_to_status_id": "DXGnV_UDC9L",
            "created_at": "2026-04-14T09:27:42Z",
            "permalink": "https://www.threads.com/@mktpavlenko/post/DXG2amQDONO",
            "text": (
                "20k invisible tokens is basically a system prompt that keeps growing every release.\n"
                "classic case of the DX team not talking to the billing team"
            ),
            "raw_text": (
                "mktpavlenko\n1d\n"
                "20k invisible tokens is basically a system prompt that keeps growing every release.\n"
                "classic case of the DX team not talking to the billing team\n40"
            ),
            "author_name": "mktpavlenko",
            "author_username": "mktpavlenko",
            "like_count": "40",
        },
        {
            "status_id": "DXGnp1fiCZk",
            "reply_to_status_id": "DXG2amQDONO",
            "created_at": "2026-04-14T07:18:42Z",
            "permalink": "https://www.threads.com/@dinohensen/post/DXGnp1fiCZk",
            "text": "Stuff like this is why the harness exists",
            "raw_text": "dinohensen\n1d\nStuff like this is why the harness exists\n11\n2",
            "author_name": "dinohensen",
            "author_username": "dinohensen",
            "like_count": "2",
        },
    ]


def test_threads_web_order_detail_rows_only_reorders_when_parent_signal_exists() -> None:
    ordered = ThreadsWebCollector._order_detail_rows(
        [
            {
                "status_id": "DXGnV_UDC9L",
                "reply_to_status_id": "",
                "text": "Main post",
            },
            {
                "status_id": "DXGnp1fiCZk",
                "reply_to_status_id": "DXG2amQDONO",
                "text": "Nested reply",
            },
            {
                "status_id": "DXG2amQDONO",
                "reply_to_status_id": "DXGnV_UDC9L",
                "text": "Top reply",
            },
            {
                "status_id": "DXGorphan123",
                "reply_to_status_id": "DXGmissing456",
                "text": "Reply with unknown parent",
            },
        ],
        main_status_id="DXGnV_UDC9L",
    )

    assert [item["status_id"] for item in ordered] == [
        "DXGnV_UDC9L",
        "DXG2amQDONO",
        "DXGnp1fiCZk",
        "DXGorphan123",
    ]
    assert ordered[3]["reply_to_status_id"] == "DXGmissing456"


def test_threads_web_collect_replies_for_post_preserves_raw_text_and_parent_mapping(tmp_path: Path) -> None:
    collector = ThreadsWebCollector(_threads_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    post = PostSnapshot(
        post_id="threads:example_account:200",
        platform="threads",
        source_id="example_account",
        created_at="2026-04-08T10:00:00Z",
        message="Visible threads post",
        permalink="https://www.threads.net/@example_account/post/200",
        source_collector="threads_web",
    )

    class FakePage:
        def goto(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return None

        def close(self) -> None:
            return None

    class FakeContext:
        def new_page(self) -> FakePage:
            return FakePage()

    collector._scroll_timeline = lambda page, passes=None: None  # type: ignore[method-assign]
    collector._extract_detail_payload = lambda page: {  # type: ignore[method-assign]
        "main_status_id": "200",
        "replies": [
            {
                "status_id": "201",
                "reply_to_status_id": "200",
                "created_at": "2026-04-08T10:05:00Z",
                "text": "Top reply",
                "raw_text": "alice\n1d\nTop reply\n9",
                "permalink": "https://www.threads.net/@alice/post/201",
                "author_name": "alice",
                "author_username": "alice",
                "like_count": "9",
            },
            {
                "status_id": "202",
                "reply_to_status_id": "201",
                "created_at": "2026-04-08T10:06:00Z",
                "text": "Nested reply",
                "raw_text": "bob\n1d\nNested reply\n2",
                "permalink": "https://www.threads.net/@bob/post/202",
                "author_name": "bob",
                "author_username": "bob",
                "like_count": "2",
            },
        ],
    }  # type: ignore[method-assign]

    replies = collector._collect_replies_for_post(
        context=FakeContext(),
        post=post,
        raw_store=raw_store,
    )

    assert [reply.depth for reply in replies] == [0, 1]
    assert replies[0].raw_text == "alice\n1d\nTop reply\n9"
    assert replies[1].parent_comment_id == replies[0].comment_id


def test_instagram_graph_api_collector_collects_posts_and_nested_comments(tmp_path: Path, monkeypatch) -> None:
    collector = InstagramGraphApiCollector(_instagram_graph_api_config())

    def fake_get_json(self, endpoint, params=None):  # noqa: ANN001, ANN202
        if endpoint == "/v25.0/17841400000000000":
            return {
                "id": "17841400000000000",
                "username": "example_account",
                "name": "Example Account",
                "biography": "Bio",
                "followers_count": 10,
            }
        if endpoint == "/v25.0/17841400000000000/media":
            return {
                "data": [
                    {
                        "id": "media_1",
                        "caption": "Instagram post",
                        "media_type": "IMAGE",
                        "media_url": "https://img.example/media.jpg",
                        "permalink": "https://www.instagram.com/p/media_1/",
                        "timestamp": "2026-04-08T10:00:00+00:00",
                        "comments_count": 2,
                        "like_count": 7,
                    }
                ],
                "paging": {},
            }
        if endpoint == "/v25.0/media_1/comments":
            return {
                "data": [
                    {
                        "id": "c1",
                        "text": "Top level",
                        "timestamp": "2026-04-08T10:05:00+00:00",
                        "username": "alice",
                        "like_count": 2,
                        "replies": {
                            "data": [
                                {
                                    "id": "c2",
                                    "text": "Nested",
                                    "timestamp": "2026-04-08T10:06:00+00:00",
                                    "username": "bob",
                                    "like_count": 1,
                                }
                            ]
                        },
                    }
                ]
            }
        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    monkeypatch.setattr(collector, "_get_json", fake_get_json.__get__(collector, InstagramGraphApiCollector))
    manifest = collector.collect("ig-run-1", RawSnapshotStore(tmp_path / "raw"))

    assert manifest.source.platform == "instagram"
    assert len(manifest.posts) == 1
    assert manifest.posts[0].reactions == 7
    assert len(manifest.posts[0].comments) == 2
    assert manifest.posts[0].comments[1].parent_comment_id == manifest.posts[0].comments[0].comment_id


def test_instagram_web_collector_builds_posts_and_comment_snapshots(tmp_path: Path) -> None:
    collector = InstagramWebCollector(_instagram_web_config())
    raw_store = RawSnapshotStore(tmp_path / "raw")
    posts = collector._build_posts_from_payload(
        {
            "posts": [
                {
                    "status_id": "abc123",
                    "created_at": "2026-04-08T10:00:00Z",
                    "text": "Visible Instagram post",
                    "raw_text": "Visible Instagram post\n@subject_handle",
                    "permalink": "https://www.instagram.com/p/abc123/",
                    "author_name": "Example Account",
                    "author_username": "example_account",
                    "comment_count": "8",
                    "like_count": "120",
                    "has_media": True,
                    "media_type": "reel",
                }
            ]
        },
        source_id="example_account",
        source_name="Example Account",
        raw_store=raw_store,
    )

    class FakePage:
        def goto(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return None

        def close(self) -> None:
            return None

    class FakeContext:
        def new_page(self) -> FakePage:
            return FakePage()

    collector._scroll_timeline = lambda page, passes=None: None  # type: ignore[method-assign]
    collector._extract_post_payload = lambda page: {  # type: ignore[method-assign]
        "comments": [
            {
                "comment_id": "c1",
                "created_at": "2026-04-08T10:05:00Z",
                "text": "Top level",
                "raw_text": "Alice\nTop level\n@subject_handle",
                "author_name": "Alice",
                "author_username": "alice",
                "like_count": "2",
            },
            {
                "comment_id": "c2",
                "reply_to_comment_id": "c1",
                "created_at": "2026-04-08T10:06:00Z",
                "text": "Nested",
                "raw_text": "Bob\nNested\nhttps://www.instagram.com/subject_handle/",
                "author_name": "Bob",
                "author_username": "bob",
                "like_count": "1",
            },
        ]
    }  # type: ignore[method-assign]
    comments = collector._collect_comments_for_post(
        context=FakeContext(),
        post=posts[0],
        raw_store=raw_store,
    )

    assert posts[0].comments_count == 8
    assert posts[0].reactions == 120
    assert posts[0].raw_text == "Visible Instagram post\n@subject_handle"
    assert [comment.depth for comment in comments] == [0, 1]
    assert comments[0].raw_text == "Alice\nTop level\n@subject_handle"
    assert comments[1].raw_text == "Bob\nNested\nhttps://www.instagram.com/subject_handle/"
    assert comments[1].parent_comment_id == comments[0].comment_id
