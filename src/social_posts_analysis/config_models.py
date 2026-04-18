from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, model_validator

from .config_env import env_int, env_value
from .config_validation import validate_project_config, validate_source_reference


class DateRangeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str | None = None
    end: str | None = None


class HistoryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    _active: bool = PrivateAttr(default=False)

    start: str | None = None
    end: str | None = None
    window: Literal["month"] = "month"
    max_windows: int = Field(default=240, gt=0)
    max_items_per_window: int = Field(default=5000, gt=0)
    max_comments_per_post: int = Field(default=5000, gt=0)
    resume: bool = True
    stop_on_error: bool = False

    @property
    def active(self) -> bool:
        return self._active

    @active.setter
    def active(self, value: bool) -> None:
        self._active = bool(value)


class TelegramSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discussion_chat_id: str | None = None


class WatchlistSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str | None = None
    source_id: str | None = None
    source_name: str | None = None
    source_type: str | None = None


class SourceSearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    include_posts: bool = True
    include_comments: bool = True
    max_items_per_query: int = 50
    queries: list[str] | None = None


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["feed", "person_monitor"] = "feed"
    platform: Literal["facebook", "telegram", "x", "threads", "instagram"] = "facebook"
    url: str | None = None
    source_id: str | None = None
    source_name: str | None = None
    aliases: list[str] = Field(default_factory=list)
    watchlist: list[WatchlistSourceConfig] = Field(default_factory=list)
    search: SourceSearchConfig = Field(default_factory=SourceSearchConfig)
    telegram: TelegramSourceConfig = Field(default_factory=TelegramSourceConfig)

    @model_validator(mode="after")
    def validate_source(self) -> "SourceConfig":
        validate_source_reference(self)
        return self


class SideConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    side_id: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    support_keywords: list[str] = Field(default_factory=list)
    oppose_keywords: list[str] = Field(default_factory=list)

    @property
    def all_names(self) -> list[str]:
        names = [self.name, *self.aliases]
        return [item.lower() for item in names if item]


class FacebookMetaApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    api_version: str = "v25.0"
    base_url: str = "https://graph.facebook.com"
    access_token: str | None = Field(default_factory=lambda: env_value("META_ACCESS_TOKEN"))
    page_size: int = 25
    timeout_seconds: float = 30.0
    max_retries: int = 3


class AuthenticatedBrowserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    browser: Literal["chrome", "edge", "custom"] = "chrome"
    user_data_dir: str | None = Field(default_factory=lambda: env_value("SOCIAL_BROWSER_USER_DATA_DIR"))
    profile_directory: str = Field(default_factory=lambda: env_value("SOCIAL_BROWSER_PROFILE_DIRECTORY") or "Default")
    copy_profile: bool = True
    temp_root_dir: str | None = None


class FacebookPublicWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 8
    wait_after_scroll_ms: int = 1500
    timeout_seconds: float = 30.0
    authenticated_browser: AuthenticatedBrowserConfig = Field(default_factory=AuthenticatedBrowserConfig)


class TelegramWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 6
    wait_after_scroll_ms: int = 1200
    timeout_seconds: float = 30.0


class TelegramMtprotoConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    session_file: str | None = Field(default_factory=lambda: env_value("TELEGRAM_SESSION_FILE"))
    api_id: int | None = Field(default_factory=lambda: env_int("TELEGRAM_API_ID"))
    api_hash: str | None = Field(default_factory=lambda: env_value("TELEGRAM_API_HASH"))
    page_size: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3


class TelegramBotApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: str = "https://api.telegram.org"
    bot_token: str | None = Field(default_factory=lambda: env_value("TELEGRAM_BOT_TOKEN"))
    update_limit: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3
    consume_updates: bool = False
    offset: int | None = None


class XApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    base_url: str = "https://api.x.com/2"
    bearer_token: str | None = Field(default_factory=lambda: env_value("X_BEARER_TOKEN"))
    page_size: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3
    search_scope: Literal["recent", "all"] = "recent"


class XWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 8
    wait_after_scroll_ms: int = 1500
    timeout_seconds: float = 30.0
    authenticated_browser: AuthenticatedBrowserConfig = Field(default_factory=AuthenticatedBrowserConfig)


class ThreadsApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    base_url: str = "https://graph.threads.net/v1.0"
    access_token: str | None = Field(default_factory=lambda: env_value("THREADS_ACCESS_TOKEN"))
    page_size: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3


class ThreadsWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 8
    wait_after_scroll_ms: int = 1500
    timeout_seconds: float = 30.0
    authenticated_browser: AuthenticatedBrowserConfig = Field(default_factory=AuthenticatedBrowserConfig)


class InstagramGraphApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    api_version: str = "v25.0"
    base_url: str = "https://graph.facebook.com"
    access_token: str | None = Field(default_factory=lambda: env_value("INSTAGRAM_ACCESS_TOKEN"))
    page_size: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3


class InstagramWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 8
    wait_after_scroll_ms: int = 1500
    timeout_seconds: float = 30.0
    authenticated_browser: AuthenticatedBrowserConfig = Field(default_factory=AuthenticatedBrowserConfig)


class CollectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal[
        "api",
        "web",
        "hybrid",
        "mtproto",
        "bot_api",
        "x_api",
        "threads_api",
        "instagram_graph_api",
    ] = "hybrid"
    multi_pass_runs: int = 1
    wait_between_passes_seconds: float = 0.0
    meta_api: FacebookMetaApiConfig = Field(default_factory=FacebookMetaApiConfig)
    public_web: FacebookPublicWebConfig = Field(default_factory=FacebookPublicWebConfig)
    telegram_web: TelegramWebConfig = Field(default_factory=TelegramWebConfig)
    telegram_mtproto: TelegramMtprotoConfig = Field(default_factory=TelegramMtprotoConfig)
    telegram_bot_api: TelegramBotApiConfig = Field(default_factory=TelegramBotApiConfig)
    x_api: XApiConfig = Field(default_factory=XApiConfig)
    x_web: XWebConfig = Field(default_factory=XWebConfig)
    threads_api: ThreadsApiConfig = Field(default_factory=ThreadsApiConfig)
    threads_web: ThreadsWebConfig = Field(default_factory=ThreadsWebConfig)
    instagram_graph_api: InstagramGraphApiConfig = Field(default_factory=InstagramGraphApiConfig)
    instagram_web: InstagramWebConfig = Field(default_factory=InstagramWebConfig)


class EmbeddingProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["auto", "openai_compatible", "hash"] = "auto"
    base_url: str | None = Field(default_factory=lambda: env_value("EMBEDDING_BASE_URL"))
    api_key: str | None = Field(default_factory=lambda: env_value("EMBEDDING_API_KEY"))
    model: str = "text-embedding-3-small"
    dimension: int = 256
    timeout_seconds: float = 45.0


class LLMProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["auto", "openai_compatible", "heuristic"] = "auto"
    base_url: str | None = Field(default_factory=lambda: env_value("LLM_BASE_URL"))
    api_key: str | None = Field(default_factory=lambda: env_value("LLM_API_KEY"))
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    timeout_seconds: float = 60.0


class ProvidersConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    embeddings: EmbeddingProviderConfig = Field(default_factory=EmbeddingProviderConfig)
    llm: LLMProviderConfig = Field(default_factory=LLMProviderConfig)


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    languages: list[str] = Field(default_factory=lambda: ["ru", "uk", "en"])
    min_cluster_size: int = 3
    min_samples: int = 1
    exemplar_count: int = 3
    batch_size: int = 25
    max_items_per_item_type: int | None = None


class NormalizationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    merge_recent_runs: int = 1
    source_run_ids: list[str] = Field(default_factory=list)


class PathsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    review_dir: str = "review"
    reports_dir: str = "reports"
    database_path: str = "data/processed/social_posts_analysis.duckdb"


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_name: str = "social_posts_analysis"
    source: SourceConfig
    date_range: DateRangeConfig = Field(default_factory=DateRangeConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    sides: list[SideConfig] = Field(default_factory=list)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    normalization: NormalizationConfig = Field(default_factory=NormalizationConfig)
    history: HistoryConfig = Field(default_factory=HistoryConfig)
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)

    @model_validator(mode="after")
    def validate_project(self) -> "ProjectConfig":
        validate_project_config(self)
        return self


def load_config(path: str | Path) -> ProjectConfig:
    config_path = Path(path)
    raw_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    try:
        return ProjectConfig.model_validate(raw_data)
    except ValidationError as exc:
        raise ValueError(f"Invalid config at {config_path}: {exc}") from exc
