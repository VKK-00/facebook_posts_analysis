from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


def _env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _env_int(name: str) -> int | None:
    value = _env(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


class DateRangeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str | None = None
    end: str | None = None


class TelegramSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discussion_chat_id: str | None = None


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: Literal["facebook", "telegram", "x", "threads", "instagram"] = "facebook"
    url: str | None = None
    source_id: str | None = None
    source_name: str | None = None
    telegram: TelegramSourceConfig = Field(default_factory=TelegramSourceConfig)

    @model_validator(mode="after")
    def validate_source_reference(self) -> "SourceConfig":
        has_reference = any([self.url, self.source_id, self.source_name])
        if not has_reference:
            raise ValueError("At least one of source.url, source.source_id, or source.source_name must be provided.")
        if self.platform == "facebook" and not (self.url or self.source_id):
            raise ValueError("Facebook source requires source.url or source.source_id.")
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
    access_token: str | None = Field(default_factory=lambda: _env("META_ACCESS_TOKEN"))
    page_size: int = 25
    timeout_seconds: float = 30.0
    max_retries: int = 3


class AuthenticatedBrowserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    browser: Literal["chrome", "edge", "custom"] = "chrome"
    user_data_dir: str | None = Field(default_factory=lambda: _env("SOCIAL_BROWSER_USER_DATA_DIR"))
    profile_directory: str = Field(default_factory=lambda: _env("SOCIAL_BROWSER_PROFILE_DIRECTORY") or "Default")
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
    session_file: str | None = Field(default_factory=lambda: _env("TELEGRAM_SESSION_FILE"))
    api_id: int | None = Field(default_factory=lambda: _env_int("TELEGRAM_API_ID"))
    api_hash: str | None = Field(default_factory=lambda: _env("TELEGRAM_API_HASH"))
    page_size: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3


class TelegramBotApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: str = "https://api.telegram.org"
    bot_token: str | None = Field(default_factory=lambda: _env("TELEGRAM_BOT_TOKEN"))
    update_limit: int = 100
    timeout_seconds: float = 30.0
    max_retries: int = 3
    consume_updates: bool = False
    offset: int | None = None


class XApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    base_url: str = "https://api.x.com/2"
    bearer_token: str | None = Field(default_factory=lambda: _env("X_BEARER_TOKEN"))
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
    access_token: str | None = Field(default_factory=lambda: _env("THREADS_ACCESS_TOKEN"))
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
    access_token: str | None = Field(default_factory=lambda: _env("INSTAGRAM_ACCESS_TOKEN"))
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
    base_url: str | None = Field(default_factory=lambda: _env("EMBEDDING_BASE_URL"))
    api_key: str | None = Field(default_factory=lambda: _env("EMBEDDING_API_KEY"))
    model: str = "text-embedding-3-small"
    dimension: int = 256
    timeout_seconds: float = 45.0


class LLMProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["auto", "openai_compatible", "heuristic"] = "auto"
    base_url: str | None = Field(default_factory=lambda: _env("LLM_BASE_URL"))
    api_key: str | None = Field(default_factory=lambda: _env("LLM_API_KEY"))
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
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)

    @model_validator(mode="after")
    def validate_project(self) -> "ProjectConfig":
        if not self.sides:
            raise ValueError("At least one side must be configured for stance analysis.")

        if self.source.platform == "telegram":
            if self.collector.mode not in {"mtproto", "web", "bot_api"}:
                raise ValueError("Telegram source requires collector.mode='mtproto', collector.mode='web', or collector.mode='bot_api'.")
            if self.collector.mode == "mtproto":
                if not self.collector.telegram_mtproto.enabled:
                    raise ValueError("Telegram source requires collector.telegram_mtproto.enabled=true.")
                telegram_config = self.collector.telegram_mtproto
                missing_fields: list[str] = []
                if not telegram_config.session_file:
                    missing_fields.append("collector.telegram_mtproto.session_file or TELEGRAM_SESSION_FILE")
                if telegram_config.api_id is None:
                    missing_fields.append("collector.telegram_mtproto.api_id or TELEGRAM_API_ID")
                if not telegram_config.api_hash:
                    missing_fields.append("collector.telegram_mtproto.api_hash or TELEGRAM_API_HASH")
                if missing_fields:
                    raise ValueError("Telegram source requires " + ", ".join(missing_fields) + ".")
            elif self.collector.mode == "bot_api":
                if not self.collector.telegram_bot_api.enabled:
                    raise ValueError("Telegram Bot API source requires collector.telegram_bot_api.enabled=true.")
                if not self.collector.telegram_bot_api.bot_token:
                    raise ValueError("Telegram Bot API source requires collector.telegram_bot_api.bot_token or TELEGRAM_BOT_TOKEN.")
            else:
                if not self.collector.telegram_web.enabled:
                    raise ValueError("Telegram web source requires collector.telegram_web.enabled=true.")
        elif self.source.platform == "x":
            if self.collector.mode not in {"x_api", "web"}:
                raise ValueError("X source requires collector.mode='x_api' or collector.mode='web'.")
            if self.collector.mode == "x_api":
                if not self.collector.x_api.enabled:
                    raise ValueError("X source requires collector.x_api.enabled=true.")
                if not self.collector.x_api.bearer_token:
                    raise ValueError("X source requires collector.x_api.bearer_token or X_BEARER_TOKEN.")
            else:
                if not self.collector.x_web.enabled:
                    raise ValueError("X web source requires collector.x_web.enabled=true.")
        elif self.source.platform == "threads":
            if self.collector.mode not in {"threads_api", "web"}:
                raise ValueError("Threads source requires collector.mode='threads_api' or collector.mode='web'.")
            if self.collector.mode == "threads_api":
                if not self.collector.threads_api.enabled:
                    raise ValueError("Threads source requires collector.threads_api.enabled=true.")
                if not self.collector.threads_api.access_token:
                    raise ValueError("Threads source requires collector.threads_api.access_token or THREADS_ACCESS_TOKEN.")
            else:
                if not self.collector.threads_web.enabled:
                    raise ValueError("Threads web source requires collector.threads_web.enabled=true.")
        elif self.source.platform == "instagram":
            if self.collector.mode not in {"instagram_graph_api", "web"}:
                raise ValueError("Instagram source requires collector.mode='instagram_graph_api' or collector.mode='web'.")
            if self.collector.mode == "instagram_graph_api":
                if not self.collector.instagram_graph_api.enabled:
                    raise ValueError("Instagram source requires collector.instagram_graph_api.enabled=true.")
                if not self.collector.instagram_graph_api.access_token:
                    raise ValueError(
                        "Instagram source requires collector.instagram_graph_api.access_token or INSTAGRAM_ACCESS_TOKEN."
                    )
                if not self.source.source_id:
                    raise ValueError("Instagram Graph API source requires source.source_id.")
            else:
                if not self.collector.instagram_web.enabled:
                    raise ValueError("Instagram web source requires collector.instagram_web.enabled=true.")
        else:
            if self.collector.mode == "mtproto":
                raise ValueError("Facebook source cannot use collector.mode='mtproto'.")
            if self.collector.mode == "bot_api":
                raise ValueError("Facebook source cannot use collector.mode='bot_api'.")
            if self.collector.mode == "x_api":
                raise ValueError("Facebook source cannot use collector.mode='x_api'.")
            if self.collector.mode == "threads_api":
                raise ValueError("Facebook source cannot use collector.mode='threads_api'.")
            if self.collector.mode == "instagram_graph_api":
                raise ValueError("Facebook source cannot use collector.mode='instagram_graph_api'.")
            if self.collector.mode == "api" and not self.collector.meta_api.enabled:
                raise ValueError("collector.meta_api.enabled must be true when collector.mode='api'.")
            if self.collector.mode == "web" and not self.collector.public_web.enabled:
                raise ValueError("collector.public_web.enabled must be true when collector.mode='web'.")
            if self.collector.mode == "hybrid" and not (
                self.collector.meta_api.enabled or self.collector.public_web.enabled
            ):
                raise ValueError("collector.hybrid requires at least one enabled Facebook collector.")

        return self


def load_config(path: str | Path) -> ProjectConfig:
    config_path = Path(path)
    raw_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    try:
        return ProjectConfig.model_validate(raw_data)
    except ValidationError as exc:
        raise ValueError(f"Invalid config at {config_path}: {exc}") from exc
