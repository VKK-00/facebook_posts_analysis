from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


def _env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


class DateRangeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str | None = None
    end: str | None = None


class PageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str | None = None
    page_id: str | None = None
    page_name: str | None = None

    @model_validator(mode="after")
    def validate_page_reference(self) -> "PageConfig":
        if not self.url and not self.page_id:
            raise ValueError("Either page.url or page.page_id must be provided.")
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


class MetaApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    api_version: str = "v25.0"
    base_url: str = "https://graph.facebook.com"
    access_token: str | None = Field(default_factory=lambda: _env("META_ACCESS_TOKEN"))
    page_size: int = 25
    timeout_seconds: float = 30.0
    max_retries: int = 3


class PublicWebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    browser_channel: str | None = None
    max_scrolls: int = 8
    wait_after_scroll_ms: int = 1500
    timeout_seconds: float = 30.0
    authenticated_browser: "AuthenticatedBrowserConfig" = Field(default_factory=lambda: AuthenticatedBrowserConfig())


class AuthenticatedBrowserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    browser: Literal["chrome", "edge", "custom"] = "chrome"
    user_data_dir: str | None = Field(default_factory=lambda: _env("FACEBOOK_BROWSER_USER_DATA_DIR"))
    profile_directory: str = Field(default_factory=lambda: _env("FACEBOOK_BROWSER_PROFILE_DIRECTORY") or "Default")
    copy_profile: bool = True
    temp_root_dir: str | None = None


class CollectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["api", "web", "hybrid"] = "hybrid"
    multi_pass_runs: int = 1
    wait_between_passes_seconds: float = 0.0
    meta_api: MetaApiConfig = Field(default_factory=MetaApiConfig)
    public_web: PublicWebConfig = Field(default_factory=PublicWebConfig)


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


class PathsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    review_dir: str = "review"
    reports_dir: str = "reports"
    database_path: str = "data/processed/facebook_posts_analysis.duckdb"


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_name: str = "facebook_posts_analysis"
    page: PageConfig
    date_range: DateRangeConfig = Field(default_factory=DateRangeConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    sides: list[SideConfig] = Field(default_factory=list)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    normalization: NormalizationConfig = Field(default_factory=NormalizationConfig)
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)

    @model_validator(mode="after")
    def validate_sides(self) -> "ProjectConfig":
        if not self.sides:
            raise ValueError("At least one side must be configured for stance analysis.")
        return self


def load_config(path: str | Path) -> ProjectConfig:
    config_path = Path(path)
    raw_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    try:
        return ProjectConfig.model_validate(raw_data)
    except ValidationError as exc:
        raise ValueError(f"Invalid config at {config_path}: {exc}") from exc
