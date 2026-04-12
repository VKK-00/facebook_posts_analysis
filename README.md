# Social Posts Analysis

Local-first pipeline for collecting posts, replies, reposts, and discussion threads across social platforms, then analyzing narratives, stance, support, conflict, and propagation.

The project stores all outputs locally and is built around one configured source per run.

## Platform Support

Support is intentionally tiered. The project does not claim equal coverage across every network.

- `facebook`: supported. Meta API, public web, authenticated browser web, propagation via visible shares and share comments on a best-effort basis.
- `telegram`: supported. MTProto, public web, Bot API update queue, linked discussion trees, visible forwards on a best-effort basis.
- `x`: supported. Official X API v2, public web, quotes and reposts as first-class propagation objects.
- `threads`: beta. Threads API for owned-account scenarios is wired in, but public web coverage is still unstable and can yield zero posts for a valid public profile depending on the current Threads UI.
- `instagram`: beta. Instagram Graph API is wired in for owned professional accounts; public web collection currently behaves mostly as post-level extraction with shallow or empty public comments.

## What The Project Does

- collects source posts or messages for one configured account, page, or channel
- collects comments, replies, and discussion trees where the platform exposes them
- detects visible propagation instances such as shares, forwards, quotes, and repost-like copies
- stores raw snapshots under `data/raw/<run_id>/`
- normalizes data into parquet files and DuckDB tables
- detects language for `ru`, `uk`, and `en`
- clusters posts, propagations, and comments into narrative groups
- labels stance toward configured sides or actors
- computes support metrics for:
  - direct origin-post comments
  - propagation instances
  - aggregated `origin_plus_propagations`
- exports review files for manual corrections
- renders Markdown, HTML, CSV, and XLSX reports

## Project Layout

```text
config/project.yaml
src/social_posts_analysis/
  analysis/
  collectors/
  reporting/
tests/
```

## Requirements

- Python 3.12+
- recommended: `uv`
- Playwright plus Chromium for web collectors
- Meta token for Facebook API collection
- Telegram API credentials and an authorized session file for MTProto
- Telegram bot token for Bot API mode
- X bearer token for X API mode
- Threads access token for Threads API mode
- Instagram access token plus an owned professional account for Instagram Graph API mode

## Installation

Using `uv`:

```bash
uv venv
uv sync --extra dev
```

Using `pip`:

```bash
python -m venv .venv
```

Activate the environment:

- Windows PowerShell: `.venv\Scripts\Activate.ps1`
- macOS/Linux: `source .venv/bin/activate`

Then install the package:

```bash
python -m pip install -e .[dev]
```

If you plan to use web collectors:

```bash
python -m playwright install chromium
```

## Configuration

The checked-in [project.yaml](C:\Coding projects\facebook_posts_analysis\config\project.yaml) is a safe public template. For real runs, create a private local file such as `config/project.local.yaml` and pass it explicitly with `--config`.

Important top-level settings:

- `source.platform`: `facebook`, `telegram`, `x`, `threads`, or `instagram`
- `source.url`, `source.source_id`, or `source.source_name`
- `source.telegram.discussion_chat_id`
- `date_range.start` and `date_range.end`
- `collector.mode`: `api`, `web`, `hybrid`, `mtproto`, `bot_api`, `x_api`, `threads_api`, or `instagram_graph_api`
- `collector.multi_pass_runs`
- `collector.wait_between_passes_seconds`
- `normalization.merge_recent_runs`
- `normalization.source_run_ids`
- `sides`
- `providers.embeddings`
- `providers.llm`

Environment variables supported by default:

- `META_ACCESS_TOKEN`
- `SOCIAL_BROWSER_USER_DATA_DIR`
- `SOCIAL_BROWSER_PROFILE_DIRECTORY`
- `TELEGRAM_SESSION_FILE`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_BOT_TOKEN`
- `X_BEARER_TOKEN`
- `THREADS_ACCESS_TOKEN`
- `INSTAGRAM_ACCESS_TOKEN`
- `EMBEDDING_BASE_URL`
- `EMBEDDING_API_KEY`
- `LLM_BASE_URL`
- `LLM_API_KEY`

## Migration Note

The codebase used to be Facebook-only.

Existing configs must be migrated from:

- `page.url`
- `page.page_id`
- `page.page_name`

to:

- `source.platform: facebook`
- `source.url`
- `source.source_id`
- `source.source_name`

Package and CLI names were also changed:

- old package: `facebook_posts_analysis`
- new package: `social_posts_analysis`
- old CLI: `facebook-posts-analysis`
- new CLI: `social-posts-analysis`

No backward-compatible alias is kept for the old package or CLI names.

## Quickstart

1. Copy the public template:

```bash
cp config/project.yaml config/project.local.yaml
```

Windows PowerShell:

```powershell
Copy-Item config/project.yaml config/project.local.yaml
```

2. Edit `config/project.local.yaml`.
3. Run one of the modes below with `--config config/project.local.yaml`.

### Facebook Public Web

```yaml
source:
  platform: "facebook"
  url: "https://www.facebook.com/example-public-page/"

collector:
  mode: "web"
  meta_api:
    enabled: false
  public_web:
    enabled: true
    authenticated_browser:
      enabled: false
```

Run:

```bash
social-posts-analysis run-many --config config/project.local.yaml --passes 3
```

### Facebook Authenticated Web

```yaml
source:
  platform: "facebook"
  url: "https://www.facebook.com/example-public-page/"

collector:
  mode: "web"
  public_web:
    enabled: true
    authenticated_browser:
      enabled: true
      browser: "chrome"
      profile_directory: "Default"
      copy_profile: true
```

Optional environment variables:

```powershell
$env:SOCIAL_BROWSER_USER_DATA_DIR="C:\Users\<user>\AppData\Local\Google\Chrome\User Data"
$env:SOCIAL_BROWSER_PROFILE_DIRECTORY="Default"
```

If authenticated browser mode is enabled but Facebook still returns a login wall on detail pages, the collector now emits an explicit warning. In practice that means the selected Chrome/Edge profile launched successfully, but it does not appear to be logged in to Facebook for the target surface.

### Facebook Meta API

```yaml
source:
  platform: "facebook"
  source_id: "123456789"

collector:
  mode: "api"
  meta_api:
    enabled: true
```

```powershell
$env:META_ACCESS_TOKEN="your-token"
social-posts-analysis run-all --config config/project.local.yaml
```

### Telegram Public Web

```yaml
source:
  platform: "telegram"
  source_name: "example_channel"
  telegram:
    discussion_chat_id: "example_discussion"

collector:
  mode: "web"
  telegram_web:
    enabled: true
  telegram_mtproto:
    enabled: false
```

### Telegram MTProto

```yaml
source:
  platform: "telegram"
  source_name: "example_channel"

collector:
  mode: "mtproto"
  telegram_mtproto:
    enabled: true
    session_file: ".sessions/example_channel"
    api_id: 123456
    api_hash: "your-api-hash"
```

```powershell
$env:TELEGRAM_SESSION_FILE=".sessions/example_channel"
$env:TELEGRAM_API_ID="123456"
$env:TELEGRAM_API_HASH="your-api-hash"
social-posts-analysis run-all --config config/project.local.yaml
```

### Telegram Bot API

```yaml
source:
  platform: "telegram"
  source_name: "example_channel"
  telegram:
    discussion_chat_id: "-1001234567890"

collector:
  mode: "bot_api"
  telegram_bot_api:
    enabled: true
    bot_token: null
    consume_updates: false
```

```powershell
$env:TELEGRAM_BOT_TOKEN="123456:bot-token"
social-posts-analysis run-all --config config/project.local.yaml
```

### X Web

```yaml
source:
  platform: "x"
  source_name: "example_account"

collector:
  mode: "web"
  x_web:
    enabled: true
    authenticated_browser:
      enabled: false
```

### X API

```yaml
source:
  platform: "x"
  source_name: "example_account"

collector:
  mode: "x_api"
  x_api:
    enabled: true
    bearer_token: null
    search_scope: "recent"
```

```powershell
$env:X_BEARER_TOKEN="your-bearer-token"
social-posts-analysis run-all --config config/project.local.yaml
```

### Threads API

```yaml
source:
  platform: "threads"
  source_name: "example_account"

collector:
  mode: "threads_api"
  threads_api:
    enabled: true
    access_token: null
```

```powershell
$env:THREADS_ACCESS_TOKEN="your-threads-token"
social-posts-analysis run-all --config config/project.local.yaml
```

### Threads Web

```yaml
source:
  platform: "threads"
  source_name: "example_account"

collector:
  mode: "web"
  threads_web:
    enabled: true
```

Expected public-web shape today:

- posts: best-effort, can be `0` for a valid public profile
- comments/replies: often `0`
- propagations: only visible quote/repost surfaces
- warning: current public Threads UI can hide feed content from the scraper

### Instagram Graph API

```yaml
source:
  platform: "instagram"
  source_id: "17841400000000000"

collector:
  mode: "instagram_graph_api"
  instagram_graph_api:
    enabled: true
    access_token: null
```

```powershell
$env:INSTAGRAM_ACCESS_TOKEN="your-instagram-token"
social-posts-analysis run-all --config config/project.local.yaml
```

### Instagram Web

```yaml
source:
  platform: "instagram"
  source_name: "example_account"

collector:
  mode: "web"
  instagram_web:
    enabled: true
```

Expected public-web shape today:

- posts: usually available for public profiles
- comments/replies: often shallow or `0`
- propagations: only directly observable public surfaces
- warning: public comment visibility depends on the current Instagram web UI

## Usage

Full pipeline:

```bash
social-posts-analysis run-all --config config/project.local.yaml
```

Multi-pass full pipeline:

```bash
social-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Step by step:

```bash
social-posts-analysis collect --config config/project.local.yaml
social-posts-analysis normalize --config config/project.local.yaml --run-id <run_id>
social-posts-analysis analyze --config config/project.local.yaml --run-id <run_id>
social-posts-analysis review-export --config config/project.local.yaml --run-id <run_id>
social-posts-analysis report --config config/project.local.yaml --run-id <run_id>
social-posts-analysis export-tables --config config/project.local.yaml --run-id <run_id>
```

## CLI Commands

The package exposes the `social-posts-analysis` CLI with:

- `collect`
- `normalize`
- `analyze`
- `review-export`
- `report`
- `export-tables`
- `run-all`
- `run-many`

## Output Tables

Normalized tables:

- `posts.parquet`
- `propagations.parquet`
- `propagation_edges.parquet`
- `comments.parquet`
- `comment_edges.parquet`
- `authors.parquet`
- `media_refs.parquet`
- `collection_runs.parquet`

Analysis tables:

- `detected_languages.parquet`
- `cluster_memberships.parquet`
- `narrative_clusters.parquet`
- `stance_labels.parquet`
- `support_metrics.parquet`
- `analysis_runs.parquet`

Review files:

- `review/narrative_overrides.csv`
- `review/stance_overrides.csv`

Reports:

- `reports/report_<run_id>.md`
- `reports/report_<run_id>.html`
- `reports/report_<run_id>.xlsx`
- `reports/report_<run_id>_tables/*.csv`

## Private Local Files

These files or directories should stay local and should not be committed:

- `config/project.local.yaml`
- `data/`
- `reports/`
- `review/`
- local browser profile paths
- Meta, Telegram, X, Threads, Instagram, and provider secrets
- virtual environments and cache directories

## Testing

Run:

```bash
uv run ruff check .
uv run mypy src
uv run pytest -q
```

The current test suite covers:

- Facebook Meta API pagination, nested comments, and visible shares
- Facebook public-web parsing and timestamp handling
- Telegram MTProto source resolution, discussion collection, service-message filtering, and forward detection
- Telegram web and Bot API mappings
- X API replies plus quote/repost propagation
- Threads API and web payload mapping
- Instagram Graph API and web payload mapping
- normalization, propagation tables, and merged snapshots
- analysis helpers and support metrics
- review override application

## CI

GitHub Actions runs:

- `ruff check .`
- `mypy src`
- `pytest -q`

## Manual Acceptance Notes

Validated smoke runs in this repository session on April 11, 2026:

- `threads_web` against `https://www.threads.net/@zuck` completed successfully but returned `0` posts, `0` comments, and `0` propagations. The run status was `partial` and reported the expected best-effort public UI warning.
- `instagram_web` against `https://www.instagram.com/zuck/` completed successfully and returned `12` posts, `0` comments, and `0` propagations. The run status was `partial`; the report also detected `3` reels.

Not live-validated in this repository session:

- `threads_api`
- `instagram_graph_api`

Reason:

- the required access tokens were not configured in the local environment during this validation pass

## Coverage And Limits

Propagation coverage is asymmetric by design.

- strongest in this repository today: `facebook`, `telegram`, `x`
- beta or partial today: `threads`, `instagram`

Platform-specific limits:

- Facebook public-web collection is best-effort. The DOM can expose different content across runs.
- Facebook authenticated browser mode still only sees what the logged-in account can see.
- Facebook propagation coverage is limited to shares and visible reshared surfaces the collectors can actually discover.
- Telegram MTProto and web collection support one source channel per run, plus its linked discussion when visible.
- Telegram web collection only works for public `t.me/s/...` feeds.
- Telegram propagation coverage is limited to visible forwards or quoted surfaces available to the current collector.
- Telegram Bot API only sees updates currently available to the bot. It does not backfill history.
- X API reply coverage depends on the current search access window. With `search_scope: recent`, older replies can be missing.
- X web collection can scrape public profile posts, but public reply visibility is often shallower than the reply counter suggests unless an authenticated browser session is used.
- Threads API works best for owned-account scenarios. Threads web coverage is best-effort and, in the April 11, 2026 public smoke run against `@zuck`, it returned zero visible posts.
- Instagram Graph API works for owned professional-account scenarios. Instagram web coverage is best-effort and public comments are often shallow. In the April 11, 2026 public smoke run against `@zuck`, it returned 12 posts and zero extracted comments.
- Instagram propagation coverage is intentionally conservative and limited to surfaces that are directly observable.
- Heuristic fallback providers keep the pipeline usable offline, but proper embeddings and LLM providers produce better analytical quality.

## License

This project is licensed under the MIT License. See `LICENSE`.
