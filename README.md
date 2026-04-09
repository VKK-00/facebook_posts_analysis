# Social Posts Analysis

Local-first Python pipeline for collecting posts and comments from multiple social platforms, then analyzing narratives, stance, support, and conflict patterns.

Current supported platforms:

- Facebook via Meta API or Playwright web collection
- Telegram via MTProto session access with one channel per run and optional linked discussion chat

Outputs are stored locally and can be reviewed in DuckDB, parquet, CSV, XLSX, Markdown, and HTML.

## What The Project Does

- collects posts/messages and comments/replies for one configured source per run
- stores raw snapshots per run under `data/raw/<run_id>/`
- normalizes collected data into parquet files and DuckDB tables
- detects language for `ru`, `uk`, and `en`
- groups posts and comments into narrative clusters
- labels stance toward configured sides or actors
- computes support metrics from comment/reply texts
- exports review files for manual corrections
- renders Markdown, HTML, CSV, and XLSX reports

Current codebase additions include:

- platform-aware `source` model instead of Facebook-only `page` naming
- Telegram MTProto collector with linked-discussion support
- authenticated browser profile support for Facebook web collection
- multi-pass collection runs
- merged normalized snapshots from several source runs
- reply-depth extraction for both Facebook and Telegram discussion trees
- coverage-gap reporting for posts where visible counters exceed extracted text comments

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
- Playwright plus Chromium if using Facebook web collection
- a valid Meta token if using the Facebook API collector
- Telegram API credentials and an authorized session file if using Telegram MTProto

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

If you plan to use Playwright:

```bash
python -m playwright install chromium
```

## Configuration

The checked-in `config/project.yaml` is a safe public template. Treat it as an example, not as a real working target-specific config.

For actual runs, create a private local file such as `config/project.local.yaml` and pass it explicitly with `--config`. That local file should contain the real source target, date range, browser profile paths, API tokens, MTProto session path, and provider settings.

Important top-level settings:

- `source.platform`: `facebook` or `telegram`
- `source.url`, `source.source_id`, or `source.source_name`
- `source.telegram.discussion_chat_id`
- `date_range.start` and `date_range.end`
- `collector.mode`: `api`, `web`, `hybrid`, or `mtproto`
- `collector.multi_pass_runs`
- `collector.wait_between_passes_seconds`
- `normalization.merge_recent_runs`
- `normalization.source_run_ids`
- `sides`: stance targets
- `providers.embeddings` and `providers.llm`

Environment variables supported by default:

- `META_ACCESS_TOKEN`
- `SOCIAL_BROWSER_USER_DATA_DIR`
- `SOCIAL_BROWSER_PROFILE_DIRECTORY`
- `TELEGRAM_SESSION_FILE`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `EMBEDDING_BASE_URL`
- `EMBEDDING_API_KEY`
- `LLM_BASE_URL`
- `LLM_API_KEY`

## Migration Note

This repository used to be Facebook-only. Existing configs must be migrated from:

- `page.url`, `page.page_id`, `page.page_name`

to:

- `source.platform: facebook`
- `source.url`, `source.source_id`, `source.source_name`

The package and CLI were also renamed:

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

### Facebook Public Web Collection

Use this when you want public-only scraping without a logged-in browser.

Minimal settings:

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

Expected result:

- posts and visible comments collected from public Facebook sources
- best-effort coverage only
- repeated passes can improve coverage

### Facebook Authenticated Browser Collection

Use this when the public DOM is too shallow and your local browser session can see more content.

Minimal settings:

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

```bash
export SOCIAL_BROWSER_USER_DATA_DIR="/path/to/browser/User Data"
export SOCIAL_BROWSER_PROFILE_DIRECTORY="Default"
```

Windows PowerShell:

```powershell
$env:SOCIAL_BROWSER_USER_DATA_DIR="C:\Users\<user>\AppData\Local\Google\Chrome\User Data"
$env:SOCIAL_BROWSER_PROFILE_DIRECTORY="Default"
```

Run:

```bash
social-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Expected result:

- deeper comment extraction than public-only mode
- still limited to what the logged-in account can see
- no credentials stored in the project

### Facebook Meta API Collection

Use this when the target object and Meta permissions allow API access.

Minimal settings:

```yaml
source:
  platform: "facebook"
  source_id: "123456789"

collector:
  mode: "api"
  meta_api:
    enabled: true
```

Environment variable:

```bash
export META_ACCESS_TOKEN="your-token"
```

Windows PowerShell:

```powershell
$env:META_ACCESS_TOKEN="your-token"
```

Run:

```bash
social-posts-analysis run-all --config config/project.local.yaml
```

Expected result:

- more stable structure than web scraping
- still depends on Meta permissions and target object type

### Telegram MTProto Collection

Use this when you want one Telegram channel per run, optionally with its linked discussion chat.

Minimal settings:

```yaml
source:
  platform: "telegram"
  source_name: "example_channel"
  telegram:
    discussion_chat_id: null

collector:
  mode: "mtproto"
  meta_api:
    enabled: false
  public_web:
    enabled: false
  telegram_mtproto:
    enabled: true
    session_file: ".sessions/example_channel"
    api_id: 123456
    api_hash: "your-api-hash"
```

Environment variables:

```bash
export TELEGRAM_SESSION_FILE=".sessions/example_channel"
export TELEGRAM_API_ID="123456"
export TELEGRAM_API_HASH="your-api-hash"
```

Windows PowerShell:

```powershell
$env:TELEGRAM_SESSION_FILE=".sessions/example_channel"
$env:TELEGRAM_API_ID="123456"
$env:TELEGRAM_API_HASH="your-api-hash"
```

Run:

```bash
social-posts-analysis run-all --config config/project.local.yaml
```

Expected result:

- channel posts collected in the configured date range
- linked discussion comments/replies collected when the discussion chat exists and is visible to the session
- service messages filtered but counted in metadata
- if there is no linked discussion chat, the run still succeeds with posts only and a warning

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

If you use another config path, replace `config/project.local.yaml` in the commands above.

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

`run-many` is most useful for unstable Facebook web collection, because the public DOM can reveal slightly different content across repeated passes.

## Output Tables

Normalized tables:

- `posts.parquet`
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
- Meta tokens, Telegram credentials, and provider keys
- virtual environments and cache directories

## Testing

Run:

```bash
uv run ruff check .
uv run mypy src
uv run pytest -q
```

The current test suite covers:

- Facebook Meta API pagination and nested comments
- Facebook public-web parsing and timestamp handling
- Telegram MTProto source resolution, discussion collection, service-message filtering, and reaction parsing
- normalization and merged snapshots
- analysis helpers and support metrics
- review override application
- collection fallback and multi-pass behavior

## CI

GitHub Actions runs:

- `ruff check .`
- `mypy src`
- `pytest -q`

## Important Limits

- Facebook public-web collection is best-effort. The DOM can expose different content across runs.
- Facebook authenticated browser mode still only sees what the logged-in account can see.
- Telegram v1 supports one channel per run, plus its linked discussion chat when it exists.
- Telegram Bot API and Telegram web scraping are intentionally out of scope for v1.
- Some posts may still show a visible comment counter while not yielding full text comments in the DOM.
- API-first Facebook collection depends on the current Meta permission model and the target object type.
- Heuristic fallback providers keep the pipeline usable offline, but proper embeddings and LLM providers produce better analytical quality.

## License

This project is licensed under the MIT License. See `LICENSE`.
