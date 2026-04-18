# Social Posts Analysis

Local-first pipeline for collecting posts, replies, reposts, and discussion threads across social platforms, then analyzing narratives, stance, support, conflict, and propagation.

The project stores all outputs locally and is built around one configured source per run.

## Platform Support

Support is intentionally tiered. The project does not claim equal coverage across every network.

- `facebook`: supported. Meta API, public web, authenticated browser web, propagation via visible shares and share comments on a best-effort basis. Facebook public web now also preserves localized visible comment counters from detail pages even when extracted comment snapshots are shallower than the visible UI count, and strips more localized UI/control lines out of extracted comment snapshots.
- `telegram`: supported. MTProto, public web, Bot API update queue, linked discussion trees, visible forwards on a best-effort basis. Telegram web now preserves visible discussion counters from the post surface even before a separate public discussion feed is attached. Telegram MTProto now reorders nested discussion messages by parent chain and grows its fallback scan using the visible thread size when that signal is available.
- `x`: supported. Official X API v2, public web, quotes and reposts as first-class propagation objects. X web now filters embedded quoted/origin status cards out of detail-page reply extraction.
- `threads`: beta. Threads API for owned-account scenarios is wired in, but public web coverage is still unstable and can yield zero posts for a valid public profile depending on the current Threads UI.
- `instagram`: beta. Instagram Graph API is wired in for owned professional accounts; public web collection currently behaves mostly as post-level extraction with shallow or empty public comments.

## What The Project Does

- collects source posts or messages for one configured account, page, or channel
- collects comments, replies, and discussion trees where the platform exposes them
- preserves visible discussion/comment counters where the platform exposes them even if full comment text extraction is partial
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
- exports a stable OpenClaw JSON bundle for agent or chatbot handoff through local files
- runs monthly historical backfills for a profile/source and builds timeline, narrative, stance, and audience-comment trend tables

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

Path resolution for `paths.*` is intentionally simple:

- when the config file lives under a `config/` directory, relative output paths are resolved from the project root
- when the config file lives somewhere else, relative output paths are resolved from that config file directory
- if you generate temporary configs outside the repository, prefer absolute `paths.raw_dir`, `paths.processed_dir`, `paths.review_dir`, `paths.reports_dir`, and `paths.database_path`

The CLI now emits an explicit warning for the second case so that temp-config runs do not silently write outputs into an unexpected directory.

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
- `history.start`, `history.end`, `history.window`
- `history.max_windows`, `history.max_items_per_window`, `history.max_comments_per_post`
- `history.resume`, `history.stop_on_error`
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

### Instagram Web Auth Preflight

Use `doctor-instagram-web` before spending time on Instagram selector tuning. It checks whether the configured Chrome or Edge profile actually opens Instagram as a logged-in session and writes a diagnostic JSON file to `data/raw/_diagnostics/<run_id>/instagram_web_session.json`.

```powershell
$env:SOCIAL_BROWSER_USER_DATA_DIR="C:\Users\<user>\AppData\Local\Google\Chrome\User Data"
$env:SOCIAL_BROWSER_PROFILE_DIRECTORY="Default"
social-posts-analysis doctor-instagram-web --config config/project.local.yaml --target-url https://www.instagram.com/nasa/
```

Relevant config:

```yaml
collector:
  mode: "web"
  instagram_web:
    enabled: true
    headless: true
    authenticated_browser:
      enabled: true
      browser: "chrome"
      profile_directory: "Default"
      copy_profile: true
```

Use `SOCIAL_BROWSER_USER_DATA_DIR` for the browser user-data root, not the nested profile folder. For Chrome this is usually `C:\Users\<user>\AppData\Local\Google\Chrome\User Data`; the profile name, such as `Default` or `Profile 1`, belongs in `SOCIAL_BROWSER_PROFILE_DIRECTORY`.

If you put a Windows path directly in YAML instead of an environment variable, wrap it in single quotes. For example: `user_data_dir: 'C:\Users\<user>\AppData\Local\Google\Chrome\User Data'`. Double-quoted YAML treats backslashes as escape sequences and can fail before the browser starts.

If the diagnostic returns `status: "login_wall"`, the selected profile launched but Instagram still showed login/signup UI. Try a profile that is visibly logged in to Instagram, or temporarily set `instagram_web.headless: false` to validate the session in a visible browser window. Keep `copy_profile: true` when using a normal daily browser profile: it scans a temporary snapshot instead of attaching directly to the profile that Chrome may already have open.

If the diagnostic returns `status: "content_visible"` with `body_text_length: 0`, check `extraction_sources.media_candidates`, `extraction_sources.comment_candidates`, and `serialized_candidates`. Those fields show whether Instagram exposed usable post/comment-like objects in serialized JSON even when the visible DOM is empty.

If `json_script_blocks` is greater than zero but candidate counts are `0`, check `serialized_structure`. It stores a redacted structure map: top-level JSON types/keys, frequent nested key paths, marker keys like `__typename` or `__bbox`, and small shape samples. It intentionally does not store full raw JSON values.

For detail URLs such as `/p/<shortcode>/` or `/reel/<shortcode>/`, also check `target_status_id`, `extraction_sources.target_media_candidates`, and `extraction_sources.other_media_candidates`. Instagram can expose unrelated recommended media in the same page scripts; only `target_media_candidates > 0` indicates that the serialized payload appears to include the requested post itself.

Also check `target_author_username` and warnings. If the target author does not match the configured Instagram source, the detail URL likely belongs to a different profile even if the shortcode resolved successfully.

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
social-posts-analysis openclaw-export --config config/project.local.yaml --run-id <run_id>
```

Historical monthly backfill:

```bash
social-posts-analysis history-run --config config/project.local.yaml --history-run-id <history_run_id>
social-posts-analysis history-report --config config/project.local.yaml --history-run-id <history_run_id>
social-posts-analysis openclaw-export --config config/project.local.yaml --history-run-id <history_run_id>
```

### Historical Profile Intelligence

`history-run` creates monthly child runs with ids in the form `<history_run_id>__YYYYMM`. Each child run uses the existing collect and normalize path with a temporary monthly `date_range.start` and `date_range.end`. The parent history manifest is written to `data/raw/_history/<history_run_id>/manifest.json`.

Example config:

```yaml
history:
  start: "2026-01-01"
  end: null
  window: month
  max_windows: 240
  max_items_per_window: 5000
  max_comments_per_post: 5000
  resume: true
  stop_on_error: false
```

In v1, the production-quality historical collector target is `telegram_mtproto`. For Telegram MTProto, `history.start: null` means the collector tries to discover the oldest accessible source message and starts from that month. For web collectors, set `history.start` explicitly; if it is missing, `history-run` fails before collection because deep web history is not stable yet.

`history-report` runs the historical analysis step and writes:

- `reports/history/<history_run_id>/history_report.md`
- `reports/history/<history_run_id>/history_report.html`
- `reports/history/<history_run_id>/tables/*.csv`

The report includes a comment coverage score for each month. The score is `extracted_comments / visible_comments`, where `visible_comments` is the extracted count plus the known visible-vs-extracted gap. A failed monthly window gets `coverage_score=0.0`; a successful month with no visible comments gets `coverage_score=1.0`.

The report also lists top turning-point months from real month-over-month stance changes. These rows compare neighboring monthly `stance` metrics for the same item type and side, then sort by the absolute `net_support_delta`.

The historical tables are separate from the current snapshot tables. Existing `posts`, `comments`, `propagations`, `match_hits`, and `observed_sources` semantics are not changed.

### OpenClaw File Export

`openclaw-export` is a read-only integration layer. It does not collect, normalize, analyze, or rerun the pipeline. It reads an existing run from `data/raw/<run_id>/`, optional parquet/DuckDB tables under `data/processed/`, and optional report exports under `reports/`.

```powershell
social-posts-analysis openclaw-export --config config/project.local.yaml --run-id <run_id>
```

Outputs:

- `reports/openclaw/<run_id>/bundle.json`
- `reports/openclaw/<run_id>/brief.md`

The JSON bundle uses schema version `openclaw.social_posts_analysis.v1` and includes run metadata, source/platform/collector status, local artifact paths, counts, warnings with `source_run_id` when available, person-monitor `observed_sources` and `match_hits` summaries, coverage gaps, and deterministic next actions.

For historical runs, use:

```powershell
social-posts-analysis openclaw-export --config config/project.local.yaml --history-run-id <history_run_id>
```

Historical bundles use schema version `openclaw.social_posts_analysis.history.v1` and include monthly windows with `coverage_score`, temporal metrics, narrative clusters, month-over-month stance shifts, coverage summary, coverage gaps, report paths, and deterministic next actions. `--run-id` and `--history-run-id` are mutually exclusive.

This v1 contract is intentionally `CLI + files`. It does not start an HTTP server, webhook listener, MCP server, browser session, or Claude/OpenClaw API call. OpenClaw can run the CLI as a local process and then read `bundle.json` without knowing the internal repository layout.

## CLI Commands

The package exposes the `social-posts-analysis` CLI with:

- `collect`
- `normalize`
- `analyze`
- `review-export`
- `report`
- `export-tables`
- `openclaw-export`
- `doctor-instagram-web`
- `history-run`
- `history-report`
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

`collection_runs.parquet` now carries both `warning_count` and merged `warning_messages`, so collector warnings from multi-run normalization survive into reporting. For per-run traceability, report export still resolves raw warnings by `source_run_id` whenever the underlying raw manifests are present.

Analysis tables:

- `detected_languages.parquet`
- `cluster_memberships.parquet`
- `narrative_clusters.parquet`
- `stance_labels.parquet`
- `support_metrics.parquet`
- `analysis_runs.parquet`

Historical tables:

- `history_runs.parquet`
- `history_windows.parquet`
- `history_item_index.parquet`
- `history_narrative_clusters.parquet`
- `history_cluster_memberships.parquet`
- `history_stance_labels.parquet`
- `history_temporal_metrics.parquet`
- `history_coverage_gaps.parquet`

Review files:

- `review/narrative_overrides.csv`
- `review/stance_overrides.csv`

Reports:

- `reports/report_<run_id>.md`
- `reports/report_<run_id>.html`
- `reports/report_<run_id>.xlsx`
- `reports/report_<run_id>_tables/*.csv`
- `reports/openclaw/<run_id>/bundle.json`
- `reports/openclaw/<run_id>/brief.md`
- `reports/history/<history_run_id>/history_report.md`
- `reports/history/<history_run_id>/history_report.html`
- `reports/history/<history_run_id>/tables/*.csv`
- `reports/openclaw/<history_run_id>/bundle.json`
- `reports/openclaw/<history_run_id>/brief.md`

Important report exports include:

- `reports/report_<run_id>_tables/source_run_trace.csv`
- `reports/report_<run_id>_tables/source_warnings.csv`
- `reports/report_<run_id>_tables/coverage_gaps.csv`
- `reports/report_<run_id>_tables/propagation_coverage_gaps.csv`

`source_run_trace.csv` includes `source_run_id`, `collector`, `mode`, `status`, `fallback_used`, and `warning_count`, so merged snapshots still show which collector path actually ran for each raw source run.

`source_warnings.csv` now includes explicit `source_run_id`, `warning_index`, and `warning` columns, so merged snapshots still show which raw run produced each collector warning.

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
- historical monthly windows, resume behavior, temporal tables, history reports, and OpenClaw history bundles

## CI

GitHub Actions runs:

- `ruff check .`
- `mypy src`
- `pytest -q`

## Manual Acceptance Notes

Validated smoke runs in this repository session on April 11, 2026:

- `threads_web` against `https://www.threads.net/@zuck` completed successfully but returned `0` posts, `0` comments, and `0` propagations. The run status was `partial` and reported the expected best-effort public UI warning.
- `instagram_web` against `https://www.instagram.com/zuck/` completed successfully and returned `12` posts, `0` comments, and `0` propagations. The run status was `partial`; the report also detected `3` reels.

Validated smoke runs in this repository session on April 13, 2026:

- `facebook_web` against `https://www.facebook.com/VolodymyrBugrov/` completed successfully and returned `10` posts, `3` extracted comments, and `0` propagations. The run status was `partial`. The report preserved visible comment counters, including a reel with `visible=99` and `extracted=1`, and repeated the explicit authenticated-browser login-wall warning.
- `telegram_web` against `https://t.me/s/durov` completed successfully and returned `11` posts, `0` comments, and `0` propagations. The run status was `partial`. No linked discussion chat was visible, so the collector correctly stayed posts-only.
- `x_web` against `https://x.com/OpenAI` completed successfully and returned `2` posts, `0` comments, and `0` propagations. The run status was `partial`. The report preserved visible reply counters (`386` and `762`) and emitted explicit warnings that the public detail pages exposed no reply articles.

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
- Facebook public-web comment counters are now parsed from English, Ukrainian, and Russian surface text, and localized reply/control lines are filtered more aggressively out of extracted comment snapshots, but the DOM can still hide the actual comment list.
- Facebook authenticated browser mode still only sees what the logged-in account can see.
- Facebook propagation coverage is limited to shares and visible reshared surfaces the collectors can actually discover.
- `facebook_web` now filters localized UI/control lines such as `Відповісти`, `1 відповідь`, `Ответить`, and `Ответы` both in Python comment cleanup and in the earlier DOM author-selection heuristic. It also preserves more `published_hint` values directly from visible comment blocks. This improves comment snapshot quality when Facebook actually exposes comment blocks, but it does not remove the April 13, 2026 heavy-reel login-wall limitation.
- The Facebook timestamp parser also recognizes localized “yesterday” hints such as `Вчора` and `Вчера в 14:03`, so these lines are treated as timestamps during cleanup instead of leaking into comment body text.
- `facebook_web` comment extraction now keeps both `raw_text` and a cleaner `text` candidate per comment block. Normalization uses `raw_text` for author/timestamp fallback and `text` for the final message body, which reduces DOM noise without throwing away recovery signals.
- Telegram MTProto and web collection support one source channel per run, plus its linked discussion when visible.
- Telegram web collection only works for public `t.me/s/...` feeds.
- Telegram propagation coverage is limited to visible forwards or quoted surfaces available to the current collector.
- Telegram MTProto now orders nested replies more defensively and scales fallback scan size from the visible thread size, but the scan is still bounded and can miss very large discussion threads.
- Telegram MTProto history mode uses `history.max_items_per_window` instead of the normal page size for monthly source-message collection. Discussion comments are collected best-effort up to `history.max_comments_per_post`; if the visible reply count is larger than extracted comments, the run records a coverage warning.
- Telegram Bot API only sees updates currently available to the bot. It does not backfill history.
- X API reply coverage depends on the current search access window. With `search_scope: recent`, older replies can be missing.
- X web collection can scrape public profile posts, but public reply visibility is often shallower than the reply counter suggests unless an authenticated browser session is used. In the April 13, 2026 smoke run against `https://x.com/OpenAI`, visible counters (`386`, `762`) were preserved while reply article extraction stayed empty.
- Threads API works best for owned-account scenarios. Threads web coverage is best-effort and, in the April 11, 2026 public smoke run against `@zuck`, it returned zero visible posts.
- Instagram Graph API works for owned professional-account scenarios. Instagram web coverage is best-effort and public comments are often shallow. In the April 11, 2026 public smoke run against `@zuck`, it returned 12 posts and zero extracted comments.
- Instagram propagation coverage is intentionally conservative and limited to surfaces that are directly observable.
- Heuristic fallback providers keep the pipeline usable offline, but proper embeddings and LLM providers produce better analytical quality.

## License

This project is licensed under the MIT License. See `LICENSE`.
