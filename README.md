# Facebook Posts Analysis

Local-first Python pipeline for collecting Facebook posts and comments, then analyzing narratives, stance, and support.

The project includes:

- narrative clusters
- stance toward selected sides or actors
- support and oppose ratios
- high-conflict threads
- language mix for `ru`, `uk`, and `en`

It supports both Meta API collection and a Playwright-based web collector, with reviewable outputs in DuckDB, parquet, CSV, Markdown, and HTML.

## What The Project Does

- collects posts, comments, and visible replies from Facebook
- stores raw snapshots per run under `data/raw/<run_id>/`
- normalizes everything into parquet and DuckDB
- detects language and clusters posts/comments into narrative groups
- labels stance toward configured sides
- computes support metrics globally and by cluster
- exports review files for manual corrections
- renders Markdown and HTML reports

Recent additions in the current codebase:

- authenticated browser profile support for the web collector
- multi-pass collection runs
- merged normalized snapshots from several recent runs
- reply depth extraction for visible nested comments
- coverage-gap reporting for posts where visible counters exceed extracted text comments

## CLI Commands

The package exposes the `facebook-posts-analysis` CLI with:

- `collect`
- `normalize`
- `analyze`
- `review-export`
- `report`
- `run-all`
- `run-many`

`run-many` is useful for unstable public-web collection, because Facebook can reveal slightly different content across repeated passes.

## Project Layout

```text
config/project.yaml
src/facebook_posts_analysis/
  analysis/
  collectors/
  reporting/
tests/
```

## Requirements

- Python 3.12+
- recommended: `uv`
- Playwright plus Chromium if using the web collector
- a valid Meta token if using the API collector

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

The tracked config at `config/project.yaml` is a safe template.
For real work, copy it to a private local file such as `config/project.local.yaml` and pass that file with `--config`.

Important settings:

- `page.url` or `page.page_id`
- `date_range.start` and `date_range.end`
- `collector.mode`: `api`, `web`, or `hybrid`
- `collector.multi_pass_runs`
- `collector.wait_between_passes_seconds`
- `collector.public_web.authenticated_browser.*`
- `normalization.merge_recent_runs`
- `sides`: stance targets
- `providers.embeddings` and `providers.llm`

Environment variables supported by default:

- `META_ACCESS_TOKEN`
- `FACEBOOK_BROWSER_USER_DATA_DIR`
- `FACEBOOK_BROWSER_PROFILE_DIRECTORY`
- `EMBEDDING_BASE_URL`
- `EMBEDDING_API_KEY`
- `LLM_BASE_URL`
- `LLM_API_KEY`

Do not commit real target URLs, actor lists, tokens, or browser-profile settings. Keep those in an untracked local config file.

## Usage

Full pipeline:

```bash
facebook-posts-analysis run-all --config config/project.local.yaml
```

Multi-pass full pipeline:

```bash
facebook-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Step by step:

```bash
facebook-posts-analysis collect --config config/project.local.yaml
facebook-posts-analysis normalize --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis analyze --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis review-export --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis report --config config/project.local.yaml --run-id <run_id>
```

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

## Authenticated Browser Mode

For web collection, the safest supported approach is reusing an already logged-in local browser profile rather than storing credentials in the project.

Current config supports:

- Chrome
- Edge
- custom user-data directory

The collector can launch a copied snapshot of the browser profile, which reduces the chance of interfering with a live browser session.

## Testing

Run:

```bash
pytest
```

The test suite currently covers:

- Meta API pagination and nested comments
- public-web parsing and timestamp handling
- reply/control-line cleanup
- comment hierarchy construction from visible nesting
- normalization and merged snapshots
- analysis helpers and support metrics
- review override application
- collection fallback and multi-pass behavior

## Important Limits

- The public-web collector is best-effort. Facebook can expose different DOM states across runs.
- Authenticated browser mode still only sees what the logged-in account can see.
- Some posts may still show a visible comment counter while not yielding full text comments in the DOM.
- API-first collection depends on the current Meta permission model and the target object type.
- Heuristic fallback providers keep the pipeline usable offline, but proper embeddings and LLM providers will produce better analytical quality.

## License

MIT. See `LICENSE`.
