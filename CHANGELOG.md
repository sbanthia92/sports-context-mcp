# Changelog

All notable changes to sports-context-mcp will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.2.0] — 2026-05-11

### Removed

- **API-Sports dependency from `jobs/ingest_match_data.py`** — the supplementary
  PL standings fetch (Thread 2) has been deleted along with the `_sports_get`
  helper, `_SPORTS_BASE` / `_PL_LEAGUE_ID` constants, and the
  `_current_season_start_year` helper. The job now runs a single FPL fetch
  thread plus the delta writer. The Gaffer app no longer consumes API-Sports
  data, so the dependency is no longer needed.

- **`api_sports_key` from `config.py`** and the matching `API_SPORTS_KEY` env
  var from `.github/workflows/ingest_match_data.yml`, `tests/conftest.py`, the
  `server.py --check` output, and the docs.

## [0.1.0]

### Added

- **Package scaffolding** — `sports-context-mcp` structured as a standalone,
  installable Python package (`pyproject.toml`) ready to be extracted into its
  own repository. Dependencies are declared explicitly and do not rely on the
  Gaffer's `requirements.txt`.

- **`config.py`** — Self-contained configuration module that reads from
  environment variables (and a `.env` file if `python-dotenv` is installed).
  Uses the same variable names as the Gaffer server so a single `.env` covers
  both packages during local development.

- **`tools/query_press_conferences.py`** — MCP tool that performs semantic search
  over the Pinecone `press` namespace. Embeds queries with `multilingual-e5-large`
  via Pinecone built-in inference and applies the same recency-weighted re-ranking
  formula used in the Gaffer's `server/rag.py`.

- **`tools/query_historical_stats.py`** — MCP tool that executes read-only SQL
  SELECT statements against the Gaffer PostgreSQL database. Includes a mutation
  keyword blocklist, a 10-second statement timeout, and a 100-row result cap.
  Inline schema description helps LLMs construct valid queries without a separate
  schema-inspection tool call.

- **`server.py`** — MCP server entry point (stdio transport). Registers both tools
  via the `mcp` Python SDK and dispatches incoming tool calls. Can be registered
  in `claude_desktop_config.json` or run directly with `python server.py`.

- **`jobs/ingest_press_content.py`** — Threaded press ingestion job. Fetches
  Premier League content from BBC Sport (RSS) and The Guardian (open content API
  at `content.guardianapis.com`) concurrently via `ThreadPoolExecutor`. Follows
  the exact embedding and upsert pattern from `pipeline/ingest_press.py`:
  `multilingual-e5-large`, batch size 96, `input_type="passage"`, namespace
  `press`. Stale articles (>14 days) are deleted after each run. Adding a new
  source requires only subclassing `_BaseFetcher` and appending to `FETCHERS`.

- **`jobs/ingest_match_data.py`** — Threaded match data ingestion job. Thread 1
  fetches FPL bootstrap + fixtures; Thread 2 fetches API-Sports PL standings.
  `concurrent.futures.wait()` blocks Thread 3 (the delta writer) until both fetch
  threads complete. The delta writer finds the latest `kickoff_time` in PostgreSQL
  and writes only fixtures newer than that timestamp, then fetches and upserts
  `gw_player_stats` for any newly-finished fixtures. Partial fetch failures are
  logged with full tracebacks and do not abort the job.

- **`.github/workflows/ingest_press_content.yml`** — GitHub Actions workflow that
  runs `ingest_press_content` nightly at midnight UTC, on push to the `ingestion`
  branch, and on manual dispatch.

- **`.github/workflows/ingest_match_data.yml`** — GitHub Actions workflow that
  runs `ingest_match_data` twice daily (06:00 + 22:00 UTC) by default, with
  detailed inline comments explaining how to adjust the schedule for FPL
  gameweek cadence, World Cup match cadence, and off-season operation.
