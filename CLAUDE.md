# sports-context-mcp

Standalone MCP server that exposes Premier League sports stats and press-conference
RAG as MCP tools, plus threaded ingestion jobs that keep the underlying PostgreSQL
and Pinecone stores up to date.

Designed to be registered in Claude Desktop or any MCP-compatible host. When
extracted from the Gaffer monorepo it becomes a fully independent service.

## Stack
- **Language**: Python 3.11+
- **MCP framework**: `mcp` Python SDK (stdio transport)
- **Vector store**: Pinecone (`multilingual-e5-large` built-in inference, namespace `press`)
- **Database**: PostgreSQL (read-only for MCP tools, read/write for ingestion jobs)
- **HTTP**: `requests` (sync, used by jobs) + `asyncpg` (async, used by MCP tools)
- **Concurrency**: `concurrent.futures.ThreadPoolExecutor` in jobs — no asyncio mixing

## Package structure
```
sports-context-mcp/
  config.py                        # Env-var config (same var names as the Gaffer)
  server.py                        # MCP server entry point (stdio transport)
  tools/
    query_historical_stats.py      # MCP tool: read-only SQL → PostgreSQL
    query_press_conferences.py     # MCP tool: semantic search → Pinecone 'press' namespace
  jobs/
    ingest_press_content.py        # Pinecone updater: BBC RSS + Guardian API, threaded
    ingest_match_data.py           # PostgreSQL updater: FPL, delta write
  tests/
    conftest.py
    test_config.py
    test_tools_press.py
    test_tools_stats.py
    test_ingest_press_content.py
    test_ingest_match_data.py
  pyproject.toml
  CHANGELOG.md
  .github/workflows/
    ingest_press_content.yml       # Nightly press ingestion
    ingest_match_data.yml          # Configurable match data ingestion
```

## Dev commands
```bash
# Install (editable) with dev extras
pip install -e ".[dev]"

# Lint + format (must pass before every push)
ruff check . && ruff format .

# Tests
pytest tests/ -v

# Run the MCP server locally (stdio — wire into claude_desktop_config.json)
python server.py

# Run ingestion jobs manually
python -m jobs.ingest_press_content
python -m jobs.ingest_match_data
```

## Configuration

All config is read from environment variables. A `.env` file at the repo root is
loaded automatically by `config.py` when `python-dotenv` is installed.

| Variable            | Required | Default      | Purpose |
|---------------------|----------|--------------|---------|
| `PINECONE_API_KEY`  | Yes      | —            | Pinecone API key |
| `PINECONE_INDEX_NAME` | No     | `the-gaffer` | Pinecone index name |
| `DATABASE_URL`      | Yes*     | —            | Read-only PostgreSQL DSN (`gaffer_readonly` user) |
| `DATABASE_ETL_URL`  | Yes*     | —            | Read/write PostgreSQL DSN (`gaffer_etl` user). Falls back to `DATABASE_URL`. |
| `GUARDIAN_API_KEY`  | No       | `test`       | Guardian open platform key. Register at open-platform.theguardian.com for full body text. |

*Required for the respective tool/job to function; the package will start without them
and log an error on first use.

## MCP registration (Claude Desktop)

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "sports-context": {
      "command": "python",
      "args": ["/absolute/path/to/sports-context-mcp/server.py"]
    }
  }
}
```

## MCP tools exposed

### `query_historical_stats`
Executes a read-only SQL SELECT against the Gaffer PostgreSQL database.
- Blocks mutation keywords (INSERT/UPDATE/DELETE/DROP etc.)
- 10-second statement timeout
- 100-row result cap
- Inline schema description helps the model write valid queries without a schema-lookup call

### `query_press_conferences`
Semantic search over the Pinecone `press` namespace (BBC Sport + Guardian articles,
FPL player injury updates). Applies recency-weighted re-ranking identical to the
Gaffer's `server/rag.py`.

## Ingestion jobs

### `ingest_press_content`
- Thread 1: BBC Sport PL RSS → press articles
- Thread 2: Guardian API (`content.guardianapis.com`) → press articles
- Sequential after threads: FPL bootstrap → player injury/availability docs
- Embeds with `multilingual-e5-large`, batch 96, upserts to `press` namespace
- Deletes articles older than 14 days on every run

**Adding a new source**: subclass `_BaseFetcher`, implement `fetch()`, append to `FETCHERS`.
No other changes needed.

### `ingest_match_data`
- Thread 1: FPL API bootstrap-static + fixtures
- `concurrent.futures.wait([f1])` ensures Thread 3 never starts until the fetch thread completes
- Thread 3 (delta write): finds `MAX(kickoff_time)` in PostgreSQL, upserts only newer fixtures
  and their `gw_player_stats` rows. Commits atomically; rolls back on any write error.

**Fetch failure**: if the fetch thread fails, its result is `None` and the delta
writer logs the full traceback and aborts the write step.

## Pinecone document schema

Documents upserted to the `press` namespace carry this metadata:
```python
{
    "text": str,           # Full document text (embedded)
    "type": "press_article" | "player_news",
    "source": str,         # "BBC Sport" | "The Guardian" | "FPL"
    "date": str,           # RFC 2822 or ISO 8601
    "pub_timestamp": float, # Unix timestamp — used for stale-doc deletion
    "recency_score": float, # 1.0 (today) → 0.1 (14 days) — used for re-ranking
    "url": str,            # press_article only
}
```

## Git workflow
- **Branch from main**: `git checkout -b fix/description origin/main`
- **PR per change** — keep commits small and descriptive
- **Before pushing**: `ruff check . && ruff format . && pytest tests/ -v`

## Every PR checklist
1. Bump the version in `pyproject.toml` and `CHANGELOG.md`
2. Add a `CHANGELOG.md` entry under the new version
3. Update `CLAUDE.md` if conventions, architecture, or env vars change

## Commit conventions
- `feat:` — new tool, job, or fetcher
- `fix:` — bug fix
- `chore:` — deps, CI, formatting
- `refactor:` — restructure without behaviour change
- `docs:` — documentation only

## Known gotchas
- **Guardian `test` API key**: does not return `bodyText` — only `trailText` (summary).
  Register for a free production key to get full article text.
- **Pinecone inference rate limits**: the 8-second sleep between embed batches in `_upsert`
  exists to avoid HTTP 429s on the free inference tier. Remove or reduce it on paid tiers.
- **Thread 3 ordering**: `concurrent.futures.wait([f1])` is the only enforcement that
  Thread 3 starts after Thread 1 (the FPL fetch). Do not refactor this to `as_completed`
  in a way that allows the writer to start before the fetcher finishes.
- **psycopg2 vs asyncpg**: jobs use `psycopg2` (sync), MCP tools use `asyncpg` (async).
  Do not swap them — jobs must stay sync to avoid mixing asyncio and threading.
