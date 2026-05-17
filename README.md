# sports-context-mcp

An [MCP](https://modelcontextprotocol.io) server that gives Claude (or any MCP client) two tools for answering Premier League football questions:

| Tool | What it does |
|---|---|
| `query_historical_stats` | Runs a read-only SQL SELECT against a PostgreSQL database with 3+ seasons of PL stats |
| `query_press_conferences` | Semantic search over BBC Sport and The Guardian press-conference summaries and injury updates stored in Pinecone |

Both ingestion jobs that keep the data fresh are included:

| Job | What it does |
|---|---|
| `ingest_press_content` | Fetches articles from BBC Sport RSS and The Guardian API, embeds them, and upserts into Pinecone |
| `ingest_match_data` | Fetches fixture and player-stat data from the FPL API, and delta-writes to PostgreSQL |

---

## Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Registering with Claude Desktop](#registering-with-claude-desktop)
- [Running the server standalone](#running-the-server-standalone)
- [Verifying connectivity (--check)](#verifying-connectivity---check)
- [Dry-run mode](#dry-run-mode)
- [MCP tools reference](#mcp-tools-reference)
- [Running ingestion jobs](#running-ingestion-jobs)
- [Database schema](#database-schema)
- [Running tests](#running-tests)
- [Extending with new press sources](#extending-with-new-press-sources)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| PostgreSQL | Any recent version with a `gaffer_readonly` role |
| Pinecone | Index named `the-gaffer` using the `multilingual-e5-large` model (1024 dims) |

The PostgreSQL database and Pinecone index are part of [The Gaffer](https://github.com/sbanthia92/Gaffer). If you're running this server standalone (without The Gaffer), you'll need to provision those resources yourself — see [Configuration](#configuration) for the expected schema.

---

## Installation

### From PyPI (recommended)

```bash
pip install sports-context-mcp
```

### With uv

```bash
git clone https://github.com/sbanthia92/sports-context-mcp
cd sports-context-mcp
uv sync
```

### With pip (from source)

```bash
git clone https://github.com/sbanthia92/sports-context-mcp
cd sports-context-mcp
pip install -e ".[dev]"
```

### As a dependency of another project

```
sports-context-mcp @ git+https://github.com/sbanthia92/sports-context-mcp.git
```

---

## Configuration

The server reads all secrets from environment variables. Create a `.env` file in the repo root (it is gitignored):

```dotenv
# PostgreSQL — read-only connection for the query_historical_stats tool
DATABASE_URL=postgresql://gaffer_readonly:password@localhost:5432/gaffer

# PostgreSQL — read/write connection for the ingest_match_data job
# Falls back to DATABASE_URL if not set
DATABASE_ETL_URL=postgresql://gaffer_etl:password@localhost:5432/gaffer

# Pinecone — required for both the press tool and the ingest_press_content job
PINECONE_API_KEY=pcsk_...
PINECONE_INDEX_NAME=the-gaffer   # optional, defaults to 'the-gaffer'

# The Guardian open platform API key
# Register free at https://open-platform.theguardian.com/access/
# Defaults to 'test' (public key — lower rate limit, no full article body)
GUARDIAN_API_KEY=your-key-here
```

### Which variables does each component need?

| Component | Variables required |
|---|---|
| `query_historical_stats` tool | `DATABASE_URL` |
| `query_press_conferences` tool | `PINECONE_API_KEY` |
| `ingest_press_content` job | `PINECONE_API_KEY`, `GUARDIAN_API_KEY` |
| `ingest_match_data` job | `DATABASE_ETL_URL` (or `DATABASE_URL`) |

---

## Registering with Claude Desktop

Add the server to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows).

### If installed from PyPI (recommended)

```json
{
  "mcpServers": {
    "sports-context": {
      "command": "sports-context-mcp",
      "env": {
        "DATABASE_URL": "postgresql://gaffer_readonly:password@localhost:5432/gaffer",
        "PINECONE_API_KEY": "pcsk_..."
      }
    }
  }
}
```

### If running from source

```json
{
  "mcpServers": {
    "sports-context": {
      "command": "python",
      "args": ["/absolute/path/to/sports-context-mcp/server.py"],
      "env": {
        "DATABASE_URL": "postgresql://gaffer_readonly:password@localhost:5432/gaffer",
        "PINECONE_API_KEY": "pcsk_..."
      }
    }
  }
}
```

> **Tip:** If you use `uv`, replace `"python"` with `"uv"` and prepend `"run"` to `args`:
> ```json
> "command": "uv",
> "args": ["run", "/absolute/path/to/sports-context-mcp/server.py"]
> ```

Restart Claude Desktop. You should see `sports-context` appear in the tools panel.

---

## Running the server standalone

```bash
# If installed from PyPI
sports-context-mcp

# If running from source
python server.py
```

The server communicates over stdio — it is designed to be launched by an MCP client, not run as a persistent HTTP service. Running it directly is mainly useful for smoke-testing startup and environment variable loading.

---

## Verifying connectivity (--check)

Before registering the server with a client, verify that your environment variables are correct and all backends are reachable:

```bash
# If installed from PyPI
sports-context-mcp --check

# If running from source
python server.py --check
```

Output example:

```
=== sports-context-mcp configuration check ===

✅ Pinecone          connected (index: 'the-gaffer')
✅ PostgreSQL (RO)   connected (localhost:5432/gaffer)
✅ PostgreSQL (ETL)  connected (localhost:5432/gaffer)
✅ Guardian API      registered key configured

✅ All required components OK
```

The command exits with code `0` if all required components pass, or `1` if any required component fails. Optional components (Guardian API) emit warnings but do not cause a non-zero exit.

---

## Dry-run mode

Set `DRY_RUN=true` to fetch data and verify routing without writing anything to Pinecone or PostgreSQL:

```bash
DRY_RUN=true sports-context-mcp
```

```bash
DRY_RUN=true python -c "from jobs.ingest_press_content import run; run()"
```

In dry-run mode:

- **Tools** return a human-readable description of the call that *would* have been made — the SQL with host, or the Pinecone index/namespace/params — without opening any connection.
- **Ingestion jobs** still call all external APIs (verifying connectivity) but skip every Pinecone and PostgreSQL write. Log output shows how many documents would have been upserted.
- The server logs a `DRY RUN MODE` warning at startup so it is obvious from the logs.

Accepted values for `DRY_RUN`: `true`, `1`, `yes` (case-insensitive). Any other value (or absent) disables dry-run.

---

## MCP tools reference

### `query_historical_stats`

Executes a read-only SQL `SELECT` against the historical stats database.

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `sql` | string | A `SELECT` statement. Mutations are rejected before reaching the database. `LIMIT` is injected automatically if omitted (capped at 100 rows). |

**Example prompts**

- *"Who are the top 10 midfielders by total points this season?"*
- *"How many goals has Salah scored across the last three seasons?"*
- *"Which teams have the best defensive record at home in 2024/25?"*

**Safety**

The tool enforces two layers of protection: a keyword blocklist rejects `INSERT`, `UPDATE`, `DELETE`, `DROP`, and similar statements before any database call is made, and the database connection uses a read-only role with no write grants.

---

### `query_press_conferences`

Semantic search over Premier League press coverage ingested from BBC Sport and The Guardian.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | Natural-language question or topic |
| `top_k` | integer | 5 | Number of documents to return |
| `recency_weight` | float | 0.3 | Recency boost: `0.0` = pure semantic similarity, `1.0` = heavy recency bias |

**Ranking formula**

Results are re-ranked after retrieval:

```
final_score = semantic_score × (1 + recency_weight × recency_score)
```

`recency_score` is 1.0 for an article published today and decays toward 0.1 over 14 days.

**Example prompts**

- *"Any injury concerns for Saka this week?"*
- *"What did Slot say about Salah's contract situation?"*
- *"Who is doubtful for Arsenal's next match?"*

---

## Running ingestion jobs

Ingestion jobs are plain Python scripts — run them directly or schedule them via cron.

### Press content (Pinecone)

Fetches articles from BBC Sport RSS and The Guardian content API, embeds them with `multilingual-e5-large`, and upserts into the `press` Pinecone namespace. Stale articles (>14 days) are deleted on each run.

```bash
python -c "from jobs.ingest_press_content import run; run()"
```

Run twice daily for fresh injury and team news. Both fetchers run concurrently; if one source fails the other's articles are still upserted.

**Guardian API key note:** With the default `test` key, `bodyText` is not populated — the fetcher falls back to `trailText` (the article summary). For full article text, register for a free key at [open-platform.theguardian.com](https://open-platform.theguardian.com/access/).

### Match data (PostgreSQL)

Fetches fixture and player-stat data from the FPL bootstrap API, then delta-writes to PostgreSQL — only rows with a `kickoff_time` newer than the latest stored fixture are inserted.

```bash
python -c "from jobs.ingest_match_data import run; run()"
```

Runs the FPL fetch in a worker thread, waits for it to complete, then writes in a single transaction. If the fetch fails, the delta write is skipped.

---

## Database schema

The `query_historical_stats` tool has access to these tables:

```
seasons          id, label (e.g. '2025/26'), start_year, is_current

teams            season_id, fpl_id, name, short_name, strength,
                 strength_attack_home/away, strength_defence_home/away

gameweeks        season_id, gw_number (1–38), deadline_time, is_current,
                 is_next, is_finished, average_entry_score, highest_score

players          season_id, fpl_id, team_fpl_id, first_name, second_name,
                 web_name, position (GKP/DEF/MID/FWD), now_cost, form,
                 total_points, minutes, goals_scored, assists, clean_sheets,
                 expected_goals, expected_assists, ict_index, status, news

fixtures         season_id, fpl_id, gw_number, kickoff_time,
                 home_team_fpl_id, away_team_fpl_id, home_score, away_score,
                 finished, home_team_difficulty, away_team_difficulty

gw_player_stats  season_id, player_fpl_id, gw_number, fixture_fpl_id,
                 opponent_team_fpl_id, was_home, minutes, goals_scored,
                 assists, clean_sheets, bonus, total_points,
                 expected_goals, expected_assists, ict_index, starts

player_xpts      materialized view: player_fpl_id, web_name, team_name,
                 position, now_cost, expected_points (next GW projection)
```

**Join hint:** `teams.fpl_id = players.team_fpl_id` (within the same `season_id`).

---

## Running tests

```bash
# Install dev dependencies if you haven't already
pip install -e ".[dev]"

# Run the full suite (81 tests, all mocked — no real DB or API calls)
pytest tests/ -v

# Lint and format
ruff check . && ruff format .
```

The test suite covers:

| File | What's tested |
|---|---|
| `tests/test_config.py` | Env var reading, defaults, dotenv loading, dry-run flag |
| `tests/test_tools_stats.py` | Mutation guard, row formatter, async DB path, dry-run |
| `tests/test_tools_press.py` | Pinecone query, recency re-ranking, degradation, dry-run |
| `tests/test_ingest_press_content.py` | BBC/Guardian fetchers, deduplication, orchestration, dry-run |
| `tests/test_ingest_match_data.py` | Delta filtering, thread coordination, rollback, dry-run |

---

## Extending with new press sources

To add a new press source, subclass `_BaseFetcher` in `jobs/ingest_press_content.py` and add an instance to the `FETCHERS` list. The orchestrator picks it up automatically — no other changes needed.

```python
class MySportsFetcher(_BaseFetcher):
    source_name = "My Sports Site"

    def fetch(self) -> list[tuple[str, str, dict]]:
        # return a list of (doc_id, text, metadata) tuples
        ...

FETCHERS: list[_BaseFetcher] = [BBCSportFetcher(), GuardianAPIFetcher(), MySportsFetcher()]
```

Each tuple is `(doc_id, text, metadata)` where:

- `doc_id` — a stable 32-char hex ID (use `_doc_id(source + url)`)
- `text` — the full text to embed, prefixed with the source name
- `metadata` — must include `type`, `source`, `recency_score`, and `pub_timestamp`
