"""
MCP tool: query_historical_stats

Executes a read-only SQL SELECT against the Gaffer PostgreSQL database, which
holds 3+ seasons of Premier League historical stats (players, fixtures, teams,
gameweeks, gw_player_stats).

Safety guarantees mirror those in the Gaffer's server/tools/db.py:
  - Keyword blocklist rejects any mutation statement before it reaches the DB
  - Connection uses the read-only DATABASE_URL (gaffer_readonly user)
  - Statement timeout: 10 seconds
  - Result cap: 100 rows
"""

import logging

import asyncpg

from config import cfg

log = logging.getLogger(__name__)

# Mutations to block before they reach the DB. The readonly DB user would reject
# them anyway, but failing early gives a cleaner error message to the caller.
_BLOCKED_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "REPLACE",
    "MERGE",
    "GRANT",
    "REVOKE",
}

# Maximum rows returned per query to keep response payloads manageable.
_MAX_ROWS = 100

# Hard limit on query execution time (milliseconds).
_STATEMENT_TIMEOUT_MS = 10_000

# Database schema description injected into the tool description so the model
# knows what tables and columns are available without needing a separate schema
# lookup call.
SCHEMA_DESCRIPTION = """
Available tables (read-only, current season + 3 historical seasons):

  seasons        — id, label (e.g. '2025/26'), start_year, is_current
  teams          — season_id, fpl_id, name, short_name, strength,
                   strength_attack_home/away, strength_defence_home/away
  gameweeks      — season_id, gw_number (1–38), deadline_time, is_current,
                   is_next, is_finished, average_entry_score, highest_score
  players        — season_id, fpl_id, team_fpl_id, first_name, second_name,
                   web_name, position (GKP/DEF/MID/FWD), now_cost, form,
                   total_points, minutes, goals_scored, assists, clean_sheets,
                   expected_goals, expected_assists, ict_index, status, news
  fixtures       — season_id, fpl_id, gw_number, kickoff_time,
                   home_team_fpl_id, away_team_fpl_id, home_score, away_score,
                   finished, home_team_difficulty, away_team_difficulty
  gw_player_stats — season_id, player_fpl_id, gw_number, fixture_fpl_id,
                    opponent_team_fpl_id, was_home, minutes, goals_scored,
                    assists, clean_sheets, bonus, total_points,
                    expected_goals, expected_assists, ict_index, starts
  player_xpts    — materialized view: player_fpl_id, web_name, team_name,
                   position, now_cost, expected_points (next GW projection)

Join hint: teams.fpl_id = players.team_fpl_id (within the same season_id).
"""


def _check_for_mutations(sql: str) -> None:
    """
    Reject SQL that contains mutation keywords.

    Normalises the query to uppercase and splits on whitespace to avoid matching
    keywords that appear inside string literals or column names in most realistic
    cases. This is a defence-in-depth check — the DB user's grants are the real
    enforcement layer.

    Args:
        sql: The raw SQL string submitted by the caller.

    Raises:
        ValueError: If a blocked keyword is found, with a descriptive message.
    """
    tokens = sql.upper().split()
    for token in tokens:
        # Strip trailing punctuation so "DELETE;" still matches "DELETE".
        clean = token.rstrip(";(),")
        if clean in _BLOCKED_KEYWORDS:
            raise ValueError(
                f"Query contains a disallowed keyword: {clean!r}. "
                "Only SELECT statements are permitted."
            )


def _format_rows(rows: list[asyncpg.Record]) -> str:
    """
    Render asyncpg result rows as a plain-text table.

    Args:
        rows: List of asyncpg Record objects from conn.fetch().

    Returns:
        A newline-separated string with a header row and one data row per record.
        Returns a human-readable message if the result set is empty.
    """
    if not rows:
        return "Query returned no results."

    columns = list(rows[0].keys())
    header = " | ".join(columns)
    separator = "-" * len(header)

    data_lines = [" | ".join(str(row[col]) for col in columns) for row in rows]
    return "\n".join([header, separator, *data_lines])


async def query_historical_stats(sql: str) -> str:
    """
    Execute a read-only SQL SELECT against the Gaffer historical stats database.

    Validates the query, executes it against PostgreSQL with a 10-second timeout,
    and returns the results as a formatted plain-text table. Results are capped at
    100 rows to keep response payloads manageable.

    Args:
        sql: A SELECT statement. Any mutation keywords (INSERT, UPDATE, DELETE,
             DROP, etc.) will be rejected before reaching the database.

    Returns:
        A formatted plain-text table of results, an empty-result message, or an
        error description if the query fails or times out.

    Raises:
        ValueError: If the SQL contains a blocked keyword.
    """
    if not cfg.database_url:
        log.error("DATABASE_URL is not set — cannot query historical stats.")
        return "Error: DATABASE_URL is not configured."

    _check_for_mutations(sql)

    # Inject LIMIT if none present to enforce the row cap without rejecting valid queries.
    normalized = sql.strip().rstrip(";")
    if "LIMIT" not in normalized.upper():
        normalized = f"{normalized} LIMIT {_MAX_ROWS}"

    if cfg.dry_run:
        log.info("[dry run] query_historical_stats: would execute SQL:\n%s", normalized)
        host = cfg.database_url.split("@")[-1] if cfg.database_url else "DATABASE_URL"
        return (
            f"[DRY RUN] No database connection was made. "
            f"Would have executed against {host}:\n\n{normalized}"
        )

    conn: asyncpg.Connection = await asyncpg.connect(
        cfg.database_url,
        statement_cache_size=0,  # avoid prepared-statement conflicts on read-only connections
    )
    try:
        # Set a per-statement timeout so runaway queries don't block the MCP server.
        await conn.execute(f"SET statement_timeout = {_STATEMENT_TIMEOUT_MS}")
        rows = await conn.fetch(normalized)
    except asyncpg.PostgresError as exc:
        log.error("PostgreSQL query failed: %s | sql=%r", exc, sql, exc_info=True)
        return f"Database error: {exc}"
    except Exception as exc:
        log.error("Unexpected error running query: %s", exc, exc_info=True)
        return f"Unexpected error: {exc}"
    finally:
        await conn.close()

    log.info("query_historical_stats: %d rows returned", len(rows))
    return _format_rows(list(rows))
