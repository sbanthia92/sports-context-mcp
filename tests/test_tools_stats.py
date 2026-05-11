"""
Tests for tools/query_historical_stats.py.

All PostgreSQL calls are mocked — no real database is required.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tools.query_historical_stats import _check_for_mutations, _format_rows, query_historical_stats


# ---------------------------------------------------------------------------
# Unit tests — mutation guard (synchronous, no mocking needed)
# ---------------------------------------------------------------------------


def test_select_passes_mutation_guard():
    """Plain SELECT does not raise."""
    _check_for_mutations("SELECT * FROM players")


def test_select_with_where_passes():
    """SELECT with WHERE clause does not raise."""
    _check_for_mutations("SELECT id, name FROM teams WHERE season_id = 1")


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO players VALUES (1, 2)",
        "UPDATE players SET form = 5",
        "DELETE FROM fixtures WHERE id = 1",
        "DROP TABLE seasons",
        "CREATE TABLE foo (id INT)",
        "ALTER TABLE players ADD COLUMN foo TEXT",
        "TRUNCATE gw_player_stats",
    ],
)
def test_mutation_keywords_are_blocked(sql):
    """Every mutation keyword raises ValueError."""
    with pytest.raises(ValueError, match="disallowed keyword"):
        _check_for_mutations(sql)


def test_mutation_keyword_case_insensitive():
    """Keyword check is case-insensitive."""
    with pytest.raises(ValueError):
        _check_for_mutations("insert into players values (1)")


# ---------------------------------------------------------------------------
# Unit tests — row formatter
# ---------------------------------------------------------------------------


def test_format_rows_empty():
    """Empty result set returns a human-readable message."""
    assert _format_rows([]) == "Query returned no results."


def test_format_rows_with_data():
    """Rows are rendered as a header + separator + data lines."""
    row = MagicMock()
    row.keys.return_value = ["id", "name"]
    row.__getitem__ = lambda self, k: {"id": 1, "name": "Arsenal"}[k]

    result = _format_rows([row])

    assert "id | name" in result
    assert "Arsenal" in result


# ---------------------------------------------------------------------------
# Integration-style tests — full async path (mocked asyncpg)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_asyncpg():
    """Patch asyncpg.connect so no real DB connection is made."""
    import asyncpg as real_asyncpg

    with patch("tools.query_historical_stats.asyncpg") as mock_pg:
        conn = AsyncMock()
        mock_pg.connect = AsyncMock(return_value=conn)
        # Preserve the real exception class so except-clauses in the code work correctly.
        mock_pg.PostgresError = real_asyncpg.PostgresError
        # Default: return empty result set
        conn.fetch = AsyncMock(return_value=[])
        conn.execute = AsyncMock()
        conn.close = AsyncMock()
        yield mock_pg, conn


@pytest.mark.asyncio
async def test_returns_formatted_rows(mock_asyncpg):
    """Successful query returns formatted row data."""
    _, conn = mock_asyncpg

    row = MagicMock()
    row.keys.return_value = ["web_name", "total_points"]
    row.__getitem__ = lambda self, k: {"web_name": "Salah", "total_points": 220}[k]
    conn.fetch.return_value = [row]

    result = await query_historical_stats("SELECT web_name, total_points FROM players LIMIT 1")

    assert "Salah" in result
    assert "220" in result


@pytest.mark.asyncio
async def test_limit_injected_when_absent(mock_asyncpg):
    """LIMIT is appended to queries that don't already have one."""
    _, conn = mock_asyncpg
    conn.fetch.return_value = []

    await query_historical_stats("SELECT * FROM players")

    called_sql: str = conn.fetch.call_args[0][0]
    assert "LIMIT" in called_sql.upper()


@pytest.mark.asyncio
async def test_existing_limit_not_doubled(mock_asyncpg):
    """Queries that already contain LIMIT are not modified."""
    _, conn = mock_asyncpg
    conn.fetch.return_value = []

    await query_historical_stats("SELECT * FROM players LIMIT 5")

    called_sql: str = conn.fetch.call_args[0][0]
    assert called_sql.upper().count("LIMIT") == 1


@pytest.mark.asyncio
async def test_mutation_blocked_before_db_call(mock_asyncpg):
    """Mutation keywords raise ValueError without touching the database."""
    _, conn = mock_asyncpg

    with pytest.raises(ValueError):
        await query_historical_stats("DELETE FROM fixtures")

    conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_returns_error_string_on_missing_db_url(monkeypatch, mock_asyncpg):
    """Returns an error string when DATABASE_URL is not configured."""
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_ETL_URL", "")

    result = await query_historical_stats("SELECT 1")

    assert "Error" in result
    _, conn = mock_asyncpg
    conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_returns_error_string_on_db_exception(mock_asyncpg):
    """PostgreSQL errors are caught and returned as a descriptive string."""
    _, conn = mock_asyncpg
    conn.fetch.side_effect = conn.fetch.side_effect  # reset
    import asyncpg as real_asyncpg
    conn.fetch.side_effect = real_asyncpg.PostgresError("relation does not exist")

    result = await query_historical_stats("SELECT * FROM nonexistent_table")

    assert "Database error" in result
