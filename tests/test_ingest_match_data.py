"""
Tests for jobs/ingest_match_data.py.

All HTTP and PostgreSQL calls are mocked. Tests cover thread coordination,
delta filtering, partial failure handling, and the full run() orchestration.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from jobs.ingest_match_data import (
    _current_season_start_year,
    _get_last_kickoff,
    _parse_dt,
    _upsert_new_fixtures,
    delta_write,
    fetch_fpl_data,
    run,
)

# ---------------------------------------------------------------------------
# Pure helper tests
# ---------------------------------------------------------------------------


def test_parse_dt_iso():
    """_parse_dt handles ISO 8601 with trailing Z."""
    dt = _parse_dt("2026-05-10T15:00:00Z")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 5


def test_parse_dt_none():
    """_parse_dt returns None for empty string."""
    assert _parse_dt("") is None
    assert _parse_dt(None) is None


def test_current_season_start_year_during_season():
    """During the PL season (e.g. May), returns the previous calendar year."""
    # May 2026 → season started August 2025 → start year = 2025
    with patch("jobs.ingest_match_data.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 5, 10, tzinfo=UTC)
        year = _current_season_start_year()
    assert year == 2025


def test_current_season_start_year_in_august():
    """In August, the new season has just started — return current year."""
    with patch("jobs.ingest_match_data.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 8, 15, tzinfo=UTC)
        year = _current_season_start_year()
    assert year == 2026


# ---------------------------------------------------------------------------
# _get_last_kickoff
# ---------------------------------------------------------------------------


def test_get_last_kickoff_returns_datetime():
    """Returns the datetime from the DB query."""
    expected = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (expected,)

    result = _get_last_kickoff(conn)
    assert result == expected


def test_get_last_kickoff_returns_none_when_empty():
    """Returns None when the fixtures table is empty."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (None,)

    assert _get_last_kickoff(conn) is None


# ---------------------------------------------------------------------------
# _upsert_new_fixtures — delta filtering
# ---------------------------------------------------------------------------


def _make_fixture(fpl_id: int, kickoff: str, finished: bool = True) -> dict:
    return {
        "id": fpl_id,
        "event": 35,
        "kickoff_time": kickoff,
        "team_h": 1,
        "team_a": 2,
        "team_h_score": 2,
        "team_a_score": 1,
        "finished": finished,
        "started": finished,
        "team_h_difficulty": 3,
        "team_a_difficulty": 4,
    }


def test_upsert_new_fixtures_filters_old():
    """Fixtures with kickoff_time <= last_kickoff are not upserted."""
    cur = MagicMock()
    last_kickoff = datetime(2026, 5, 5, 15, 0, 0, tzinfo=UTC)

    fixtures = [
        _make_fixture(1, "2026-05-03T15:00:00Z"),  # before cutoff → skip
        _make_fixture(2, "2026-05-10T14:00:00Z"),  # after cutoff → include
    ]

    new = _upsert_new_fixtures(cur, season_id=1, fixtures=fixtures, last_kickoff=last_kickoff)

    assert len(new) == 1
    assert new[0]["id"] == 2
    cur.execute.assert_called_once()  # only one INSERT


def test_upsert_new_fixtures_includes_all_when_no_last_kickoff():
    """When last_kickoff is None (first run), all fixtures are upserted."""
    cur = MagicMock()
    fixtures = [
        _make_fixture(1, "2026-04-01T15:00:00Z"),
        _make_fixture(2, "2026-05-01T15:00:00Z"),
    ]

    new = _upsert_new_fixtures(cur, season_id=1, fixtures=fixtures, last_kickoff=None)

    assert len(new) == 2
    assert cur.execute.call_count == 2


def test_upsert_new_fixtures_skips_no_kickoff():
    """Fixtures without a kickoff_time (postponed) are skipped."""
    cur = MagicMock()
    fixtures = [{"id": 99, "kickoff_time": None, "team_h": 1, "team_a": 2}]

    new = _upsert_new_fixtures(cur, season_id=1, fixtures=fixtures, last_kickoff=None)

    assert new == []
    cur.execute.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_fpl_data
# ---------------------------------------------------------------------------


def test_fetch_fpl_data_returns_bootstrap_and_fixtures():
    """fetch_fpl_data returns both bootstrap and fixtures dicts."""
    bootstrap = {"elements": [{"id": 1}], "teams": [], "events": []}
    fixtures = [{"id": 1, "kickoff_time": "2026-05-10T15:00:00Z"}]

    with patch("jobs.ingest_match_data.requests.get") as mock_get:
        resp1 = MagicMock()
        resp1.json.return_value = bootstrap
        resp1.raise_for_status = MagicMock()
        resp2 = MagicMock()
        resp2.json.return_value = fixtures
        resp2.raise_for_status = MagicMock()
        mock_get.side_effect = [resp1, resp2]

        result = fetch_fpl_data()

    assert result["bootstrap"] == bootstrap
    assert result["fixtures"] == fixtures


# ---------------------------------------------------------------------------
# delta_write — thread coordination and partial failure
# ---------------------------------------------------------------------------


def _make_fpl_result() -> dict:
    return {
        "bootstrap": {
            "elements": [],
            "teams": [],
            "events": [{"deadline_time": "2026-08-01T17:30:00Z"}],
        },
        "fixtures": [],
    }


def test_delta_write_aborts_if_fpl_result_is_none():
    """delta_write logs and returns without touching the DB when FPL fetch failed."""
    with patch("jobs.ingest_match_data._get_db_conn") as mock_conn:
        delta_write(fpl_result=None, sports_result=None)

    mock_conn.assert_not_called()


def test_delta_write_proceeds_without_sports_result():
    """delta_write works normally when API-Sports fetch failed (sports_result=None)."""
    fpl_result = _make_fpl_result()

    ro_conn = MagicMock()
    etl_conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)  # season_id
    cur.fetchall.return_value = []  # no players for stat fetch
    etl_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    etl_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    ro_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=MagicMock(fetchone=MagicMock(return_value=(None,)))
    )
    ro_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("jobs.ingest_match_data._get_db_conn", side_effect=[ro_conn, etl_conn]):
        delta_write(fpl_result=fpl_result, sports_result=None)

    # Should commit without error
    etl_conn.commit.assert_called_once()


def test_delta_write_rolls_back_on_error():
    """delta_write rolls back the transaction if any write step raises."""
    fpl_result = _make_fpl_result()

    ro_conn = MagicMock()
    etl_conn = MagicMock()
    cur = MagicMock()
    # Simulate a DB write failure on the first execute call
    cur.execute.side_effect = Exception("DB write failed")
    etl_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    etl_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    ro_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=MagicMock(fetchone=MagicMock(return_value=(None,)))
    )
    ro_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("jobs.ingest_match_data._get_db_conn", side_effect=[ro_conn, etl_conn]):
        delta_write(fpl_result=fpl_result, sports_result=None)

    etl_conn.rollback.assert_called_once()
    etl_conn.commit.assert_not_called()


# ---------------------------------------------------------------------------
# run() — thread coordination
# ---------------------------------------------------------------------------


def test_run_waits_for_both_fetch_threads_before_write():
    """Thread 3 (delta_write) is submitted only after wait([f1, f2]) returns."""
    call_order = []

    def fake_fetch_fpl():
        call_order.append("fpl")
        return _make_fpl_result()

    def fake_fetch_sports():
        call_order.append("sports")
        return {"standings": [], "season": 2025}

    def fake_delta_write(fpl_result, sports_result):
        call_order.append("write")

    with (
        patch("jobs.ingest_match_data.fetch_fpl_data", side_effect=fake_fetch_fpl),
        patch("jobs.ingest_match_data.fetch_api_sports_data", side_effect=fake_fetch_sports),
        patch("jobs.ingest_match_data.delta_write", side_effect=fake_delta_write),
    ):
        run()

    # Both fetch threads must complete before write is called
    assert "write" in call_order
    assert call_order.index("write") > call_order.index("fpl")
    assert call_order.index("write") > call_order.index("sports")


def test_run_passes_none_to_write_on_fetch_failure():
    """If a fetch thread raises, delta_write receives None for that result."""
    received: dict = {}

    def fake_delta_write(fpl_result, sports_result):
        received["fpl"] = fpl_result
        received["sports"] = sports_result

    _sports_rv = {"standings": [], "season": 2025}
    with (
        patch("jobs.ingest_match_data.fetch_fpl_data", side_effect=RuntimeError("FPL down")),
        patch("jobs.ingest_match_data.fetch_api_sports_data", return_value=_sports_rv),
        patch("jobs.ingest_match_data.delta_write", side_effect=fake_delta_write),
    ):
        run()

    assert received["fpl"] is None
    assert received["sports"] is not None


# ---------------------------------------------------------------------------
# Dry-run tests
# ---------------------------------------------------------------------------


def test_run_dry_run_skips_delta_write():
    """run(dry_run=True) fetches from both sources but never calls delta_write."""
    _sports_rv = {"standings": [], "season": 2025}
    with (
        patch("jobs.ingest_match_data.fetch_fpl_data", return_value=_make_fpl_result()),
        patch("jobs.ingest_match_data.fetch_api_sports_data", return_value=_sports_rv),
        patch("jobs.ingest_match_data.delta_write") as mock_write,
    ):
        run(dry_run=True)

    mock_write.assert_not_called()


def test_run_dry_run_still_fetches():
    """run(dry_run=True) still calls both fetch functions to verify API reachability."""
    _sports_rv = {"standings": [], "season": 2025}
    with (
        patch("jobs.ingest_match_data.fetch_fpl_data", return_value=_make_fpl_result()) as mock_fpl,
        patch(
            "jobs.ingest_match_data.fetch_api_sports_data", return_value=_sports_rv
        ) as mock_sports,  # noqa: E501
        patch("jobs.ingest_match_data.delta_write"),
    ):
        run(dry_run=True)

    mock_fpl.assert_called_once()
    mock_sports.assert_called_once()
