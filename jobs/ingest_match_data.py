"""
Ingestion job: match and player data → PostgreSQL.

Fetches Premier League match and player data from two live sources concurrently,
then performs a delta write to PostgreSQL using only the data that has arrived
since the last recorded entry. Thread coordination is enforced via
concurrent.futures.wait() so Thread 3 (the writer) never starts until both fetch
threads have finished.

Threads:
  Thread 1 — FPL API: bootstrap-static (players, teams, gameweeks) + /fixtures/
  Thread 2 — API-Sports: current-season PL standings (supplementary context)
  Thread 3 — Delta write: find last kickoff_time in PostgreSQL, upsert only
             newer fixtures and their player stats to the existing schema

Thread 3 starts only after Threads 1 and 2 complete. If a fetch thread fails,
its result is treated as None and the delta write proceeds with whatever data
is available — partial failure never aborts the entire job.

PostgreSQL schema (read from db.py docstring and etl_v2.py):
  seasons, teams, gameweeks, players, fixtures, gw_player_stats

Run from the sports-context-mcp directory:
    python -m jobs.ingest_match_data

Cron: see .github/workflows/ingest_match_data.yml for schedule.
"""

import logging
import traceback
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from typing import Any

import psycopg2
import psycopg2.extras
import requests

from config import cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

_FPL_BASE = "https://fantasy.premierleague.com/api"
_SPORTS_BASE = "https://v3.football.api-sports.io"
_PL_LEAGUE_ID = 39  # API-Sports Premier League ID
_POSITION_MAP = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}

# psycopg2 connection options
_CONNECT_TIMEOUT = 10  # seconds


# ---------------------------------------------------------------------------
# HTTP helpers (synchronous — no asyncio in this module)
# ---------------------------------------------------------------------------


def _fpl_get(path: str, timeout: int = 30) -> dict:
    """
    Perform a synchronous GET request to the FPL API.

    Args:
        path:    Path relative to the FPL API base URL (e.g. '/bootstrap-static/').
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        requests.HTTPError: On non-2xx responses.
    """
    resp = requests.get(
        f"{_FPL_BASE}{path}",
        timeout=timeout,
        headers={"User-Agent": "sports-context-mcp/0.1"},
    )
    resp.raise_for_status()
    return resp.json()


def _sports_get(path: str, params: dict | None = None, timeout: int = 30) -> dict:
    """
    Perform a synchronous GET request to the API-Sports football API.

    Args:
        path:    Path relative to the API-Sports base URL (e.g. '/standings').
        params:  Optional query parameters.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        requests.HTTPError: On non-2xx responses.
    """
    resp = requests.get(
        f"{_SPORTS_BASE}{path}",
        params=params or {},
        headers={
            "x-apisports-key": cfg.api_sports_key,
            "User-Agent": "sports-context-mcp/0.1",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Thread 1: FPL data fetch
# ---------------------------------------------------------------------------


def fetch_fpl_data() -> dict[str, Any]:
    """
    Fetch current-season data from the FPL API.

    Retrieves bootstrap-static (players, teams, gameweeks) and the full fixture
    list in two sequential HTTP requests. Both are fast (~1–2 s each) so running
    them sequentially within this thread is preferable to adding more concurrency.

    Returns:
        Dict with keys:
          'bootstrap' — the full bootstrap-static JSON response
          'fixtures'  — the full /fixtures/ JSON response (list of fixture dicts)

    Raises:
        Exception: Any network or HTTP error is propagated to the caller
                   (ThreadPoolExecutor captures it as the Future's exception).
    """
    log.info("[Thread-FPL] fetching bootstrap-static...")
    bootstrap = _fpl_get("/bootstrap-static/")

    log.info("[Thread-FPL] fetching fixtures...")
    fixtures = _fpl_get("/fixtures/")

    log.info(
        "[Thread-FPL] done — %d players, %d fixtures.",
        len(bootstrap.get("elements", [])),
        len(fixtures),
    )
    return {"bootstrap": bootstrap, "fixtures": fixtures}


# ---------------------------------------------------------------------------
# Thread 2: API-Sports supplementary data fetch
# ---------------------------------------------------------------------------


def fetch_api_sports_data() -> dict[str, Any]:
    """
    Fetch supplementary Premier League data from API-Sports.

    Retrieves current-season PL standings, which give team form, goal difference,
    and league position — data that the FPL API does not expose. The current
    season start year is inferred from today's date (July = season transition).

    Returns:
        Dict with keys:
          'standings' — list of standing objects from API-Sports /standings response
          'season'    — integer start year of the current season (e.g. 2025)

    Raises:
        Exception: Any network, HTTP, or missing-key error is propagated to the caller.
    """
    if not cfg.api_sports_key:
        # Degrade gracefully if the key isn't configured rather than crashing the job.
        log.warning("[Thread-Sports] API_SPORTS_KEY not set — skipping supplementary fetch.")
        return {"standings": [], "season": _current_season_start_year()}

    season = _current_season_start_year()
    log.info("[Thread-Sports] fetching PL standings for season %d...", season)

    data = _sports_get("/standings", {"league": _PL_LEAGUE_ID, "season": season})
    standings_wrapper = data.get("response", [])

    # API-Sports nests standings: response[0].league.standings[0] is the table.
    standings: list[dict] = []
    if standings_wrapper:
        league_data = standings_wrapper[0].get("league", {})
        all_standings = league_data.get("standings", [[]])
        if all_standings:
            standings = all_standings[0]  # first group = the main PL table

    log.info("[Thread-Sports] done — %d teams in standings.", len(standings))
    return {"standings": standings, "season": season}


def _current_season_start_year() -> int:
    """
    Determine the start year of the current PL season from today's date.

    The PL season starts in August, so:
      - Jan–Jul → the season that started the previous year (e.g. Jan 2026 → 2025)
      - Aug–Dec → the season starting this year (e.g. Aug 2025 → 2025)

    Returns:
        Integer start year (e.g. 2025 for the 2025/26 season).
    """
    now = datetime.now(UTC)
    return now.year if now.month >= 8 else now.year - 1


# ---------------------------------------------------------------------------
# Thread 3: Delta write to PostgreSQL
# ---------------------------------------------------------------------------


def _get_db_conn(etl: bool = False) -> psycopg2.extensions.connection:
    """
    Open and return a synchronous psycopg2 connection.

    Args:
        etl: If True, use the read/write ETL connection string. Otherwise use
             the read-only connection string (for the delta timestamp query).

    Returns:
        psycopg2 connection object with autocommit disabled.

    Raises:
        RuntimeError: If the required environment variable is not set.
    """
    url = cfg.database_etl_url if etl else cfg.database_url
    if not url:
        raise RuntimeError(
            "DATABASE_ETL_URL (or DATABASE_URL) must be set to run ingest_match_data."
        )
    # psycopg2 accepts standard PostgreSQL DSNs (postgresql://user:pass@host/db).
    conn = psycopg2.connect(url, connect_timeout=_CONNECT_TIMEOUT)
    conn.autocommit = False
    return conn


def _get_last_kickoff(conn: psycopg2.extensions.connection) -> datetime | None:
    """
    Query PostgreSQL for the latest fixture kickoff_time in the current season.

    This timestamp is used as the delta boundary — only fixtures newer than this
    are fetched and written during the current run.

    Args:
        conn: Open psycopg2 connection (read-only is fine).

    Returns:
        The most recent kickoff_time as a timezone-aware datetime, or None if the
        fixtures table is empty for the current season.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(f.kickoff_time)
            FROM fixtures f
            JOIN seasons s ON f.season_id = s.id
            WHERE s.is_current = TRUE
            """
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def _parse_dt(value: str | None) -> datetime | None:
    """
    Parse an ISO 8601 datetime string into a timezone-aware datetime.

    Args:
        value: ISO 8601 string (with or without trailing Z).

    Returns:
        Timezone-aware datetime, or None if value is empty or unparseable.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _upsert_season(cur, bootstrap: dict) -> int:
    """
    Ensure the current season row exists in the seasons table and return its id.

    Clears any stale is_current flags on other rows before marking this season
    as current — mirrors the logic in pipeline/etl_v2.py:upsert_current_season.

    Args:
        cur:       psycopg2 cursor.
        bootstrap: FPL bootstrap-static JSON response.

    Returns:
        The integer primary key of the current season row.
    """
    events = bootstrap.get("events", [])
    if not events:
        raise ValueError("bootstrap-static returned no events — cannot determine current season.")

    # Derive season label and start year from the first event's deadline_time.
    year = int(events[0]["deadline_time"][:4])
    label = f"{year}/{str(year + 1)[2:]}"

    cur.execute("UPDATE seasons SET is_current = FALSE WHERE is_current = TRUE")
    cur.execute(
        """
        INSERT INTO seasons (label, start_year, is_current)
        VALUES (%s, %s, TRUE)
        ON CONFLICT (label) DO UPDATE
            SET is_current = TRUE, start_year = EXCLUDED.start_year
        RETURNING id
        """,
        (label, year),
    )
    row = cur.fetchone()
    season_id: int = row[0]
    log.info("[Thread-Write] season: %s (id=%d)", label, season_id)
    return season_id


def _upsert_teams(cur, season_id: int, bootstrap: dict) -> None:
    """
    Upsert all PL teams from the FPL bootstrap-static response.

    Args:
        cur:       psycopg2 cursor.
        season_id: Current season primary key.
        bootstrap: FPL bootstrap-static JSON response.
    """
    teams = bootstrap.get("teams", [])
    for t in teams:
        cur.execute(
            """
            INSERT INTO teams (
                season_id, fpl_id, name, short_name, strength,
                strength_attack_home, strength_attack_away,
                strength_defence_home, strength_defence_away
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season_id, fpl_id) DO UPDATE SET
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                strength = EXCLUDED.strength,
                strength_attack_home = EXCLUDED.strength_attack_home,
                strength_attack_away = EXCLUDED.strength_attack_away,
                strength_defence_home = EXCLUDED.strength_defence_home,
                strength_defence_away = EXCLUDED.strength_defence_away
            """,
            (
                season_id,
                t["id"],
                t["name"],
                t.get("short_name"),
                t.get("strength"),
                t.get("strength_attack_home"),
                t.get("strength_attack_away"),
                t.get("strength_defence_home"),
                t.get("strength_defence_away"),
            ),
        )
    log.info("[Thread-Write] upserted %d teams.", len(teams))


def _upsert_players(cur, season_id: int, bootstrap: dict) -> None:
    """
    Upsert all FPL players from the bootstrap-static response.

    Args:
        cur:       psycopg2 cursor.
        season_id: Current season primary key.
        bootstrap: FPL bootstrap-static JSON response.
    """
    players = bootstrap.get("elements", [])
    for p in players:
        position = _POSITION_MAP.get(p["element_type"], "MID")

        def _f(key: str) -> float | None:
            """Parse a string field to float, returning None if empty."""
            v = p.get(key)
            return float(v) if v else None

        cur.execute(
            """
            INSERT INTO players (
                season_id, fpl_id, team_fpl_id, first_name, second_name, web_name,
                position, now_cost, total_points, minutes, goals_scored, assists,
                clean_sheets, goals_conceded, yellow_cards, red_cards, bonus,
                form, points_per_game, selected_by_percent,
                transfers_in_event, transfers_out_event, status,
                chance_of_playing_next_round, news,
                creativity, influence, threat, ict_index,
                expected_goals, expected_assists, expected_goal_involvements,
                updated_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()
            )
            ON CONFLICT (season_id, fpl_id) DO UPDATE SET
                team_fpl_id = EXCLUDED.team_fpl_id,
                now_cost = EXCLUDED.now_cost,
                total_points = EXCLUDED.total_points,
                minutes = EXCLUDED.minutes,
                goals_scored = EXCLUDED.goals_scored,
                assists = EXCLUDED.assists,
                clean_sheets = EXCLUDED.clean_sheets,
                goals_conceded = EXCLUDED.goals_conceded,
                yellow_cards = EXCLUDED.yellow_cards,
                red_cards = EXCLUDED.red_cards,
                bonus = EXCLUDED.bonus,
                form = EXCLUDED.form,
                points_per_game = EXCLUDED.points_per_game,
                selected_by_percent = EXCLUDED.selected_by_percent,
                transfers_in_event = EXCLUDED.transfers_in_event,
                transfers_out_event = EXCLUDED.transfers_out_event,
                status = EXCLUDED.status,
                chance_of_playing_next_round = EXCLUDED.chance_of_playing_next_round,
                news = EXCLUDED.news,
                creativity = EXCLUDED.creativity,
                influence = EXCLUDED.influence,
                threat = EXCLUDED.threat,
                ict_index = EXCLUDED.ict_index,
                expected_goals = EXCLUDED.expected_goals,
                expected_assists = EXCLUDED.expected_assists,
                expected_goal_involvements = EXCLUDED.expected_goal_involvements,
                updated_at = NOW()
            """,
            (
                season_id,
                p["id"],
                p["team"],
                p["first_name"],
                p["second_name"],
                p["web_name"],
                position,
                p.get("now_cost"),
                p.get("total_points"),
                p.get("minutes"),
                p.get("goals_scored"),
                p.get("assists"),
                p.get("clean_sheets"),
                p.get("goals_conceded"),
                p.get("yellow_cards"),
                p.get("red_cards"),
                p.get("bonus"),
                _f("form"),
                _f("points_per_game"),
                _f("selected_by_percent"),
                p.get("transfers_in_event"),
                p.get("transfers_out_event"),
                p.get("status"),
                p.get("chance_of_playing_next_round"),
                p.get("news"),
                _f("creativity"),
                _f("influence"),
                _f("threat"),
                _f("ict_index"),
                _f("expected_goals"),
                _f("expected_assists"),
                _f("expected_goal_involvements"),
            ),
        )
    log.info("[Thread-Write] upserted %d players.", len(players))


def _upsert_new_fixtures(
    cur,
    season_id: int,
    fixtures: list[dict],
    last_kickoff: datetime | None,
) -> list[dict]:
    """
    Upsert only fixtures whose kickoff_time is newer than last_kickoff.

    This is the delta: on first run (last_kickoff=None) all fixtures are written;
    on subsequent runs only newly scheduled or played matches are written.

    Args:
        cur:          psycopg2 cursor.
        season_id:    Current season primary key.
        fixtures:     Full list of fixture dicts from the FPL /fixtures/ endpoint.
        last_kickoff: The latest kickoff_time already in the database, or None.

    Returns:
        List of the new fixture dicts that were actually upserted, so callers can
        decide whether to fetch GW player stats for them.
    """
    new_fixtures = []
    for f in fixtures:
        kickoff_str = f.get("kickoff_time")
        if not kickoff_str:
            continue  # Skip fixtures without a scheduled kickoff (e.g. postponed)

        kickoff_dt = _parse_dt(kickoff_str)
        if kickoff_dt is None:
            continue

        # Delta filter: skip fixtures we've already recorded.
        if last_kickoff and kickoff_dt <= last_kickoff:
            continue

        # Note: FPL API swaps team_h_difficulty and team_a_difficulty — the
        # difficulty figure is from the *opponent's* perspective, so they need
        # to be swapped when storing home_team_difficulty and away_team_difficulty.
        cur.execute(
            """
            INSERT INTO fixtures (
                season_id, fpl_id, gw_number, kickoff_time,
                home_team_fpl_id, away_team_fpl_id,
                home_score, away_score, finished, started,
                home_team_difficulty, away_team_difficulty
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season_id, fpl_id) DO UPDATE SET
                gw_number = EXCLUDED.gw_number,
                kickoff_time = EXCLUDED.kickoff_time,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                finished = EXCLUDED.finished,
                started = EXCLUDED.started,
                home_team_difficulty = EXCLUDED.home_team_difficulty,
                away_team_difficulty = EXCLUDED.away_team_difficulty
            """,
            (
                season_id,
                f["id"],
                f.get("event"),
                kickoff_dt,
                f["team_h"],
                f["team_a"],
                f.get("team_h_score"),
                f.get("team_a_score"),
                f.get("finished") or False,
                f.get("started") or False,
                # Swap: team_a_difficulty is the difficulty FOR the home team
                f.get("team_a_difficulty"),
                f.get("team_h_difficulty"),
            ),
        )
        new_fixtures.append(f)

    log.info(
        "[Thread-Write] upserted %d new fixtures (delta from %s).",
        len(new_fixtures),
        last_kickoff,
    )
    return new_fixtures


def _fetch_and_upsert_player_stats(
    cur,
    season_id: int,
    new_fixtures: list[dict],
) -> int:
    """
    For each newly-upserted fixture, fetch per-player GW stats from the FPL API
    and write them to gw_player_stats.

    Only finished fixtures are processed — in-progress or future fixtures have no
    stats to collect yet.

    Args:
        cur:          psycopg2 cursor.
        season_id:    Current season primary key.
        new_fixtures: List of fixture dicts returned by _upsert_new_fixtures.

    Returns:
        Total number of gw_player_stats rows upserted.
    """
    finished = [f for f in new_fixtures if f.get("finished")]
    log.info(
        "[Thread-Write] fetching GW player stats for %d finished fixtures...",
        len(finished),
    )

    total_rows = 0
    for f in finished:
        gw = f.get("event")
        if gw is None:
            continue  # Fixture has no assigned GW (e.g. postponed without reschedule)

        # The FPL element-summary endpoint returns per-player GW history. We collect
        # only the entry matching this fixture_fpl_id to avoid duplicates.
        fixture_id = f["id"]
        home_team_id = f["team_h"]
        away_team_id = f["team_a"]

        # Build a mapping of fpl_id → (gw history list) by fetching all involved players.
        # This is done sequentially within the write thread to keep things simple.
        for team_id in [home_team_id, away_team_id]:
            # Get players for this team from the players table (already upserted above).
            cur.execute(
                "SELECT fpl_id FROM players WHERE season_id = %s AND team_fpl_id = %s",
                (season_id, team_id),
            )
            player_rows = cur.fetchall()

            for (player_fpl_id,) in player_rows:
                try:
                    data = _fpl_get(f"/element-summary/{player_fpl_id}/")
                except Exception as exc:
                    log.warning(
                        "[Thread-Write] failed to fetch element-summary for player %d: %s",
                        player_fpl_id,
                        exc,
                    )
                    continue

                history = data.get("history", [])
                for g in history:
                    if g.get("fixture") != fixture_id:
                        continue  # Only write the row for this specific fixture

                    def _gf(key: str) -> float | None:
                        """Parse a string field to float, returning None if empty."""
                        v = g.get(key)
                        return float(v) if v else None

                    try:
                        cur.execute(
                            """
                            INSERT INTO gw_player_stats (
                                season_id, player_fpl_id, gw_number, fixture_fpl_id,
                                opponent_team_fpl_id, was_home,
                                team_h_score, team_a_score,
                                minutes, goals_scored, assists, clean_sheets,
                                goals_conceded, own_goals, penalties_saved,
                                penalties_missed, yellow_cards, red_cards,
                                saves, bonus, bps, total_points,
                                value, selected, transfers_in, transfers_out,
                                transfers_balance,
                                influence, creativity, threat, ict_index,
                                expected_goals, expected_assists,
                                expected_goal_involvements, expected_goals_conceded,
                                starts
                            ) VALUES (
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s
                            )
                            ON CONFLICT (season_id, player_fpl_id, fixture_fpl_id) DO UPDATE SET
                                total_points = EXCLUDED.total_points,
                                minutes = EXCLUDED.minutes,
                                goals_scored = EXCLUDED.goals_scored,
                                assists = EXCLUDED.assists,
                                clean_sheets = EXCLUDED.clean_sheets,
                                bonus = EXCLUDED.bonus,
                                bps = EXCLUDED.bps,
                                expected_goals = EXCLUDED.expected_goals,
                                expected_assists = EXCLUDED.expected_assists,
                                expected_goal_involvements = EXCLUDED.expected_goal_involvements,
                                expected_goals_conceded = EXCLUDED.expected_goals_conceded
                            """,
                            (
                                season_id,
                                player_fpl_id,
                                g["round"],
                                fixture_id,
                                g["opponent_team"],
                                g["was_home"],
                                g.get("team_h_score"),
                                g.get("team_a_score"),
                                g.get("minutes", 0),
                                g.get("goals_scored", 0),
                                g.get("assists", 0),
                                g.get("clean_sheets", 0),
                                g.get("goals_conceded", 0),
                                g.get("own_goals", 0),
                                g.get("penalties_saved", 0),
                                g.get("penalties_missed", 0),
                                g.get("yellow_cards", 0),
                                g.get("red_cards", 0),
                                g.get("saves", 0),
                                g.get("bonus", 0),
                                g.get("bps", 0),
                                g.get("total_points", 0),
                                g.get("value"),
                                g.get("selected"),
                                g.get("transfers_in"),
                                g.get("transfers_out"),
                                g.get("transfers_balance"),
                                _gf("influence"),
                                _gf("creativity"),
                                _gf("threat"),
                                _gf("ict_index"),
                                _gf("expected_goals"),
                                _gf("expected_assists"),
                                _gf("expected_goal_involvements"),
                                _gf("expected_goals_conceded"),
                                g.get("starts"),
                            ),
                        )
                        total_rows += 1
                    except Exception as exc:
                        log.warning(
                            "[Thread-Write] failed to upsert stat row player=%d fixture=%d: %s",
                            player_fpl_id,
                            fixture_id,
                            exc,
                        )

    log.info("[Thread-Write] upserted %d gw_player_stats rows.", total_rows)
    return total_rows


def delta_write(fpl_result: dict | None, sports_result: dict | None) -> None:
    """
    Thread 3: write fetched data to PostgreSQL, only what's new since last run.

    Must not be called until Threads 1 and 2 have both finished (enforced by the
    orchestrator using concurrent.futures.wait). Operates on whichever of the two
    fetch results is available — partial failure in a fetch thread does not prevent
    writing the data that was successfully retrieved.

    Execution steps:
      1. Open a read-only connection to determine the delta boundary (last kickoff).
      2. Open a read/write ETL connection for all writes.
      3. Upsert season, teams, players, and gameweeks from FPL bootstrap.
      4. Upsert only fixtures newer than the delta boundary.
      5. For finished new fixtures, fetch and upsert per-player GW stats.
      6. Log supplementary standings data from API-Sports (no write needed —
         the existing schema does not have a standings table; this data is available
         via the Pinecone RAG or via API-Sports queries at serve time).
      7. Commit everything atomically.

    Args:
        fpl_result:    Return value of fetch_fpl_data(), or None if that thread failed.
        sports_result: Return value of fetch_api_sports_data(), or None if that thread failed.
    """
    if fpl_result is None:
        log.error("[Thread-Write] FPL fetch failed — no data to write. Aborting delta write.")
        return

    # Step 1: determine delta boundary using a read-only connection.
    log.info("[Thread-Write] querying for last kickoff_time...")
    try:
        ro_conn = _get_db_conn(etl=False)
        try:
            last_kickoff = _get_last_kickoff(ro_conn)
        finally:
            ro_conn.close()
    except Exception as exc:
        log.error(
            "[Thread-Write] failed to read last kickoff from DB: %s",
            exc,
            exc_info=True,
        )
        return

    log.info("[Thread-Write] last recorded kickoff: %s", last_kickoff)

    # Step 2: open the ETL (read/write) connection for all writes.
    try:
        etl_conn = _get_db_conn(etl=True)
    except Exception as exc:
        log.error("[Thread-Write] failed to open ETL DB connection: %s", exc, exc_info=True)
        return

    try:
        with etl_conn.cursor() as cur:
            bootstrap = fpl_result["bootstrap"]
            fixtures = fpl_result["fixtures"]

            # Step 3: upsert season, teams, players.
            season_id = _upsert_season(cur, bootstrap)
            _upsert_teams(cur, season_id, bootstrap)
            _upsert_players(cur, season_id, bootstrap)

            # Step 4: delta fixture upsert.
            new_fixtures = _upsert_new_fixtures(cur, season_id, fixtures, last_kickoff)

            # Step 5: GW player stats for newly-finished fixtures.
            if new_fixtures:
                _fetch_and_upsert_player_stats(cur, season_id, new_fixtures)

        # Step 6: log API-Sports standings if available (informational only).
        if sports_result:
            standings = sports_result.get("standings", [])
            if standings:
                top3 = ", ".join(f"{s['rank']}. {s['team']['name']}" for s in standings[:3])
                log.info("[Thread-Write] API-Sports top 3: %s", top3)

        # Step 7: commit atomically — all-or-nothing.
        etl_conn.commit()
        log.info("[Thread-Write] delta write committed successfully.")

    except Exception as exc:
        etl_conn.rollback()
        log.error(
            "[Thread-Write] delta write failed — rolling back: %s",
            exc,
            exc_info=True,
        )
    finally:
        etl_conn.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(dry_run: bool = False) -> None:
    """
    Run the full match data ingestion pipeline.

    Execution order:
      1. Submit Thread 1 (FPL fetch) and Thread 2 (API-Sports fetch) concurrently.
      2. Block with concurrent.futures.wait() until BOTH fetch threads complete.
      3. Collect results, logging full tracebacks for any thread that raised.
      4. Submit Thread 3 (delta write) with the combined results.
      5. Wait for Thread 3 to finish.

    Thread 3 is guaranteed not to start until Threads 1 and 2 are both done.
    Partial fetch failure (one thread raises) does not abort the job — delta_write
    handles None results gracefully.

    Args:
        dry_run: When True, fetch threads run normally but the delta write (Thread 3)
                 is skipped. Logs the fixture and player counts that would have been
                 written so you can verify API reachability and data shape before
                 committing to a live run. Can also be enabled via DRY_RUN=true.
    """
    log.info("=== ingest_match_data: starting%s ===", " [DRY RUN]" if dry_run else "")

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="ingest-match") as executor:
        # Submit fetch threads.
        f1: Future = executor.submit(fetch_fpl_data)
        f2: Future = executor.submit(fetch_api_sports_data)

        # Block until BOTH fetch threads finish (success or failure).
        log.info("Waiting for fetch threads (FPL + API-Sports) to complete...")
        wait([f1, f2])  # concurrent.futures.wait — returns only when all are done
        log.info("Both fetch threads finished.")

        # Collect results, logging full tracebacks for failures.
        fpl_result: dict | None = None
        if f1.exception():
            exc1 = f1.exception()
            tb1 = "".join(traceback.format_exception(type(exc1), exc1, exc1.__traceback__))
            log.error("Thread-FPL raised an exception:\n%s", tb1)
        else:
            fpl_result = f1.result()

        sports_result: dict | None = None
        if f2.exception():
            exc2 = f2.exception()
            tb2 = "".join(traceback.format_exception(type(exc2), exc2, exc2.__traceback__))
            log.error("Thread-Sports raised an exception:\n%s", tb2)
        else:
            sports_result = f2.result()

        if dry_run:
            players = len((fpl_result or {}).get("bootstrap", {}).get("elements", []))
            fixtures = len((fpl_result or {}).get("fixtures", []))
            log.info(
                "[dry run] would delta-write %d players and %d fixtures. "
                "No PostgreSQL writes made.",
                players,
                fixtures,
            )
            log.info("=== ingest_match_data: complete [DRY RUN] ===")
            return

        # Submit Thread 3 now that Threads 1 and 2 are guaranteed to be done.
        f3: Future = executor.submit(delta_write, fpl_result, sports_result)
        f3.result()  # Wait for the write thread; re-raises on unhandled exception.

    log.info("=== ingest_match_data: complete ===")


if __name__ == "__main__":
    run(dry_run=cfg.dry_run)
