# refresh_watchability_silver.py

import sqlite3
import pandas as pd
from datetime import datetime, date
import pytz
from common import get_conn, log


# --- Season-aware parameters ---
today = date.today()
is_early_season = today < date(today.year, 7, 1)

QUALITY_FLOOR = 0.520 if is_early_season else 0.530
MAX_SYNERGY_BONUS = 10 if is_early_season else 15


# --- Helpers ---
def wc_gb_value(team_id, standings_data):
    """Return numeric Wild Card distance for a team (0.0 if tied/leading)."""
    row = standings_data.loc[standings_data["team_id"] == team_id]
    if row.empty:
        return 999.0
    raw = str(row.iloc[0]["wc_games_back"])
    if raw in ("-", "+0", "+0.0", "0", "0.0", None):
        return 0.0
    try:
        return abs(float(raw.replace("+", "")))
    except Exception:
        return 999.0


def sliding_playoff_points(distance):
    """Convert distance (games back) into playoff points (0–4 GB stricter scale)."""
    if distance <= 0:
        return 30
    elif distance >= 4:
        return 0
    elif distance <= 2:
        return 30 - (distance / 2) * 15
    else:  # 2 < distance < 4
        return 15 - ((distance - 2) / 2) * 15


def playoff_points_single(team, team_id, standings):
    """Combine GB with win% floor for realistic playoff chances."""
    if team is None or team["winPct"] < 0.50:
        return 0
    div_gb = team["gamesBack"]
    wc_gb = wc_gb_value(team_id, standings)
    div_pts = sliding_playoff_points(div_gb)
    wc_pts = sliding_playoff_points(wc_gb)
    return max(div_pts, wc_pts)


def division_or_wc_head_to_head_bonus(home_id, away_id, standings):
    """Bonus if both teams are alive and either same division or both close in WC."""
    home_row = standings.loc[standings["team_id"] == home_id]
    away_row = standings.loc[standings["team_id"] == away_id]
    if home_row.empty or away_row.empty:
        return 0

    # Same division?
    if home_row.iloc[0]["division_id"] == away_row.iloc[0]["division_id"]:
        return 10

    # WC proximity (within 3 games each)
    home_wc = wc_gb_value(home_id, standings)
    away_wc = wc_gb_value(away_id, standings)
    if home_wc <= 3 and away_wc <= 3:
        return 10

    return 0


def build_watchability_silver(standings, schedule):
    """Calculate watchability scores and keep ALL intermediate columns (Silver)."""
    rows = []

    for _, g in schedule.iterrows():
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        game_pk = g["game_pk"]
        game_date = g["official_date"]

        # Parse game_datetime into ET string
        raw_iso = str(g.get("game_datetime_utc"))
        utc_dt = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        local_dt = utc_dt.astimezone(pytz.timezone("US/Eastern"))
        game_time = local_dt.strftime("%I:%M %p").lstrip("0") + " ET"

        # Lookup team records
        def team_record(team_id):
            row = standings.loc[standings["team_id"] == team_id]
            if row.empty:
                return None
            w, l = row.iloc[0]["wins"], row.iloc[0]["losses"]
            winPct = w / (w + l) if (w + l) > 0 else 0
            gb_raw = row.iloc[0]["games_back"]
            gb_val = 0.0 if gb_raw == "-" else float(gb_raw)
            return {
                "name": row.iloc[0]["team_name"],
                "winPct": winPct,
                "gamesBack": gb_val
            }

        home_team = team_record(home_id)
        away_team = team_record(away_id)

        playoff_pts_home = playoff_points_single(home_team, home_id, standings)
        playoff_pts_away = playoff_points_single(away_team, away_id, standings)
        playoff_pts = max(playoff_pts_home, playoff_pts_away)

        # Cap mismatches (one alive, one dead) at 30
        if (playoff_pts_home > 0 and playoff_pts_away == 0) or (playoff_pts_away > 0 and playoff_pts_home == 0):
            playoff_pts = min(playoff_pts, 30)

        # Both-alive synergy bonus
        both_alive_bonus = 0
        if playoff_pts_home > 0 and playoff_pts_away > 0:
            if playoff_pts_home >= 20 and playoff_pts_away >= 20:
                both_alive_bonus = min(15, MAX_SYNERGY_BONUS)
            elif playoff_pts_home >= 15 and playoff_pts_away >= 15:
                both_alive_bonus = min(10, MAX_SYNERGY_BONUS)
            elif playoff_pts_home >= 10 and playoff_pts_away >= 10:
                both_alive_bonus = min(5, MAX_SYNERGY_BONUS)
        playoff_pts += both_alive_bonus

        # Head-to-head bonus
        head2head_bonus = division_or_wc_head_to_head_bonus(home_id, away_id, standings)
        playoff_pts += head2head_bonus

        # Quality points (both ≥ threshold)
        quality_pts = 0
        if playoff_pts > 0 and home_team and away_team:
            if home_team["winPct"] >= QUALITY_FLOOR and away_team["winPct"] >= QUALITY_FLOOR:
                quality_pts = 20

        score = round(playoff_pts + quality_pts)

        rows.append({
            "game_pk": game_pk,
            "game_date": game_date,
            "game_time": game_time,
            "home_team": home_team["name"] if home_team else None,
            "away_team": away_team["name"] if away_team else None,
            "playoff_pts_home": playoff_pts_home,
            "playoff_pts_away": playoff_pts_away,
            "both_alive_bonus": both_alive_bonus,
            "head2head_bonus": head2head_bonus,
            "playoff_pts_final": playoff_pts,
            "quality_pts": quality_pts,
            "score": score,
            "created_at": datetime.now(),
            "last_updated": datetime.now()
        })

    return pd.DataFrame(rows)


def main():
    # Load silver inputs
    with get_conn() as conn:
        standings = pd.read_sql("SELECT * FROM standings_silver", conn)
        schedule = pd.read_sql("SELECT * FROM schedule_silver", conn)

    # SAFEGUARD: today-only
    today_str = date.today().isoformat()
    schedule = schedule[schedule["official_date"] == today_str].copy()

    df = build_watchability_silver(standings, schedule)

    # SAFEGUARD: handle empty dataframe
    if df.empty:
        log.warning("No games found for today — skipping watchability_silver refresh")
        return

    # Write to watchability_silver
    with get_conn() as conn:
        df.to_sql("watchability_silver", conn, if_exists="replace", index=False)

    log.info(f"watchability_silver updated with {len(df)} rows")
    print(df.head())


if __name__ == "__main__":
    main()
