# build_watchability_gold.py

import sqlite3
import pandas as pd
from common import get_conn, log

def sliding_playoff_points(distance):
    """
    Convert wild card/GB distance into playoff points:
      0 GB   -> 30 pts
      3 GB   -> 15 pts
      6+ GB  -> 0 pts
      Linear interpolation in between.
    """
    if distance is None:
        return 0
    try:
        distance = float(str(distance).replace("+", "").replace("-", ""))
    except Exception:
        return 0

    if distance <= 0:
        return 30
    elif distance >= 6:
        return 0
    elif distance <= 3:
        # interpolate between 0 and 3 GB
        return round(30 - (distance / 3 * 15), 1)
    else:
        # interpolate between 3 and 6 GB
        return round(15 - ((distance - 3) / 3 * 15), 1)

def load_silver_tables():
    """Load standings, teams, and schedule into DataFrames."""
    with get_conn() as conn:
        standings = pd.read_sql("SELECT * FROM standings_silver", conn)
        teams = pd.read_sql("SELECT * FROM teams_silver", conn)
        schedule = pd.read_sql("SELECT * FROM schedule_silver", conn)
    return standings, teams, schedule

def calculate_watchability(standings, teams, schedule):
    """Join Silver tables and calculate watchability scores."""
    log.info("Building watchability DataFrame...")

    # Apply playoff scoring to standings
    standings["playoff_pts"] = standings["wc_gb"].apply(sliding_playoff_points)

    # Join schedule with standings info for both teams
    df = schedule.merge(
        standings[["team_id", "team_name", "wc_gb", "playoff_pts"]],
        left_on="home_team", right_on="team_name", how="left"
    ).merge(
        standings[["team_id", "team_name", "wc_gb", "playoff_pts"]],
        left_on="away_team", right_on="team_name", how="left",
        suffixes=("_home", "_away")
    )

    # Quality points placeholder (can refine later)
    df["quality_pts"] = 10
    df["score"] = (
        df["playoff_pts_home"].fillna(0)
        + df["playoff_pts_away"].fillna(0)
        + df["quality_pts"]
    )

    return df[[
        "game_pk", "game_date", "game_time",
        "home_team", "away_team",
        "playoff_pts_home", "playoff_pts_away",
        "quality_pts", "score"
    ]]

def write_gold(df):
    """Write final watchability table to SQLite."""
    with get_conn() as conn:
        df.to_sql("watchability_gold", conn, if_exists="replace", index=False)
    log.info(f"✅ watchability_gold updated with {len(df)} rows")

def main():
    standings, teams, schedule = load_silver_tables()
    df = calculate_watchability(standings, teams, schedule)
    write_gold(df)
    print(df.head())  # quick peek in console

if __name__ == "__main__":
    main()
