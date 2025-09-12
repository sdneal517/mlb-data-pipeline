# build_watchability_gold.py

import sqlite3
import pandas as pd
from datetime import datetime
from common import get_conn, log


def build_watchability_gold():
    """Transform watchability_silver into watchability_gold for reporting."""

    # Load watchability_silver
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM watchability_silver", conn)

    # --- Transformations ---
    # Format game_date as MM-DD
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.strftime("%m-%d")

    # Round point columns
    df["score"] = df["score"].round(0).astype(int)
    df["playoff_pts_final"] = df["playoff_pts_final"].round(0).astype(int)
    df["quality_pts"] = df["quality_pts"].round(0).astype(int)

    # Select and rename columns in desired order
    df_gold = df.rename(columns={
        "game_date": "GameDate",
        "game_time": "GameTime",
        "away_team": "Away_Team",
        "home_team": "Home_Team",
        "score": "Total_Points",
        "playoff_pts_final": "Playoff_Pts",
        "quality_pts": "Quality_Pts"
    })[[
        "GameDate",
        "GameTime",
        "Away_Team",
        "Home_Team",
        "Total_Points",
        "Playoff_Pts",
        "Quality_Pts"
    ]]

    # Write to SQLite as watchability_gold
    with get_conn() as conn:
        df_gold.to_sql("watchability_gold", conn, if_exists="replace", index=False)

    log.info(f"watchability_gold updated with {len(df_gold)} rows")
    print(df_gold.head())


def main():
    build_watchability_gold()


if __name__ == "__main__":
    main()
