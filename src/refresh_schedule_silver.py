# refresh_schedule_silver.py

import statsapi
import pandas as pd
import sqlite3
from datetime import date, datetime
import os

def fetch_schedule_today():
    """Fetch ONLY today's schedule for MLB (sportId=1)."""
    today = date.today().isoformat()
    return statsapi.get(
        "schedule",
        {"sportId": 1, "startDate": today, "endDate": today}
    )

def transform_schedule(data):
    """Parse raw schedule data into clean rows (today only)."""
    clean_games = []
    for bucket in data.get("dates", []):
        for g in bucket.get("games", []):
            status = g.get("status", {}) or {}
            teams  = g.get("teams", {}) or {}
            home   = (teams.get("home") or {}).get("team") or {}
            away   = (teams.get("away") or {}).get("team") or {}
            venue  = g.get("venue", {}) or {}

            clean_games.append({
                "game_pk": g.get("gamePk"),
                "official_date": g.get("officialDate"),     # YYYY-MM-DD
                "game_datetime_utc": g.get("gameDate"),     # ISO UTC
                "status_code": status.get("statusCode"),
                "status_detailed": status.get("detailedState"),
                "game_type": g.get("gameType"),
                "series_game_number": g.get("seriesGameNumber"),
                "series_description": g.get("seriesDescription"),
                "doubleheader": g.get("doubleHeader"),
                "day_night": g.get("dayNight"),
                "scheduled_innings": g.get("scheduledInnings"),
                "home_team_id": home.get("id"),
                "home_team_name": home.get("name"),
                "away_team_id": away.get("id"),
                "away_team_name": away.get("name"),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("name"),
                "created_at": datetime.now(),
                "last_updated": datetime.now()
            })
    return pd.DataFrame(clean_games)

def load_schedule(df):
    """Write schedule data into SQLite (overwrite table)."""
    db_path = os.path.join(os.getcwd(), "mlb_data.db")
    conn = sqlite3.connect(db_path)
    df.to_sql("schedule_silver", conn, if_exists="replace", index=False)
    conn.close()
    print(f"schedule_silver written: {len(df)} rows")

def main():
    raw = fetch_schedule_today()
    df = transform_schedule(raw)
    load_schedule(df)

if __name__ == "__main__":
    main()
