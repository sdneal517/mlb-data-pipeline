# refresh_schedule_silver.py

import requests
import sqlite3
from common import get_conn, log

API_URL = "https://statsapi.mlb.com/api/v1/schedule"

def fetch_schedule():
    log.info("Fetching schedule data...")
    resp = requests.get(API_URL, params={"sportId": 1, "date": "09/11/2025"})  # Example date
    resp.raise_for_status()
    return resp.json()

def transform_schedule(data):
    rows = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            rows.append({
                "game_pk": game["gamePk"],
                "game_date": date["date"],
                "home_team": game["teams"]["home"]["team"]["name"],
                "away_team": game["teams"]["away"]["team"]["name"],
                "status": game["status"]["detailedState"],
                "game_time": game.get("gameDate"),
            })
    return rows

def load_schedule(rows):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM schedule_silver")
    cur.executemany("""
        INSERT INTO schedule_silver (game_pk, game_date, home_team, away_team, status, game_time)
        VALUES (:game_pk, :game_date, :home_team, :away_team, :status, :game_time)
    """, rows)
    conn.commit()
    conn.close()
    log.info(f"Inserted {len(rows)} rows into schedule_silver")

def main():
    data = fetch_schedule()
    rows = transform_schedule(data)
    load_schedule(rows)

if __name__ == "__main__":
    main()
