# refresh_teams_silver.py

import requests
import sqlite3
from common import get_conn, log

API_URL = "https://statsapi.mlb.com/api/v1/teams"

def fetch_teams():
    log.info("Fetching teams data...")
    resp = requests.get(API_URL, params={"sportId": 1})  # MLB = sportId 1
    resp.raise_for_status()
    return resp.json()

def transform_teams(data):
    rows = []
    for team in data.get("teams", []):
        rows.append({
            "team_id": team["id"],
            "team_name": team["name"],
            "location": team.get("locationName"),
            "abbrev": team.get("abbreviation"),
            "league": team["league"]["name"] if "league" in team else None,
            "division": team["division"]["name"] if "division" in team else None,
        })
    return rows

def load_teams(rows):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM teams_silver")
    cur.executemany("""
        INSERT INTO teams_silver (team_id, team_name, location, abbrev, league, division)
        VALUES (:team_id, :team_name, :location, :abbrev, :league, :division)
    """, rows)
    conn.commit()
    conn.close()
    log.info(f"Inserted {len(rows)} rows into teams_silver")

def main():
    data = fetch_teams()
    rows = transform_teams(data)
    load_teams(rows)

if __name__ == "__main__":
    main()
