# refresh_standings_silver.py

import requests
import sqlite3
from common import get_conn, log

API_URL = "https://statsapi.mlb.com/api/v1/standings"

def fetch_standings():
    """Pull standings data from MLB Stats API."""
    log.info("Fetching standings data...")
    resp = requests.get(API_URL, params={"leagueId": "103,104"})  # AL=103, NL=104
    resp.raise_for_status()
    return resp.json()

def transform_standings(data):
    """Turn raw JSON into list of dicts ready for SQLite insert."""
    rows = []
    for record in data.get("records", []):
        division = record.get("division", {}).get("name")
        for team in record["teamRecords"]:
            rows.append({
                "team_id": team["team"]["id"],
                "team_name": team["team"]["name"],
                "division": division,
                "w": team["wins"],
                "l": team["losses"],
                "gb": team.get("gamesBack", "0"),
                "wc_gb": team.get("wildCardGamesBack", "0"),
            })
    return rows

def load_standings(rows):
    """Insert standings into standings_silver table."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM standings_silver")
    cur.executemany("""
        INSERT INTO standings_silver (team_id, team_name, division, w, l, gb, wc_gb)
        VALUES (:team_id, :team_name, :division, :w, :l, :gb, :wc_gb)
    """, rows)
    conn.commit()
    conn.close()
    log.info(f"Inserted {len(rows)} rows into standings_silver")

def main():
    data = fetch_standings()
    rows = transform_standings(data)
    load_standings(rows)

if __name__ == "__main__":
    main()
