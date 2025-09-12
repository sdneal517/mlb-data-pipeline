# refresh_standings_silver.py

import statsapi
import pandas as pd
import requests
import sqlite3
from datetime import date, datetime
import os

def fetch_standings(season, standings_date):
    """Fetch standings for both AL and NL."""
    clean_rows = []
    for league_id in [103, 104]:  # AL=103, NL=104
        data = statsapi.get("standings", {"leagueId": league_id, "season": season, "date": standings_date})
        for rec in data.get("records", []):
            league = rec.get("league", {}) or {}
            division = rec.get("division", {}) or {}
            for tr in rec.get("teamRecords", []):
                team = tr.get("team", {}) or {}
                clean_rows.append({
                    "season": season,
                    "standings_date": standings_date,
                    "league_id": league.get("id"),
                    "division_id": division.get("id"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "wins": tr.get("wins"),
                    "losses": tr.get("losses"),
                    "pct": tr.get("winningPercentage"),
                    "games_back": tr.get("gamesBack"),
                    "wc_games_back": tr.get("wildCardGamesBack"),
                    "division_leader": tr.get("divisionLeader"),
                    "created_at": datetime.now(),
                    "last_updated": datetime.now()
                })
    return pd.DataFrame(clean_rows)

def enrich_with_lookups(df):
    """Add league and division names via lookup tables."""
    leagues = requests.get("https://statsapi.mlb.com/api/v1/leagues").json()["leagues"]
    df_leagues = pd.DataFrame([{"league_id": l["id"], "league_name": l["name"]} for l in leagues])

    divs = requests.get("https://statsapi.mlb.com/api/v1/divisions").json()["divisions"]
    df_divs = pd.DataFrame([{"division_id": d["id"], "division_name": d["name"]} for d in divs])

    df = df.merge(df_leagues, on="league_id", how="left")
    df = df.merge(df_divs, on="division_id", how="left")
    return df

def load_standings(df):
    """Write standings data into SQLite."""
    db_path = os.path.join(os.getcwd(), "mlb_data.db")
    conn = sqlite3.connect(db_path)
    df.to_sql("standings_silver", conn, if_exists="replace", index=False)
    conn.close()
    print(f"standings_silver written: {len(df)} rows")

def main():
    today = date.today().isoformat()
    df = fetch_standings(season=2025, standings_date=today)
    df = enrich_with_lookups(df)
    load_standings(df)

if __name__ == "__main__":
    main()
