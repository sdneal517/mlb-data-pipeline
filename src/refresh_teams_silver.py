# refresh_teams_silver.py

import statsapi
import pandas as pd
import sqlite3
from datetime import datetime
import os

def fetch_teams():
    """Fetch raw teams data from the MLB StatsAPI."""
    data = statsapi.get("teams")
    return data.get("teams", [])

def transform_teams(teams_list):
    """Clean and filter MLB teams (sport_id = 1)."""
    mlb_teams = []
    for t in teams_list:
        sport_id = t.get("sport", {}).get("id")
        if sport_id == 1:  # keep only Major League Baseball
            mlb_teams.append({
                "id": t["id"],
                "name": t["name"],
                "abbreviation": t["abbreviation"],
                "division": t.get("division", {}).get("name"),
                "league": t.get("league", {}).get("name"),
                "location": t["locationName"],
                "first_year": t["firstYearOfPlay"]
            })
    return mlb_teams

def load_teams(clean_teams):
    """Write transformed teams into SQLite teams_silver table."""
    df = pd.DataFrame(clean_teams)
    
    # Add metadata timestamps
    df["created_at"] = datetime.now()
    df["last_updated"] = datetime.now()

    # Rename columns for consistency
    df = df.rename(columns={
        "id": "team_id",
        "name": "team_name",
        "abbreviation": "team_abbr",
        "division": "division_name",
        "league": "league_name",
        "location": "location_name"
    })

    # Write to SQLite (overwrite table)
    db_path = os.path.join(os.getcwd(), "mlb_data.db")
    conn = sqlite3.connect(db_path)
    df.to_sql("teams_silver", conn, if_exists="replace", index=False)
    conn.close()

    print(f"teams_silver written: {len(df)} rows")

def main():
    teams_list = fetch_teams()
    clean_teams = transform_teams(teams_list)
    load_teams(clean_teams)

if __name__ == "__main__":
    main()
