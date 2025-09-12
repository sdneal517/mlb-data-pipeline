# init_standings_table.py
from common import get_conn, log

def main():
    create_sql = """
    CREATE TABLE IF NOT EXISTS standings_silver (
        team_id     INTEGER,
        team_name   TEXT,
        division    TEXT,
        w           INTEGER,
        l           INTEGER,
        gb          TEXT,
        wc_gb       TEXT,
        PRIMARY KEY (team_id)
    )
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
    log.info("✅ standings_silver table created (if not already present).")

if __name__ == "__main__":
    main()
