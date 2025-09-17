# common.py
from pathlib import Path
import logging
from datetime import datetime, timezone
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "mlb_data.db"

# basic logger (file + console)
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("mlb-pipeline")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_conn() -> sqlite3.Connection:
    """Return a sqlite3 connection to mlb_data.db"""
    return sqlite3.connect(DB_PATH.as_posix())

# -------------------------
# Utility functions
# -------------------------

def list_tables() -> None:
    """
    Quick helper to print all tables currently in the SQLite database.
    Useful for debugging after refresh jobs.
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
    print("📊 Tables in database:", tables)

if __name__ == "__main__":
    log.info("✅ common.py is working")
    list_tables()
