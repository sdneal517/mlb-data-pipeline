# check_logging.py
from common import PROJECT_ROOT, LOGS_DIR, log

print("=== Logging Diagnostic ===")
print(f"PROJECT_ROOT: {PROJECT_ROOT}")
print(f"LOGS_DIR: {LOGS_DIR}")
print(f"Expected log file: {LOGS_DIR / 'pipeline.log'}")

log.info("Test log entry from check_logging.py")
print("Wrote a test log entry — check your logs folder(s).")
