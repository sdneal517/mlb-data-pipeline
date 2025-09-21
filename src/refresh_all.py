# refresh_all.py

import subprocess
import sys

# List of scripts to run in order (now with src/ prefix)
scripts = [
    "src/refresh_teams_silver.py",
    "src/refresh_standings_silver.py",
    "src/refresh_schedule_silver.py",
    "src/refresh_watchability_silver.py",   # builds raw scoring table
    "src/build_watchability_gold.py",       # trims silver into email-ready gold
    "src/send_digest.py"       
]

def run_script(script):
    print(f"\nRunning {script} ...")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"FAILED: {script}")
        print(result.stderr)
        sys.exit(1)

def main():
    for script in scripts:
        run_script(script)
    print("\nAll refresh tasks completed successfully!")

if __name__ == "__main__":
    main()
