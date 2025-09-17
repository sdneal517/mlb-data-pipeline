# refresh_all.py

import subprocess
import sys

# List of scripts to run in order
scripts = [
    "refresh_teams_silver.py",
    "refresh_standings_silver.py",
    "refresh_schedule_silver.py",
    "refresh_watchability_silver.py",   # new: builds raw scoring table
    "build_watchability_gold.py"        # trims silver into email-ready gold
]

def run_script(script):
    print(f"\n▶ Running {script} ...")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"❌ {script} failed")
        print(result.stderr)
        sys.exit(1)

def main():
    for script in scripts:
        run_script(script)
    print("\n✅ All refresh tasks completed successfully!")

if __name__ == "__main__":
    main()
