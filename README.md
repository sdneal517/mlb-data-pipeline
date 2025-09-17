# MLB Data Pipeline

A practice data engineering project to simulate a real ETL pipeline:
- **Extract**: Pulls team, schedule, and standings data from the [MLB Stats API](https://statsapi.mlb.com/).
- **Transform**: Cleans and reshapes data into **bronze → silver → gold** tables in SQLite.
- **Load / Visualize**: Provides watchability scores for upcoming games and builds Jupyter-based visualizations, with a roadmap for automated PDF/email digests.

---

## Purpose
This project is designed to:
1. Practice **data engineering workflows** (API → database → visuals).
2. Build **muscle memory** with Git, virtual environments, and Python packaging.
3. Serve as a **portfolio piece** demonstrating ETL concepts on real-world sports data.

---

## Technologies
- **Python**: Core ETL scripting
- **SQLite**: Local database (`mlb_data.db`)
- **Jupyter Lab**: Analysis and visualization
- **Git/GitHub**: Version control & syncing
- **MLB Stats API**: Data source

---

## Data Flow
- **Bronze**: Raw API pulls  
- **Silver**: Clean, structured tables (teams, schedule, standings, watchability)  
- **Gold**: Final curated views (simplified daily watchability scores for email digest)

---

## Repo Structure
- `src/` → Core ETL pipeline scripts  
- `notebooks/production/` → Clean notebooks for demos/presentations  
- `notebooks/experiments/` → Scratch/analysis notebooks  
- `data/` → SQLite DB (`mlb_data.db`) and datasets  
- `logs/` → Runtime logs  
- `sandbox/` → Experiments / items to review later  
- `venv/` → Virtual environment (ignored in Git)  

