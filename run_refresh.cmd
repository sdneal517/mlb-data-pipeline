@echo off
REM Navigate to project root
cd /d C:\Users\sdnea\Dev\mlb-data-pipeline

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the pipeline
python src\refresh_all.py

REM Deactivate virtual environment
deactivate
