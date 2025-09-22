@echo off
echo === Starting run_refresh.cmd === >> src\logs\cmd_trace.log
echo %DATE% %TIME% >> src\logs\cmd_trace.log

REM Navigate to project root
cd /d C:\Users\sdnea\Dev\mlb-data-pipeline
echo Changed directory to %CD% >> src\logs\cmd_trace.log

REM Run the pipeline with venv python explicitly
C:\Users\sdnea\Dev\mlb-data-pipeline\venv\Scripts\python.exe src\refresh_all.py >> src\logs\cmd_trace.log 2>&1

echo === Finished run_refresh.cmd === >> src\logs\cmd_trace.log
