@echo off
cd /d C:\Users\sdnea\Dev\mlb-data-pipeline

echo === Starting run_refresh.cmd === >> logs\cmd_trace.log
echo %DATE% %TIME% >> logs\cmd_trace.log

C:\Users\sdnea\Dev\mlb-data-pipeline\venv\Scripts\python.exe src\refresh_all.py >> logs\cmd_trace.log 2>&1

echo === Finished run_refresh.cmd === >> logs\cmd_trace.log
