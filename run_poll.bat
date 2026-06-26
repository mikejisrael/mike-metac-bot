@echo off
REM run_poll.bat — launcher for Task Scheduler.
REM Sets the working directory and calls the venv's python directly
REM (no need to "activate" — calling venv312\Scripts\python.exe is enough).

cd /d "C:\Users\mikej\metac-bot-template"
venv312\Scripts\python.exe poll_tournament.py
