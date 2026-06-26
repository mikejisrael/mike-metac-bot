@echo off
:: ─── Open CMD and start VENV312 ───────────────────────────────────────────────────

:: Activates venv312

set PROJECT_DIR=C:\Users\mikej\metac-bot-template
set VENV_ACTIVATE=%PROJECT_DIR%\venv312\Scripts\activate.bat

cd /d "%PROJECT_DIR%"
call "%VENV_ACTIVATE%"
