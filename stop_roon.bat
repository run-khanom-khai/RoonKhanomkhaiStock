@echo off
REM ============================================================
REM  stop_roon.bat  -  Stop ALL ROON apps (ports 8501-8507)
REM ============================================================
title ROON Stop
echo Stopping ROON apps on ports 8501-8507 ...

for %%P in (8501 8502 8503 8504 8505 8506 8507) do (
    for /f "tokens=5" %%A in ('netstat -ano ^| findstr :%%P ^| findstr LISTENING') do (
        echo   Closing port %%P  (PID %%A)
        taskkill /F /PID %%A >nul 2>&1
    )
)

echo.
echo Done. All ROON apps have been stopped.
timeout /t 3 /nobreak >nul
