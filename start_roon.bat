@echo off
REM ============================================================
REM  start_roon.bat  -  Open ALL ROON apps on this PC (local)
REM  Runs each Streamlit app on its own port + own window
REM  Made for Dr.Wan
REM ============================================================
title ROON Launcher
cd /d "D:\ROON_Management_System"

set VENV=.venv\Scripts\streamlit.exe

if not exist "%VENV%" (
    echo [ERROR] Cannot find %VENV%
    echo Please make sure the virtual environment .venv exists.
    pause
    exit /b 1
)

echo ============================================================
echo   Starting ROON system - opening all apps...
echo   Please wait, browser tabs will open one by one.
echo ============================================================
echo.

REM --- 1) Boss / Executive (app.py) : port 8501 ---
echo Opening: Boss / Executive           http://localhost:8501
start "ROON Boss"       "%VENV%" run app.py             --server.port 8501
timeout /t 4 /nobreak >nul

REM --- 2) Backoffice / Accounting (accounting_app.py) : 8502 ---
echo Opening: Backoffice / Accounting    http://localhost:8502
start "ROON Backoffice" "%VENV%" run accounting_app.py  --server.port 8502
timeout /t 4 /nobreak >nul

REM --- 3) Branch (branch_app.py) : 8503 ---
echo Opening: Branch                     http://localhost:8503
start "ROON Branch"     "%VENV%" run branch_app.py      --server.port 8503
timeout /t 4 /nobreak >nul

REM --- 4) Audit (audit_app.py) : 8504 ---
echo Opening: Audit                      http://localhost:8504
start "ROON Audit"      "%VENV%" run audit_app.py       --server.port 8504
timeout /t 4 /nobreak >nul

REM --- 5) Production (production_app.py) : 8505 ---
echo Opening: Production                 http://localhost:8505
start "ROON Production" "%VENV%" run production_app.py  --server.port 8505
timeout /t 4 /nobreak >nul

REM --- 6) Purchase (purchase_app.py) : 8506 ---
echo Opening: Purchase                   http://localhost:8506
start "ROON Purchase"   "%VENV%" run purchase_app.py    --server.port 8506
timeout /t 4 /nobreak >nul

REM --- 7) Sale Audit (sale_audit_app.py) : 8507 ---
echo Opening: Sale Audit                 http://localhost:8507
start "ROON SaleAudit"  "%VENV%" run sale_audit_app.py  --server.port 8507
timeout /t 2 /nobreak >nul

echo.
echo ============================================================
echo   All apps started. 7 browser tabs should be open.
echo.
echo   Boss/Executive     http://localhost:8501
echo   Backoffice/Account http://localhost:8502
echo   Branch             http://localhost:8503
echo   Audit              http://localhost:8504
echo   Production         http://localhost:8505
echo   Purchase           http://localhost:8506
echo   Sale Audit         http://localhost:8507
echo.
echo   To STOP the system: close each app window,
echo   or run stop_roon.bat
echo ============================================================
echo.
echo This window can be closed. The apps keep running in their own windows.
pause
