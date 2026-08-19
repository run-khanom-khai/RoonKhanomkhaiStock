@echo off
chcp 65001 >nul
cd /d D:\ROON_Management_System
echo === STEP 1: fix dubious ownership ===
git config --global --add safe.directory D:/ROON_Management_System
git config --global --add safe.directory "*"
git config user.email "drwan789@gmail.com"
git config user.name "Dr Wan"
echo === STEP 2: add + commit ===
git add -A
git commit -m "update roon system"
echo === STEP 3: push ===
git push origin main
echo === STEP 4: if rejected, pull then push again ===
git pull origin main --no-rebase -X ours --no-edit
git push origin main
echo.
echo ==================================================
echo   DONE. If you see  main -^> main  = success
echo ==================================================
pause
