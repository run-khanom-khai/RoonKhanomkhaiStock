@echo off
cd /d D:\ROON_Management_System
git config --global --add safe.directory D:/ROON_Management_System
git config user.email "drwan789@gmail.com"
git config user.name "Dr Wan"
echo.
echo STEP 1 - add and commit
git add -A
git commit -m "update roon system"
echo.
echo STEP 2 - force push to GitHub
git push origin main --force
echo.
echo ================================================
echo DONE
echo success = you see  forced update  or  main - main
echo ================================================
pause
