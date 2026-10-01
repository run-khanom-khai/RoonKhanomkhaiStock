@echo off
cd /d D:\ROON_Management_System
if exist .git\index.lock del /f /q .git\index.lock
git config --global --add safe.directory D:/ROON_Management_System
git config user.email "drwan789@gmail.com"
git config user.name "Dr Wan"
echo.
echo STEP 1 - sync with GitHub
git fetch origin
echo.
echo STEP 2 - rebuild commit WITHOUT the secret file
git reset --soft origin/main
git rm --cached --ignore-unmatch .streamlit/secrets.toml
git add -A
git commit -m "fix branch sales duplicate id (DV) + stop tracking secrets"
echo.
echo STEP 3 - push to GitHub (normal push)
git push origin main
echo.
echo ================================================
echo DONE
echo success = you see  main -^> main  and NO red error
echo if you still see red  tell Dr Wan assistant and send a photo
echo ================================================
pause
