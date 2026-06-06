@echo off
title TypeCraft Keyboard Tracker
echo Checking dependencies...

pip show pynput >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing pynput...
    pip install pynput
)

pip show pymongo >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing pymongo...
    pip install pymongo
)

pip show dnspython >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing dnspython for MongoDB Atlas SRV...
    pip install dnspython
)

echo.
echo Starting global keyboard background listener (MongoDB Atlas connected)...
echo Press Ctrl+C in this terminal window to stop tracking.
echo.
python listener.py
pause
