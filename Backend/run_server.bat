@echo off
REM Setup and run the Movie Collection backend server
cd /d "%~dp0"

echo ========================================
echo Movie Collection Backend Server
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH.
    echo Please install Python 3.8+ from https://www.python.org/
    pause
    exit /b 1
)

echo Detected Python version:
python --version
echo.

REM Install/upgrade dependencies
echo Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo Starting backend server on http://127.0.0.1:5000
echo.
echo You can now open the UI in your browser at:
echo   file:///c/Users/prcha/Downloads/work/coding/Movieproject/UI/UI.html
echo.
echo Press Ctrl+C to stop the server.
echo.

python server.py
