#!/usr/bin/env bash

# Setup and run the Movie Collection backend server
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Movie Collection Backend Server"
echo "========================================"
echo

# Check if Python is installed
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "ERROR: Python is not installed or not in PATH."
    echo "Please install Python 3.8+ from https://www.python.org/"
    exit 1
fi

echo "Detected Python version:"
"$PYTHON_CMD" --version
echo

# Install/upgrade dependencies
echo "Installing dependencies..."
"$PYTHON_CMD" -m pip install -r requirements.txt

echo
echo "Starting backend server on http://127.0.0.1:5000"
echo
echo "You can now open the UI in your browser at:"
echo "  file://$ROOT_DIR/UI/UI.html"
echo
echo "Press Ctrl+C to stop the server."
echo

"$PYTHON_CMD" server.py
