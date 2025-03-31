@echo off
REM Run the SeaTunnel MCP server
REM This script sets up the environment and runs the server

REM Check if virtual environment exists, if not create it
if not exist .venv (
    echo Creating virtual environment...
    python -m venv .venv
)

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Install dependencies if needed
pip show uvicorn >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Installing dependencies...
    pip install -e .
)

REM Check if .env file exists, if not create from example
if not exist .env (
    if exist .env.example (
        echo Creating .env file from .env.example...
        copy .env.example .env
        echo Please review and update the .env file with your SeaTunnel API settings.
    ) else (
        echo Warning: .env.example file not found. Please create a .env file manually.
    )
)

REM Run the server
echo Starting SeaTunnel MCP server...
python -m src.seatunnel_mcp

REM Deactivate virtual environment when finished
call deactivate 