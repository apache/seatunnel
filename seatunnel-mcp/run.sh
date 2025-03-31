#!/bin/bash

# Run the SeaTunnel MCP server
# This script sets up the environment and runs the server

# Check if virtual environment exists, if not create it
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies if needed
if ! command -v uvicorn &> /dev/null; then
    echo "Installing dependencies..."
    pip install -e .
fi

# Check if .env file exists, if not create from example
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "Creating .env file from .env.example..."
        cp .env.example .env
        echo "Please review and update the .env file with your SeaTunnel API settings."
    else
        echo "Warning: .env.example file not found. Please create a .env file manually."
    fi
fi

# Run the server
echo "Starting SeaTunnel MCP server..."
python -m src.seatunnel_mcp 