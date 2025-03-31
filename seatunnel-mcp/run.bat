# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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