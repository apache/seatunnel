@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

setlocal enabledelayedexpansion

for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"

if defined EDGE_AGENT_CONFIG (
  set "CONF_FILE=%EDGE_AGENT_CONFIG%"
) else (
  set "CONF_FILE=%BASE_DIR%\conf\agent.yaml"
)

if defined EDGE_AGENT_PID_FILE (
  set "PID_FILE=%EDGE_AGENT_PID_FILE%"
) else (
  set "PID_FILE=%BASE_DIR%\edge-agent.pid"
)

if defined EDGE_AGENT_LOG_FILE (
  set "LOG_FILE=%EDGE_AGENT_LOG_FILE%"
) else (
  set "LOG_FILE=%BASE_DIR%\edge-agent.out"
)

set "MAIN_CLASS=org.apache.seatunnel.edge.agent.EdgeAgentMain"

if "%~1"=="" goto usage
if /I "%~1"=="start" goto start
if /I "%~1"=="stop" goto stop
if /I "%~1"=="status" goto status
if /I "%~1"=="help" goto help
if /I "%~1"=="-h" goto help
if /I "%~1"=="--help" goto help
goto usage

REM ---------- helpers ----------

:fail
echo edge-agent: error: %~1 >&2
exit /b 1

:require_java
where java >nul 2>&1
if errorlevel 1 call :fail "'java' not found in PATH; install a JRE/JDK and retry." & exit /b 1
goto :eof

:require_conf
if not exist "%CONF_FILE%" call :fail "config file not found: %CONF_FILE% (set EDGE_AGENT_CONFIG or create the file)." & exit /b 1
goto :eof

:require_lib
set FOUND_LIB=0
for %%f in ("%BASE_DIR%\lib\*.jar") do (
  if exist "%%~f" set FOUND_LIB=1
)
if "!FOUND_LIB!"=="0" call :fail "no jars under %BASE_DIR%\lib; build or copy runtime jars before starting." & exit /b 1
goto :eof

:buildcp
set "CP=%BASE_DIR%\conf"
for %%f in ("%BASE_DIR%\lib\*.jar") do (
  if exist "%%~f" set "CP=!CP!;%%~f"
)
goto :eof

:read_pid
set READ_PID=
if not exist "%PID_FILE%" goto :eof
set /p READ_PID=<"%PID_FILE%"
set READ_PID=!READ_PID: =!
echo !READ_PID!| findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 set READ_PID=
goto :eof

:is_running
call :read_pid
if "!READ_PID!"=="" exit /b 1
tasklist /FI "PID eq !READ_PID!" /NH 2>nul | findstr /I "java.exe" >nul
if not errorlevel 1 exit /b 0
exit /b 1

:cleanup_stale_pid
if not exist "%PID_FILE%" goto :eof
call :read_pid
if "!READ_PID!"=="" (
  del /q "%PID_FILE%" >nul 2>&1
  goto :eof
)
call :is_running
if errorlevel 1 del /q "%PID_FILE%" >nul 2>&1
goto :eof

:sync_pid_from_wmic
set SYNC_PID=
where wmic >nul 2>&1
if errorlevel 1 goto :eof
for /f "tokens=2 delims==" %%a in ('wmic process where "CommandLine like '%%EdgeAgentMain%%' and Name='java.exe'" get ProcessId /format:list 2^>nul ^| findstr /R /C:"^ProcessId="') do (
  set "SYNC_PID=%%a"
)
for /f "tokens=* delims= " %%s in ("!SYNC_PID!") do set "SYNC_PID=%%s"
if not "!SYNC_PID!"=="" (
  >"%PID_FILE%" echo(!SYNC_PID!
)
goto :eof

REM Background launcher: rely on native cmd plus optional WMIC for PID discovery (no PowerShell required).
:launch_java_bg
start "" /B cmd /c "java -cp \"%CP%\" %MAIN_CLASS% --config \"%CONF_FILE%\" >> \"%LOG_FILE%\" 2>&1"
ping -n 3 127.0.0.1 >nul
call :sync_pid_from_wmic
goto :eof

REM ---------- commands ----------

:start
call :cleanup_stale_pid
call :is_running
if not errorlevel 1 (
  call :read_pid
  echo edge-agent already running ^(pid !READ_PID!^, pid file %PID_FILE%^).
  exit /b 0
)

call :require_java || exit /b 1
call :require_conf || exit /b 1
call :require_lib || exit /b 1

call :buildcp
if exist "%PID_FILE%" del /q "%PID_FILE%" >nul 2>&1

call :launch_java_bg

if not exist "%PID_FILE%" (
  echo edge-agent: warning: JVM may be running but PID file was not written ^(WMIC missing or query failed^); use Task Manager or log: %LOG_FILE% >&2
  exit /b 0
)

ping -n 2 127.0.0.1 >nul
call :is_running
if errorlevel 1 (
  if exist "%PID_FILE%" del /q "%PID_FILE%" >nul 2>&1
  call :fail "process exited immediately after start; see %LOG_FILE% for details."
  exit /b 1
)

call :read_pid
echo edge-agent started ^(pid !READ_PID!^).
echo   config: %CONF_FILE%
echo   log:    %LOG_FILE%
echo   pid:    %PID_FILE%
exit /b 0

:stop
call :cleanup_stale_pid
call :is_running
if errorlevel 1 (
  echo edge-agent not running ^(no live pid in %PID_FILE%^).
  exit /b 0
)

call :read_pid
taskkill /PID !READ_PID! >nul 2>&1
if errorlevel 1 (
  echo edge-agent: warning: failed to stop pid !READ_PID!; removing stale pid file. >&2
  del /q "%PID_FILE%" >nul 2>&1
  exit /b 1
)

set WAITED=0
:stop_wait
ping -n 2 127.0.0.1 >nul
call :is_running
if errorlevel 1 goto stop_done
set /a WAITED+=1
if !WAITED! GEQ 30 goto stop_force
goto stop_wait

:stop_force
echo edge-agent: warning: pid !READ_PID! still alive after !WAITED! attempts; forcing termination. >&2
taskkill /PID !READ_PID! /F >nul 2>&1

:stop_done
del /q "%PID_FILE%" >nul 2>&1
echo edge-agent stopped.
exit /b 0

:status
call :cleanup_stale_pid
call :is_running
if not errorlevel 1 (
  call :read_pid
  echo edge-agent running ^(pid !READ_PID!^).
  echo   config: %CONF_FILE%
  echo   log:    %LOG_FILE%
  echo   pid:    %PID_FILE%
) else (
  echo edge-agent not running.
  if exist "%PID_FILE%" echo   note: pid file exists but process is dead: %PID_FILE%
)
exit /b 0

:help
echo Usage: seatunnel-edge-agent.cmd ^<start^|stop^|status^|help^>
echo.
echo Environment overrides (optional^):
echo   EDGE_AGENT_CONFIG       Path to agent YAML (default: %BASE_DIR%\conf\agent.yaml)
echo   EDGE_AGENT_PID_FILE     PID file path (default: %BASE_DIR%\edge-agent.pid)
echo   EDGE_AGENT_LOG_FILE     Stdout/stderr log (default: %BASE_DIR%\edge-agent.out)
echo.
echo Expected layout under %BASE_DIR%:
echo   conf\   configuration directory on classpath
echo   lib\    dependency jars (*.jar^)
exit /b 0

:usage
echo Usage: seatunnel-edge-agent.cmd ^<start^|stop^|status^|help^>
exit /b 1
