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
  set "CONF_FILE=%BASE_DIR%\config\agent.yaml"
)

if defined EDGE_AGENT_PID_FILE (
  set "PID_FILE=%EDGE_AGENT_PID_FILE%"
) else (
  set "PID_FILE=%BASE_DIR%\edge-agent.pid"
)

if defined EDGE_AGENT_ID_FILE (
  set "ID_FILE=%EDGE_AGENT_ID_FILE%"
) else (
  set "ID_FILE=%BASE_DIR%\edge-agent.id"
)

if defined EDGE_AGENT_LOG_FILE (
  set "LOG_FILE=%EDGE_AGENT_LOG_FILE%"
) else (
  set "LOG_FILE=%BASE_DIR%\edge-agent.out"
)

if defined EDGE_AGENT_LOG_CONFIG (
  set "LOG_CONFIG_FILE=%EDGE_AGENT_LOG_CONFIG%"
) else (
  set "LOG_CONFIG_FILE=%BASE_DIR%\config\log4j2.properties"
)

if defined EDGE_AGENT_LOG_DIR (
  set "APP_LOG_DIR=%EDGE_AGENT_LOG_DIR%"
) else (
  set "APP_LOG_DIR=%BASE_DIR%\log"
)

if defined EDGE_AGENT_APP_LOG_NAME (
  set "APP_LOG_NAME=%EDGE_AGENT_APP_LOG_NAME%"
) else (
  set "APP_LOG_NAME=edge-agent.log"
)
set "APP_LOG_FILE=%APP_LOG_DIR%\%APP_LOG_NAME%"

if defined EDGE_AGENT_STARTUP_READY_TIMEOUT_S (
  set "STARTUP_READY_TIMEOUT_S=%EDGE_AGENT_STARTUP_READY_TIMEOUT_S%"
) else (
  set "STARTUP_READY_TIMEOUT_S=10"
)

set "MAIN_CLASS=org.apache.seatunnel.edge.agent.starter.EdgeAgentStarter"
set "APP_JAR=%BASE_DIR%\starter\seatunnel-edge-agent-starter.jar"

if "%~1"=="" goto usage
if /I "%~1"=="start" goto start
if /I "%~1"=="stop" goto stop
if /I "%~1"=="status" goto status
if /I "%~1"=="db" goto db
if /I "%~1"=="help" goto help
if /I "%~1"=="-h" goto help
if /I "%~1"=="--help" goto help
goto usage

REM ---------- helpers ----------

:fail
if defined LOG_FILE (
  >>"%LOG_FILE%" echo edge-agent: error: %~1
)
echo edge-agent: error: %~1 >&2
exit /b 1

:require_java
where java >nul 2>&1
if errorlevel 1 call :fail "'java' not found in PATH; install a JRE/JDK and retry." & exit /b 1
goto :eof

:require_conf
if not exist "%CONF_FILE%" call :fail "config file not found: %CONF_FILE% (set EDGE_AGENT_CONFIG or create the file)." & exit /b 1
goto :eof

:buildcp
set "CP=%APP_JAR%"
if exist "%BASE_DIR%\config" set "CP=%BASE_DIR%\config;!CP!"
if not exist "%APP_JAR%" call :fail "main jar not found: %APP_JAR%" & exit /b 1
for %%f in ("%BASE_DIR%\starter\logging\*.jar") do (
  if exist "%%~f" set "CP=!CP!;%%~f"
)
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
for /f "tokens=2 delims==" %%a in ('wmic process where "CommandLine like '%%EdgeAgentStarter%%' and Name='java.exe'" get ProcessId /format:list 2^>nul ^| findstr /R /C:"^ProcessId="') do (
  set "SYNC_PID=%%a"
)
for /f "tokens=* delims= " %%s in ("!SYNC_PID!") do set "SYNC_PID=%%s"
if not "!SYNC_PID!"=="" (
  >"%PID_FILE%" echo(!SYNC_PID!
)
goto :eof

REM Background launcher: rely on native cmd plus optional WMIC for PID discovery (no PowerShell required).
:launch_java_bg
start "" /B cmd /c "java -Dedge.agent.home=\"%BASE_DIR%\" -Dedge.agent.log.dir=\"%APP_LOG_DIR%\" -Dedge.agent.log.name=\"%APP_LOG_NAME%\" -Dlog4j2.configurationFile=\"%LOG_CONFIG_FILE%\" -cp \"%CP%\" %MAIN_CLASS% --config \"%CONF_FILE%\" >> \"%APP_LOG_FILE%\" 2>&1"
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

call :buildcp
if exist "%PID_FILE%" del /q "%PID_FILE%" >nul 2>&1
for %%I in ("%LOG_FILE%") do if not exist "%%~dpI" mkdir "%%~dpI" >nul 2>&1
if not exist "%APP_LOG_DIR%" mkdir "%APP_LOG_DIR%" >nul 2>&1
>>"%LOG_FILE%" echo edge-agent: start requested

call :launch_java_bg

if not exist "%PID_FILE%" (
  echo edge-agent: warning: JVM may be running but PID file was not written ^(WMIC missing or query failed^); use Task Manager or log: %LOG_FILE% >&2
  exit /b 0
)

ping -n 2 127.0.0.1 >nul
call :is_running
if errorlevel 1 (
  if exist "%PID_FILE%" del /q "%PID_FILE%" >nul 2>&1
  >>"%LOG_FILE%" echo edge-agent: startup failed; process exited immediately.
  if exist "%APP_LOG_FILE%" (
    >>"%LOG_FILE%" echo edge-agent: startup error details from %APP_LOG_FILE%:
    type "%APP_LOG_FILE%" >> "%LOG_FILE%"
  )
  call :fail "process exited immediately after start; see %LOG_FILE% and %APP_LOG_FILE% for details."
  exit /b 1
)

call :read_pid
echo edge-agent started ^(pid !READ_PID!^).
set /a WAIT_MARKER=0
:wait_ready_marker
if !WAIT_MARKER! GEQ %STARTUP_READY_TIMEOUT_S% goto marker_timeout
if exist "!APP_LOG_FILE!" (
  findstr /C:"BOOTSTRAP_READY" "!APP_LOG_FILE!" >nul 2>&1
  if not errorlevel 1 goto marker_found
)
ping -n 2 127.0.0.1 >nul
set /a WAIT_MARKER+=1
goto wait_ready_marker

:marker_timeout
echo edge-agent: warning: BOOTSTRAP_READY not found within %STARTUP_READY_TIMEOUT_S%s; check !APP_LOG_FILE! and %LOG_FILE%. >&2
>>"%LOG_FILE%" echo edge-agent: warning: BOOTSTRAP_READY not found within %STARTUP_READY_TIMEOUT_S%s.
goto marker_done

:marker_found
echo edge-agent startup marker detected: BOOTSTRAP_READY

:marker_done
echo   log:     %LOG_FILE%
echo   app-log: !APP_LOG_FILE!
echo   pid:     %PID_FILE%
>>"%LOG_FILE%" echo edge-agent: started pid=!READ_PID! app-log=!APP_LOG_FILE!
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

:db
shift
call :require_java
call :buildcp
java -Dedge.agent.home="%BASE_DIR%" -Dedge.agent.log.dir="%APP_LOG_DIR%" -Dedge.agent.log.name="%APP_LOG_NAME%" -Dlog4j2.configurationFile="%LOG_CONFIG_FILE%" -cp "!CP!" %MAIN_CLASS% db %*
exit /b !ERRORLEVEL!

:status
call :cleanup_stale_pid
call :is_running
if not errorlevel 1 (
  call :read_pid
  echo edge-agent running ^(pid !READ_PID!^).
  echo   log:     %LOG_FILE%
  echo   app-log: %APP_LOG_DIR%\%APP_LOG_NAME%
  echo   pid:     %PID_FILE%
) else (
  echo edge-agent not running.
  if exist "%PID_FILE%" echo   note: pid file exists but process is dead: %PID_FILE%
)
exit /b 0

:help
echo Usage: seatunnel-edge-agent.cmd ^<start^|stop^|status^|db^|help^>
echo.
echo   db   SQLite WAL / source-position ops (see: seatunnel-edge-agent.cmd db help)
echo.
echo Direct JVM run (without this script^): java ... EdgeAgentStarter --help
echo.
echo Environment overrides (optional^):
echo   EDGE_AGENT_CONFIG       Path to agent YAML (default: %BASE_DIR%\config\agent.yaml)
echo   EDGE_AGENT_SQLITE_PATH  Override SQLite database file for db commands
echo   EDGE_AGENT_PID_FILE     PID file path (default: %BASE_DIR%\edge-agent.pid)
echo   EDGE_AGENT_ID_FILE      Identity file path (default: %BASE_DIR%\edge-agent.id)
echo   EDGE_AGENT_LOG_FILE     Startup log (default: %BASE_DIR%\edge-agent.out)
echo   EDGE_AGENT_LOG_CONFIG   log4j2 config path (default: %BASE_DIR%\config\log4j2.properties)
echo   EDGE_AGENT_LOG_DIR      Application log directory (default: %BASE_DIR%\log)
echo   EDGE_AGENT_APP_LOG_NAME Application log filename (default: edge-agent.log)
echo   EDGE_AGENT_STARTUP_READY_TIMEOUT_S  Wait seconds for BOOTSTRAP_READY marker (default: 10)
echo.
echo Expected layout under %BASE_DIR%:
echo   starter\seatunnel-edge-agent-starter.jar  main runnable jar
echo   starter\logging\  logging jars
echo   config\ configuration directory on classpath
echo   log\    runtime log directory
exit /b 0

:usage
echo Usage: seatunnel-edge-agent.cmd ^<start^|stop^|status^|db^|help^>
exit /b 1
