@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

setlocal enabledelayedexpansion
REM resolve links - %0 may be a softlink
set "PRG=%~0"

:resolveLoop
for %%F in ("%PRG%") do (
    set "PRG_DIR=%%~dpF"
    set "PRG_NAME=%%~nxF"
)
set "PRG=%PRG_DIR%%PRG_NAME%"

REM Get application directory
cd "%PRG_DIR%\.."
set "APP_DIR=%CD%"

set "CONF_DIR=%APP_DIR%\config"
set "APP_JAR=%APP_DIR%\starter\seatunnel-starter.jar"
set "APP_MAIN=org.apache.seatunnel.core.starter.seatunnel.SeaTunnelClient"

if exist "%CONF_DIR%\seatunnel-env.cmd" call "%CONF_DIR%\seatunnel-env.cmd"

if "%~1"=="" (
    set "args=-h"
) else (
    set "args=%*"
)

REM SeaTunnel Engine Config
if not defined HAZELCAST_CLIENT_CONFIG (
    set "HAZELCAST_CLIENT_CONFIG=%CONF_DIR%\hazelcast-client.yaml"
)

if not defined HAZELCAST_CONFIG (
    set "HAZELCAST_CONFIG=%CONF_DIR%\hazelcast.yaml"
)

if not defined SEATUNNEL_CONFIG (
    set "SEATUNNEL_CONFIG=%CONF_DIR%\seatunnel.yaml"
)

if defined JvmOption (
    set "JAVA_OPTS=!JAVA_OPTS! %JvmOption%"
)

set "JAVA_OPTS=!JAVA_OPTS! -Dhazelcast.client.config=%HAZELCAST_CLIENT_CONFIG%"
set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.config=%SEATUNNEL_CONFIG%"
set "JAVA_OPTS=!JAVA_OPTS! -Dhazelcast.config=%HAZELCAST_CONFIG%"

REM if you want to debug, please
REM set "JAVA_OPTS=!JAVA_OPTS! -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=5000,suspend=n"

REM Log4j2 Config
set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.isThreadContextMapInheritable=true"
if exist "%CONF_DIR%\log4j2_client.properties" (
    set "JAVA_OPTS=!JAVA_OPTS! -Dhazelcast.logging.type=log4j2"
    set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.configurationFile=%CONF_DIR%\log4j2_client.properties"
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.path=%APP_DIR%\logs"
    for %%i in (%args%) do (
        set "arg=%%i"
        if "!arg!"=="-m" set "is_local_mode=true"
        if "!arg!"=="--master" set "is_local_mode=true"
        if "!arg!"=="-e" set "is_local_mode=true"
        if "!arg!"=="--deploy-mode" set "is_local_mode=true"
    )
    if defined is_local_mode (
        for /f "tokens=1-3 delims=:" %%A in ('echo %time%') do (
            set "ntime=%%A%%B%%C"
        )
        for /f "tokens=2 delims==" %%A in ('wmic os get localdatetime /value') do (
            set datetime=%%A
            set ndate=!datetime:~0,4!!datetime:~4,2!!datetime:~6,2!
        )
        set "timestamp=!time:~0,2!!time:~3,2!!time:~6,2!!ntime:~0,6!"
        set "timestamp=!timestamp: =0!"
        set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-starter-client-!ndate!-!timestamp!"
    ) else (
        set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-starter-client"
    )
)

set "CLASS_PATH=%APP_DIR%\lib\*;%APP_JAR%"

for /f "usebackq delims=" %%a in ("%APP_DIR%\config\jvm_client_options") do (
    set "line=%%a"
    if not "!line:~0,1!"=="#" if "!line!" neq "" (
        set "JAVA_OPTS=!JAVA_OPTS! !line!"
    )
)

REM Parse JvmOption from command line, it should be parsed after jvm_client_options
for %%i in (%*) do (
    set "arg=%%i"
    if "!arg:~0,9!"=="JvmOption" (
        set "JVM_OPTION=!arg:~9!"
        set "JAVA_OPTS=!JAVA_OPTS! !JVM_OPTION!"
        goto :break_loop
    )
)
:break_loop

REM SeaTunnel requires Java 11 or newer. Fail fast with an actionable message instead of letting a
REM JDK 8 launcher abort on the JDK 9+ module flags below with a cryptic "Unrecognized option" error.
REM This mirrors the check in seatunnel.sh and seatunnel-cluster.sh. If the version cannot be parsed
REM the check is skipped rather than blocking startup.
set "JAVA_VERSION_TEXT="
for /f "tokens=3" %%V in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    if not defined JAVA_VERSION_TEXT set "JAVA_VERSION_TEXT=%%~V"
)
set "JAVA_MAJOR_VERSION=0"
if defined JAVA_VERSION_TEXT (
    for /f "tokens=1,2 delims=._-+" %%A in ("!JAVA_VERSION_TEXT!") do (
        if "%%A"=="1" (set "JAVA_MAJOR_VERSION=%%B") else (set "JAVA_MAJOR_VERSION=%%A")
    )
)
set /a JAVA_MAJOR_NUM=0
set /a JAVA_MAJOR_NUM=!JAVA_MAJOR_VERSION! 2>nul
if !JAVA_MAJOR_NUM! GTR 0 if !JAVA_MAJOR_NUM! LSS 11 (
    echo Error: SeaTunnel requires Java 11 or newer, but Java !JAVA_MAJOR_NUM! was detected. Point JAVA_HOME/PATH at a Java 11+ JDK. 1>&2
    exit /b 1
)

REM These JDK module flags are mandatory on Java 11+: Hazelcast needs reflective access to JDK
REM internals, plugin loading needs java.net, Arrow based connectors need java.nio, and the Kerberos
REM krb5.conf reload needs the jgss export. They are appended here, not only shipped in the
REM config\jvm_*_options templates, so an in-place upgrade that keeps an old config directory
REM cannot silently drop them. A flag the effective options already carry is not added again.
for %%F in (
    "--add-opens=java.base/java.lang=ALL-UNNAMED"
    "--add-opens=java.base/java.net=ALL-UNNAMED"
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
    "--add-opens=java.base/java.util=ALL-UNNAMED"
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
) do (
    echo !JAVA_OPTS! | findstr /c:%%F >nul 2>&1
    if errorlevel 1 set "JAVA_OPTS=!JAVA_OPTS! %%~F"
)

REM Ensure HeapDumpPath directory exists to avoid OOM dump failures.
set "HEAP_DUMP_PATH="
for %%I in (!JAVA_OPTS!) do (
    set "opt=%%I"
    if "!opt:~0,18!"=="-XX:HeapDumpPath=" (
        set "HEAP_DUMP_PATH=!opt:~18!"
    )
)
if defined HEAP_DUMP_PATH (
    set "HEAP_DUMP_DIR=!HEAP_DUMP_PATH!"
    if "!HEAP_DUMP_PATH:~-1!"=="/" set "HEAP_DUMP_DIR=!HEAP_DUMP_PATH:~0,-1!"
    if "!HEAP_DUMP_PATH:~-1!"=="\\" set "HEAP_DUMP_DIR=!HEAP_DUMP_PATH:~0,-1!"
    if /I "!HEAP_DUMP_PATH:~-6!"==".hprof" (
        for %%D in ("!HEAP_DUMP_PATH!") do set "HEAP_DUMP_DIR=%%~dpD"
    ) else if /I "!HEAP_DUMP_PATH:~-4!"==".phd" (
        for %%D in ("!HEAP_DUMP_PATH!") do set "HEAP_DUMP_DIR=%%~dpD"
    ) else (
        for %%D in ("!HEAP_DUMP_PATH!") do (
            if not "%%~xD"=="" set "HEAP_DUMP_DIR=%%~dpD"
        )
    )
    if defined HEAP_DUMP_DIR if not exist "!HEAP_DUMP_DIR!" mkdir "!HEAP_DUMP_DIR!"
)

REM Ensure Xloggc directory exists to avoid GC logging failures.
set "GC_LOG_PATH="
for %%I in (!JAVA_OPTS!) do (
    set "opt=%%I"
    if "!opt:~0,8!"=="-Xloggc:" (
        set "GC_LOG_PATH=!opt:~8!"
    )
)
if defined GC_LOG_PATH (
    for %%D in ("!GC_LOG_PATH!") do set "GC_LOG_DIR=%%~dpD"
    if defined GC_LOG_DIR if not exist "!GC_LOG_DIR!" mkdir "!GC_LOG_DIR!"
)

java !JAVA_OPTS! -cp %CLASS_PATH% %APP_MAIN% %args%
