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
for %%F in ("%~f0") do (
    set "PRG=%%~fF"
    set "PRG_DIR=%%~dpF"
    set "APP_DIR=%%~dpF.."
)

set "CONF_DIR=%APP_DIR%\config"
set "APP_JAR=%APP_DIR%\starter\seatunnel-starter.jar"
set "APP_MAIN=org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer"
set "OUT=%APP_DIR%\logs\seatunnel-server.out"
set "MASTER_OUT=%APP_DIR%\logs\seatunnel-engine-master.out"
set "WORKER_OUT=%APP_DIR%\logs\seatunnel-engine-worker.out"
set "NODE_ROLE=master_and_worker"

set "HELP=false"
set "args="

for %%I in (%*) do (
    set "args=!args! %%I"
    if "%%I"=="-d" set "DAEMON=true"
    if "%%I"=="--daemon" set "DAEMON=true"
    if "%%I"=="-h" set "HELP=true"
    if "%%I"=="--help" set "HELP=true"
    if "%%I"=="-r" set "NODE_ROLE=%%~nI"
    if "%%I"=="--role" set "NODE_ROLE=%%~nI"
)

set "JAVA_OPTS=%JvmOption%"
set "SEATUNNEL_CONFIG=%CONF_DIR%\seatunnel.yaml"

rem Anchor cluster-side connector discovery to the distribution when no explicit home is provided.
if not defined SEATUNNEL_HOME (
    set "SEATUNNEL_HOME=%APP_DIR%"
)

rem Publish the effective SeaTunnel home for both Java-property and environment-style lookups.
set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.home=!SEATUNNEL_HOME! -DSEATUNNEL_HOME=!SEATUNNEL_HOME!"
set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.isThreadContextMapInheritable=true"
set "JAVA_OPTS=!JAVA_OPTS! -DAsyncLogger.ThreadNameStrategy=UNCACHED"

REM Server Debug Config
REM Usage instructions:
REM If you need to debug your code in cluster mode, please enable this configuration option and listen to the specified
REM port in your IDE. After that, you can happily debug your code.
REM set "JAVA_OPTS=!JAVA_OPTS! -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5001,suspend=n"

if exist "%CONF_DIR%\log4j2.properties" (
    set "JAVA_OPTS=!JAVA_OPTS! -Dhazelcast.logging.type=log4j2
    set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.configurationFile=%CONF_DIR%\log4j2.properties"
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.path=%APP_DIR%\logs"
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-engine-server"
)

if "%NODE_ROLE%" == "master" (
    set "OUT=%MASTER_OUT%"
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-engine-master"
    for /f "usebackq delims=" %%I in ("%APP_DIR%\config\jvm_master_options") do (
        set "line=%%I"
        if not "!line:~0,1!"=="#" if "!line!" NEQ "" (
            set "JAVA_OPTS=!JAVA_OPTS! !line!"
        )
    )
    REM SeaTunnel Engine Config
    set "HAZELCAST_CONFIG=%CONF_DIR%\hazelcast-master.yaml"

) else if "%NODE_ROLE%" == "worker" (
    set "OUT=%WORKER_OUT%"
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-engine-worker"
    for /f "usebackq delims=" %%I in ("%APP_DIR%\config\jvm_worker_options") do (
        set "line=%%I"
        if not "!line:~0,1!"=="#" if "!line!" NEQ "" (
            set "JAVA_OPTS=!JAVA_OPTS! !line!"
        )
    )
    REM SeaTunnel Engine Config
    set "HAZELCAST_CONFIG=%CONF_DIR%\hazelcast-worker.yaml"
) else if "%NODE_ROLE%" == "master_and_worker" (
    set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-engine-server"
    for /f "usebackq delims=" %%I in ("%APP_DIR%\config\jvm_options") do (
        set "line=%%I"
        if not "!line:~0,1!"=="#" if "!line!" NEQ "" (
            set "JAVA_OPTS=!JAVA_OPTS! !line!"
        )
    )
    REM SeaTunnel Engine Config
    set "HAZELCAST_CONFIG=%CONF_DIR%\hazelcast.yaml"
) else (
    echo Unknown node role: %NODE_ROLE%
    exit 1
)

REM Parse JvmOption from command line, it should be parsed after jvm_options
for %%I in (%*) do (
    set "arg=%%I"
    if "!arg:~0,10!"=="JvmOption=" (
        set "JAVA_OPTS=!JAVA_OPTS! !arg:~10!"
    )
)

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

IF NOT EXIST "%HAZELCAST_CONFIG%" (
    echo Error: File %HAZELCAST_CONFIG% does not exist.
    exit /b 1
)
set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.config=%SEATUNNEL_CONFIG%"
set "JAVA_OPTS=!JAVA_OPTS! -Dhazelcast.config=%HAZELCAST_CONFIG%"
set "CLASS_PATH=%APP_DIR%\lib\*;%APP_JAR%"

if "%HELP%"=="false" (
    if not exist "%APP_DIR%\logs\" mkdir "%APP_DIR%\logs"
    start "SeaTunnel Server" java !JAVA_OPTS! -cp "%CLASS_PATH%" %APP_MAIN% %args% > "%OUT%" 2>&1
) else (
    java !JAVA_OPTS! -cp "%CLASS_PATH%" %APP_MAIN% %args%
)

endlocal
