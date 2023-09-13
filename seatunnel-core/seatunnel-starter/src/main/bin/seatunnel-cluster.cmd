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

set "HELP=false"
set "args="

for %%I in (%*) do (
    set "args=!args! %%I"
    if "%%I"=="-d" set "DAEMON=true"
    if "%%I"=="--daemon" set "DAEMON=true"
    if "%%I"=="-h" set "HELP=true"
    if "%%I"=="--help" set "HELP=true"
)

REM SeaTunnel Engine Config
set "HAZELCAST_CONFIG=%CONF_DIR%\hazelcast.yaml"
set "SEATUNNEL_CONFIG=%CONF_DIR%\seatunnel.yaml"
set "JAVA_OPTS=%JvmOption%"

for %%I in (%*) do (
    set "arg=%%I"
    if "!arg:~0,10!"=="JvmOption=" (
        set "JAVA_OPTS=%JAVA_OPTS% !arg:~10!"
    )
)

set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.config=%SEATUNNEL_CONFIG%"
set "JAVA_OPTS=%JAVA_OPTS% -Dhazelcast.config=%HAZELCAST_CONFIG%"
set "JAVA_OPTS=%JAVA_OPTS% -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"

REM Server Debug Config
REM Usage instructions:
REM If you need to debug your code in cluster mode, please enable this configuration option and listen to the specified
REM port in your IDE. After that, you can happily debug your code.
REM set "JAVA_OPTS=%JAVA_OPTS% -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5001,suspend=y"

if exist "%CONF_DIR%\log4j2.properties" (
    set "JAVA_OPTS=%JAVA_OPTS% -Dlog4j2.configurationFile=%CONF_DIR%\log4j2.properties"
    set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.logs.path=%APP_DIR%\logs"
    set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.logs.file_name=seatunnel-engine-server"
)

set "CLASS_PATH=%APP_DIR%\lib\*;%APP_JAR%"

for /f "usebackq delims=" %%I in ("%APP_DIR%\config\jvm_options") do (
    set "line=%%I"
    if not "!line:~0,1!"=="#" if "!line!" NEQ "" (
        set "JAVA_OPTS=!JAVA_OPTS! !line!"
    )
)

if "%HELP%"=="false" (
    if not exist "%APP_DIR%\logs\" mkdir "%APP_DIR%\logs"
    start "SeaTunnel Server" java %JAVA_OPTS% -cp "%CLASS_PATH%" %APP_MAIN% %args% > "%OUT%" 2>&1
) else (
    java %JAVA_OPTS% -cp "%CLASS_PATH%" %APP_MAIN% %args%
)

endlocal