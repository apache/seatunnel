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
    set "JAVA_OPTS=%JAVA_OPTS% %JvmOption%"
)

set "JAVA_OPTS=%JAVA_OPTS% -Dhazelcast.client.config=%HAZELCAST_CLIENT_CONFIG%"
set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.config=%SEATUNNEL_CONFIG%"
set "JAVA_OPTS=%JAVA_OPTS% -Dhazelcast.config=%HAZELCAST_CONFIG%"

REM if you want to debug, please
REM set "JAVA_OPTS=%JAVA_OPTS% -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=5000,suspend=n"

REM Log4j2 Config
set "JAVA_OPTS=%JAVA_OPTS% -Dlog4j2.isThreadContextMapInheritable=true"
if exist "%CONF_DIR%\log4j2_client.properties" (
    set "JAVA_OPTS=%JAVA_OPTS% -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=%CONF_DIR%\log4j2_client.properties"
    set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.logs.path=%APP_DIR%\logs"
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
        set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.logs.file_name=seatunnel-starter-client-!date:~0,4!!date:~5,2!!date:~8,2!-!time:~0,2!!time:~3,2!!time:~6,2!!ntime!"
    ) else (
        set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.logs.file_name=seatunnel-starter-client"
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

java %JAVA_OPTS% -cp %CLASS_PATH% %APP_MAIN% %args%
