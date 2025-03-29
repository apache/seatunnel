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

REM This script is used to download the connector plug-ins required during the running process.
REM All are downloaded by default. You can also choose what you need.
REM You only need to configure the plug-in name in config\plugin_config.txt.

REM Get seatunnel home
set "SEATUNNEL_HOME=%~dp0..\"
echo Set SEATUNNEL_HOME to [%SEATUNNEL_HOME%]

REM Connector default version is 2.3.11, you can also choose a custom version. eg: 2.3.11:  install-plugin.bat 2.3.11
set "version=2.3.11"
if not "%~1"=="" set "version=%~1"

REM Create the lib directory
if not exist "%SEATUNNEL_HOME%\lib" (
    mkdir "%SEATUNNEL_HOME%\lib"
    echo create lib directory
)

echo Install SeaTunnel connectors plugins, usage version is %version%

REM Create the connectors directory
if not exist "%SEATUNNEL_HOME%\connectors" (
    mkdir "%SEATUNNEL_HOME%\connectors"
    echo create connectors directory
)

for /f "usebackq delims=" %%a in ("%SEATUNNEL_HOME%\config\plugin_config") do (
    set "line=%%a"
    setlocal enabledelayedexpansion
    if "!line:~0,1!" neq "-" if "!line:~0,1!" neq "#" (
        echo install connector : !line!
        call "%SEATUNNEL_HOME%\mvnw.cmd" dependency:get -Dtransitive=false -DgroupId="org.apache.seatunnel" -DartifactId="!line!" -Dversion="%version%" -Ddest="%SEATUNNEL_HOME%\connectors"
    )
    endlocal
)
