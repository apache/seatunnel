@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

REM resolve links - %0 may be a softlink
for %%F in ("%~f0") do (
    set "PRG=%%~fF"
    set "PRG_DIR=%%~dpF"
    set "APP_DIR=%%~dpF.."
)

set "APP_JAR=%APP_DIR%\starter\seatunnel-starter.jar"
set "LOAD_CLASS=org.apache.seatunnel.core.starter.seatunnel.SeaTunnelConnector"

if "%~1" == "" (
    set "args=-h"
) else (
    set "args=%*"
)

set "CLASS_PATH=%APP_DIR%\connectors\*;%APP_JAR%;%APP_DIR%\lib\seatunnel-transforms-v2.jar"

java -cp "%CLASS_PATH%" %LOAD_CLASS% %args% | findstr /v /c:"org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery"
