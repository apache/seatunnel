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

setlocal enabledelayedexpansion

set "PRG=%~f0"
set "PRG_DIR=%~dp0"
cd /d "%PRG_DIR%" || (
  echo Cannot determine the script's current directory.
  exit /b 1
)

for %%D in ("%PRG_DIR%..") do set "APP_DIR=%%~fD"
set "CONF_DIR=%APP_DIR%\config"
set "APP_JAR_NAME=seatunnel-spark-3.5-starter.jar"
set "APP_JAR=%APP_DIR%\starter\%APP_JAR_NAME%"
set "APP_MAIN=org.apache.seatunnel.core.starter.spark.SparkStarter"

if exist "%CONF_DIR%\seatunnel-env.cmd" (
  call "%CONF_DIR%\seatunnel-env.cmd"
)

if "%~1"=="" (
  set "args=-h"
) else (
  set "args=%*"
)

set "JAVA_OPTS=-Dseatunnel.spark.starter.jar.name=%APP_JAR_NAME%"
if exist "%CONF_DIR%\log4j2.properties" (
  set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.configurationFile=%CONF_DIR%\log4j2.properties"
  set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.path=%APP_DIR%\logs"
  set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-spark-3.5-starter"
)

set "CLASS_PATH=%APP_DIR%\starter\logging\*;%APP_JAR%"

for /f "delims=" %%i in ('java %JAVA_OPTS% -cp %CLASS_PATH% %APP_MAIN% %args%') do (
  set "CMD=%%i"
  setlocal disabledelayedexpansion
  if !errorlevel! equ 234 (
    echo !CMD!
    endlocal
    exit /b 0
  ) else if !errorlevel! equ 0 (
    echo Execute SeaTunnel Spark Job: !CMD!
    endlocal
    call !CMD!
  ) else (
    echo !CMD!
    endlocal
    exit /b !errorlevel!
  )
)
