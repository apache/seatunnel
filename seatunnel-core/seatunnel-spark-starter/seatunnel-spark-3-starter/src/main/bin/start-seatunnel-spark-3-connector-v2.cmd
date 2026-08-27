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

rem resolve links - %0 may be a softlink
set "PRG=%~f0"
:resolve_loop
rem Get the parent directory of the script
set "PRG_DIR=%~dp0"
rem Change current drive and directory to %PRG_DIR% and execute the 'dir' command, which will fail if %PRG% is not a valid file.
cd /d "%PRG_DIR%" || (
  echo Cannot determine the script's current directory.
  exit /b 1
)

set "APP_DIR=%~dp0"
set "CONF_DIR=%APP_DIR%\config"
set "APP_JAR=%APP_DIR%\starter\seatunnel-spark-3-starter.jar"
set "APP_MAIN=org.apache.seatunnel.core.starter.spark.SparkStarter"

if exist "%CONF_DIR%\seatunnel-env.cmd" (
  call "%CONF_DIR%\seatunnel-env.cmd"
)

if "%~1"=="" (
  set "args=-h"
) else (
  set "args=%*"
)

set "JAVA_OPTS="
rem Log4j2 Config
if exist "%CONF_DIR%\log4j2.properties" (
  set "JAVA_OPTS=!JAVA_OPTS! -Dlog4j2.configurationFile=%CONF_DIR%\log4j2.properties"
  set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.path=%APP_DIR%\logs"
  set "JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.logs.file_name=seatunnel-spark-starter"
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
