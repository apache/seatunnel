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

setlocal disabledelayedexpansion

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

set "JAVA_OPTS=%JAVA_OPTS% -Dseatunnel.spark.starter.jar.name=%APP_JAR_NAME%"
if exist "%CONF_DIR%\log4j2.properties" (
  set JAVA_OPTS=%JAVA_OPTS% "-Dlog4j2.configurationFile=%CONF_DIR%\log4j2.properties" "-Dseatunnel.logs.path=%APP_DIR%\logs" -Dseatunnel.logs.file_name=seatunnel-spark-3.5-starter
)

set "CLASS_PATH=%APP_DIR%\starter\logging\*;%APP_JAR%"

set "OUTPUT_DIR=%TEMP%\seatunnel-spark-%RANDOM%-%RANDOM%"
mkdir "%OUTPUT_DIR%" || exit /b 1
rem Run java directly: FOR /F does not preserve the child process exit status.
java %JAVA_OPTS% -cp "%CLASS_PATH%" %APP_MAIN% %args% > "%OUTPUT_DIR%\command.txt"
set "EXIT_CODE=%errorlevel%"
if %EXIT_CODE% equ 234 (
  type "%OUTPUT_DIR%\command.txt"
  rmdir /s /q "%OUTPUT_DIR%"
  exit /b 0
)
if %EXIT_CODE% neq 0 (
  type "%OUTPUT_DIR%\command.txt"
  rmdir /s /q "%OUTPUT_DIR%"
  exit /b %EXIT_CODE%
)

rem The last output line contains the command. Replace its POSIX executable token.
set "CMD="
for /f "usebackq tokens=1,*" %%i in ("%OUTPUT_DIR%\command.txt") do set "CMD=%%j"
rmdir /s /q "%OUTPUT_DIR%"
if not defined CMD (
  echo Spark starter produced no arguments. 1>&2
  exit /b 1
)
if not defined SPARK_HOME (
  echo SPARK_HOME must point to a Spark installation. 1>&2
  exit /b 1
)
call "%SPARK_HOME%\bin\spark-submit.cmd" %CMD%
exit /b %errorlevel%
