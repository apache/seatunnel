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

set "SEATUNNEL_DEFAULT_CLUSTER_NAME=seatunnel_default_cluster"
set "SHOW_USAGE=Usage: stop-seatunnel-cluster.bat \n Options: \n -cn, --cluster The name of the cluster to shut down (default: $SEATUNNEL_DEFAULT_CLUSTER_NAME) \n -h, --help Show the usage message"
set "APP_MAIN=org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer"
set "CLUSTER_NAME="

if "%~1"=="" (
  echo !SHOW_USAGE!
  exit /B 1
)

:parse_args
if "%~1"=="-cn" (
  shift
  set "CLUSTER_NAME=%~1"
  shift
  goto :parse_args
) else if "%~1"=="--cluster" (
  shift
  set "CLUSTER_NAME=%~1"
  shift
  goto :parse_args
) else if "%~1"=="-h" (
  echo !SHOW_USAGE!
  exit /B 0
) else if "%~1"=="--help" (
  echo !SHOW_USAGE!
  exit /B 0
)

if not defined CLUSTER_NAME (
  for /f %%i in ('tasklist /fi "imagename eq java.exe" ^| find "!APP_MAIN!"') do (
    taskkill /F /PID %%i
  )
) else (
  for /f %%i in ('tasklist /fi "imagename eq java.exe" ^| find "!APP_MAIN!" ^| find "!CLUSTER_NAME!"') do (
    taskkill /F /PID %%i
  )
)

exit /B 0