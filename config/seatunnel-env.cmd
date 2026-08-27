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

REM Home directory of spark distribution.
if "%SPARK_HOME%" == "" set "SPARK_HOME=C:\Program Files\spark"

REM Home directory of flink distribution.
if "%FLINK_HOME%" == "" set "FLINK_HOME=C:\Program Files\flink"

REM Whether to enable metalake (true/false).
if "%METALAKE_ENABLED%" == "" set "META_LAKE_ENABLED=false"

REM Type of metalake implementation. 
if "%METALAKE_TYPE%" == "" set "METALAKE_TYPE=gravitino"

REM Metalake service URL, format: http://host:port/api/metalakes/{metalake_name}/catalogs/
if "%METALAKE_URL%" == "" set "METALAKE_URL=http://localhost:8090/api/metalakes/default_metalake_name/catalogs/"