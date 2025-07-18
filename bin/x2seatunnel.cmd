@echo off

rem
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
rem

rem X2SeaTunnel 配置转换工具启动脚本（Windows）

setlocal enabledelayedexpansion

rem 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"
set "SEATUNNEL_HOME=%SCRIPT_DIR%\.."

rem 查找 X2SeaTunnel JAR 文件
set "CLI_JAR="
for /r "%SEATUNNEL_HOME%\seatunnel-tools\x2seatunnel\target" %%f in (x2seatunnel-*.jar) do (
    echo %%f | findstr /v "sources" >nul
    if not errorlevel 1 (
        set "CLI_JAR=%%f"
        goto :found_jar
    )
)

:found_jar
if not defined CLI_JAR (
    echo 错误: 未找到 X2SeaTunnel JAR 文件
    echo 请确保已经编译了 seatunnel-tools 模块: mvn clean package -pl seatunnel-tools -am
    exit /b 1
)

rem 检查 Java 环境
if defined JAVA_HOME (
    set "JAVA_CMD=%JAVA_HOME%\bin\java.exe"
) else (
    set "JAVA_CMD=java"
)

rem 检查 Java 是否可用
where "%JAVA_CMD%" >nul 2>&1
if errorlevel 1 (
    echo 错误: Java 未找到，请确保 JAVA_HOME 设置正确或 java 在 PATH 中
    exit /b 1
)

rem 设置 JVM 参数
set "JVM_OPTS=-Xms512m -Xmx1024m"

rem 设置日志目录
set "LOG_DIR=%SEATUNNEL_HOME%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem 执行转换工具
echo 启动 X2SeaTunnel 配置转换工具...
echo 使用 JAR: %CLI_JAR%
echo Java 命令: %JAVA_CMD%
echo.

"%JAVA_CMD%" %JVM_OPTS% -jar "%CLI_JAR%" %*
