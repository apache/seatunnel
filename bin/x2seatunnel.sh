#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# X2SeaTunnel 配置转换工具启动脚本

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SEATUNNEL_HOME="$(dirname "$SCRIPT_DIR")"

# 设置 X2SeaTunnel 相关环境变量
export X2SEATUNNEL_HOME="$SEATUNNEL_HOME"
export X2SEATUNNEL_CONFIG_DIR="$SEATUNNEL_HOME/config"
export X2SEATUNNEL_TEMPLATES_DIR="$SEATUNNEL_HOME/config/templates"

# 查找 X2SeaTunnel JAR 文件
find_jar() {
    local jar_file=""
    
    # 1. 优先从打包后的 lib 目录查找（生产环境）
    if [ -d "$SEATUNNEL_HOME/lib" ]; then
        jar_file=$(find "$SEATUNNEL_HOME/lib" -name "x2seatunnel-*.jar" 2>/dev/null | head -1)
    fi
    
    # 2. 从 starter 目录查找（SeaTunnel 标准目录结构）
    if [ -z "$jar_file" ] && [ -d "$SEATUNNEL_HOME/starter" ]; then
        jar_file=$(find "$SEATUNNEL_HOME/starter" -name "x2seatunnel-*.jar" 2>/dev/null | head -1)
    fi
    
    # 3. 从开发环境的 target 目录查找（开发环境）
    if [ -z "$jar_file" ] && [ -d "$SEATUNNEL_HOME/seatunnel-tools/x2seatunnel/target" ]; then
        jar_file=$(find "$SEATUNNEL_HOME/seatunnel-tools/x2seatunnel/target" -name "x2seatunnel-*.jar" | grep -v sources | head -1)
    fi
    
    if [ -z "$jar_file" ] || [ ! -f "$jar_file" ]; then
        echo "错误: 未找到 X2SeaTunnel JAR 文件"
        echo "搜索路径:"
        echo "  - $SEATUNNEL_HOME/lib/"
        echo "  - $SEATUNNEL_HOME/starter/"
        echo "  - $SEATUNNEL_HOME/seatunnel-tools/x2seatunnel/target/"
        echo ""
        echo "如果是开发环境，请先编译: mvn clean package -pl seatunnel-tools -am"
        exit 1
    fi
    
    echo "$jar_file"
}

# 检查 Java 环境
check_java() {
    if [ -n "$JAVA_HOME" ]; then
        JAVA_CMD="$JAVA_HOME/bin/java"
    else
        JAVA_CMD="java"
    fi
    
    if ! command -v "$JAVA_CMD" > /dev/null 2>&1; then
        echo "错误: Java 未找到，请确保 JAVA_HOME 设置正确或 java 在 PATH 中"
        exit 1
    fi
    
    # 检查 Java 版本
    java_version=$("$JAVA_CMD" -version 2>&1 | head -1 | cut -d'"' -f2)
    case "$java_version" in
        1.8*)
            java_major_version=8
            ;;
        *)
            java_major_version=$(echo "$java_version" | cut -d'.' -f1)
            ;;
    esac
    
    if [ "$java_major_version" -lt 8 ]; then
        echo "错误: 需要 Java 8 或更高版本，当前版本: $java_version"
        exit 1
    fi
}

# 主函数
main() {
    echo "启动 X2SeaTunnel 配置转换工具..."
    
    # 检查 Java 环境
    check_java
    
    # 查找 JAR 文件
    CLI_JAR=$(find_jar)
    echo "使用 JAR: $CLI_JAR"
    echo "Java 命令: $JAVA_CMD"
    echo
    
    # 设置 JVM 参数
    JVM_OPTS="-Xms512m -Xmx1024m"
    
    # 设置日志目录
    LOG_DIR="$SEATUNNEL_HOME/logs"
    mkdir -p "$LOG_DIR"
    
    # 执行转换工具
    "$JAVA_CMD" $JVM_OPTS \
        -DX2SEATUNNEL_HOME="$X2SEATUNNEL_HOME" \
        -DX2SEATUNNEL_CONFIG_DIR="$X2SEATUNNEL_CONFIG_DIR" \
        -DX2SEATUNNEL_TEMPLATES_DIR="$X2SEATUNNEL_TEMPLATES_DIR" \
        -jar "$CLI_JAR" "$@"
}

# 运行主函数
main "$@"
