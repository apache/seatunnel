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

# X2SeaTunnel configuration conversion tool startup script

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
X2SEATUNNEL_HOME="$(dirname "$SCRIPT_DIR")"

# Set X2SeaTunnel related environment variables
export X2SEATUNNEL_CONFIG_DIR="$X2SEATUNNEL_HOME/config"
export X2SEATUNNEL_TEMPLATES_DIR="$X2SEATUNNEL_HOME/templates"

# Find X2SeaTunnel JAR file
find_jar() {
    local jar_file=""

    # 1. First search in packaged lib directory (production environment)
    if [ -d "$X2SEATUNNEL_HOME/lib" ]; then
        jar_file=$(find "$X2SEATUNNEL_HOME/lib" -name "x2seatunnel-*.jar" 2>/dev/null | head -1)
    fi

    # 2. Search in starter directory (SeaTunnel standard directory structure)
    if [ -z "$jar_file" ] && [ -d "$X2SEATUNNEL_HOME/starter" ]; then
        jar_file=$(find "$X2SEATUNNEL_HOME/starter" -name "x2seatunnel-*.jar" 2>/dev/null | head -1)
    fi

    # 3. If running in development environment resource directory, locate target directory of x2seatunnel module root
    module_root="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
    if [ -z "$jar_file" ] && [ -d "$module_root/target" ]; then
        jar_file=$(find "$module_root/target" -name "x2seatunnel-*.jar" 2>/dev/null | grep -v sources | head -1)
    fi

    if [ -z "$jar_file" ] || [ ! -f "$jar_file" ]; then
        echo "Error: X2SeaTunnel JAR file not found"
        echo "Search paths:"
        echo "  - $X2SEATUNNEL_HOME/lib/"
        echo "  - $X2SEATUNNEL_HOME/starter/"
        echo "  - $module_root/target/"
        echo ""
        echo "If in development environment, please compile first: mvn clean package -pl seatunnel-tools/x2seatunnel -am"
        exit 1
    fi

    echo "$jar_file"
}

# Check Java environment
check_java() {
    if [ -n "$JAVA_HOME" ]; then
        JAVA_CMD="$JAVA_HOME/bin/java"
    else
        JAVA_CMD="java"
    fi

    if ! command -v "$JAVA_CMD" > /dev/null 2>&1; then
        echo "Error: Java not found, please ensure JAVA_HOME is set correctly or java is in PATH"
        exit 1
    fi

    # Check Java version
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
        echo "Error: Java 8 or higher is required, current version: $java_version"
        exit 1
    fi
}

# Main function
main() {
    echo "Starting X2SeaTunnel configuration conversion tool..."

    # Check Java environment
    check_java

    # Find JAR file
    CLI_JAR=$(find_jar)
    echo "Using JAR: $CLI_JAR"
    echo "Java command: $JAVA_CMD"

    # Set JVM parameters
    JVM_OPTS="-Xms512m -Xmx1024m"

    # Set log configuration file path
    LOG4J2_CONFIG="$X2SEATUNNEL_CONFIG_DIR/log4j2.xml"
    if [ -f "$LOG4J2_CONFIG" ]; then
        JVM_OPTS="$JVM_OPTS -Dlog4j.configurationFile=$LOG4J2_CONFIG"
        echo "Using log configuration: $LOG4J2_CONFIG"
    else
        echo "Warning: Log configuration file does not exist: $LOG4J2_CONFIG"
    fi

    # Set log directory
    LOG_DIR="$X2SEATUNNEL_HOME/logs"
    mkdir -p "$LOG_DIR"

    # Build execution command
    EXEC_CMD="\"$JAVA_CMD\" $JVM_OPTS \
        -DX2SEATUNNEL_HOME=\"$X2SEATUNNEL_HOME\" \
        -DX2SEATUNNEL_CONFIG_DIR=\"$X2SEATUNNEL_CONFIG_DIR\" \
        -DX2SEATUNNEL_TEMPLATES_DIR=\"$X2SEATUNNEL_TEMPLATES_DIR\" \
        -jar \"$CLI_JAR\" $@"

    echo
    eval $EXEC_CMD
}

# Run main function
main "$@"
