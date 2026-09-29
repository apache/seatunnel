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

# Export connector metadata as structured JSON via runtime reflection.
# Uses factory.optionRule() — same mechanism as SeaTunnel Web. 100% accurate.
#
# Usage:
#   seatunnel-metadata-export.sh                          # Export all to connector_metadata.json
#   seatunnel-metadata-export.sh -o /path/to/output.json  # Custom output path
#   seatunnel-metadata-export.sh -pt source -p Jdbc       # Export only Jdbc source
#   seatunnel-metadata-export.sh --stdout                  # Write to stdout (for piping)

set -eu
# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=$(dirname "$PRG")
APP_DIR=$(cd "$PRG_DIR/.." >/dev/null; pwd)
APP_JAR=${APP_DIR}/starter/seatunnel-starter.jar
LOAD_CLASS="org.apache.seatunnel.core.starter.seatunnel.SeaTunnelMetadataExporter"

if [ $# == 0 ]
then
    args=""
else
    args=$@
fi

set +u
CLASS_PATH=${APP_DIR}/connectors/*:${APP_JAR}:${APP_DIR}/lib/seatunnel-transforms-v2.jar

java -cp ${CLASS_PATH} ${LOAD_CLASS} ${args} 2>/dev/null
