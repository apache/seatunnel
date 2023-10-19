#!/usr/bin/env bash

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

SCRIPT_DIR="$(dirname "$0")"
SEATUNNEL_ENGINE_HOME="$(cd "$SCRIPT_DIR/../../../../"; pwd)"

PYTHON="$(which python3 2>/dev/null)"
PIP3="$(which pip3 2>/dev/null)"
GIT="$(which git 2>/dev/null)"

PROTOCOL_DIRECTORY=`mktemp -d 2>/dev/null || mktemp -d -t 'protocol'`

if [ -z "$PYTHON" ]; then
    echo "Python 3 could not be found in your system."
    exit 1
fi

if [ -z "$PIP3" ]; then
    echo "PIP 3 could not be found in your system."
    exit 1
fi

if [ -z "$GIT" ]; then
    echo "Git could not be found in your system."
    exit 1
fi

echo $SCRIPT_DIR
echo $SEATUNNEL_ENGINE_HOME
echo $PROTOCOL_DIRECTORY

$GIT clone --depth=1 https://github.com/hazelcast/hazelcast-client-protocol.git $PROTOCOL_DIRECTORY

cd $PROTOCOL_DIRECTORY

$PIP3 install -r requirements.txt

$PYTHON generator.py -r $SEATUNNEL_ENGINE_HOME -p $SEATUNNEL_ENGINE_HOME/seatunnel-engine-core/src/main/resources/client-protocol-definition \
-o $SEATUNNEL_ENGINE_HOME/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/protocol/codec \
-n org.apache.seatunnel.engine.core.protocol.codec --no-binary --no-id-check

rm -rf $PROTOCOL_DIRECTORY