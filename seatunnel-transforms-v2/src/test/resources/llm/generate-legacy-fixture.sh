#!/usr/bin/env bash
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

set -euo pipefail
: "${JAVA_HOME:?Set JAVA_HOME to JDK 8 or 11}"
root=$(git rev-parse --show-toplevel)
module="$root/seatunnel-transforms-v2"
resource="$module/src/test/resources/llm"
baseline=8bea8c681cacccbd99fa642ca0853718980b9234
scratch=$(mktemp -d "$module/target/legacy-fixture.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/classes"
"$root/mvnw" -T1 -q -f "$module/pom.xml" dependency:build-classpath \
    -Dmdep.outputFile="$scratch/classpath.txt"
classpath="$module/target/classes:$root/seatunnel-api/target/classes:$root/seatunnel-common/target/classes:$(<"$scratch/classpath.txt")"
for name in LLMTransform LLMTransformConfig LLMTransformFactory LLMMultiCatalogTransform; do
    git -C "$root" show "$baseline:seatunnel-transforms-v2/src/main/java/org/apache/seatunnel/transform/nlpmodel/llm/$name.java" > "$scratch/$name.java"
done
"$JAVA_HOME/bin/javac" -source 8 -target 8 -cp "$classpath" \
    -d "$scratch/classes" "$scratch/"*.java "$resource/GenerateLegacyLLMFixture.java"
"$JAVA_HOME/bin/serialver" -classpath "$scratch/classes:$classpath" \
    org.apache.seatunnel.transform.nlpmodel.llm.LLMTransform
"$JAVA_HOME/bin/java" -cp "$scratch/classes:$classpath" GenerateLegacyLLMFixture \
    "${1:-$scratch/legacy-boolean-wrapper.base64}"
if [ "$#" -eq 0 ]; then
    cmp "$scratch/legacy-boolean-wrapper.base64" "$resource/legacy-boolean-wrapper.base64"
fi
