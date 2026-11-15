<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Legacy `ChainedMapTransform` Serialization Fixture

`legacy-chained-map.base64` contains Java serialization bytes written with
`ChainedMapTransform` from commit `02b156e45`, before `extraTablePaths` was
added. The archived class has the implicit UID `8769738606872706499`.

The fixture is read by `AbstractMultiCatalogTransformTest` to verify that the
current `ChainedMapTransform` can deserialize and use a legacy instance. The
two child `IdentityMapTransform` instances use current support classes, so
this fixture isolates `ChainedMapTransform` compatibility; it does not
represent a complete historical job plan.

## Regenerating the fixture

Compile the transform module first. Then, from the repository root, use JDK 17
to compile the old `ChainedMapTransform` source from the baseline commit with
`GenerateLegacyChainedMapFixture.java` and generate the Base64 stream:

```sh
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
./mvnw -q -f seatunnel-transforms-v2/pom.xml dependency:build-classpath \
  -Dmdep.outputFile="$scratch/classpath.txt"
classpath="seatunnel-transforms-v2/target/classes:seatunnel-api/target/classes:seatunnel-common/target/classes:$(cat "$scratch/classpath.txt")"
git show 02b156e45:seatunnel-transforms-v2/src/main/java/org/apache/seatunnel/transform/common/ChainedMapTransform.java > "$scratch/ChainedMapTransform.java"
mkdir "$scratch/classes"
"$JAVA_HOME/bin/javac" -source 8 -target 8 -cp "$classpath" \
  -d "$scratch/classes" "$scratch/ChainedMapTransform.java" \
  seatunnel-transforms-v2/src/test/resources/serialization/GenerateLegacyChainedMapFixture.java
"$JAVA_HOME/bin/java" -cp "$scratch/classes:$classpath" \
  org.apache.seatunnel.transform.common.GenerateLegacyChainedMapFixture \
  "$scratch/legacy-chained-map.base64"
cmp "$scratch/legacy-chained-map.base64" \
  seatunnel-transforms-v2/src/test/resources/serialization/legacy-chained-map.base64
```

The scratch directory is placed first on the runtime classpath so the generator
loads the old `ChainedMapTransform`; API, Common, and child transform support
classes come from the current build. The generator fails if the loaded class
declares `extraTablePaths` or `serialVersionUID`, guarding against accidentally
generating the fixture with the current class. `cmp` checks that the generated
bytes match the committed fixture.

Normal tests only read the committed fixture. They do not run this generator
or require Git history, JDK 17, or a compiler.
