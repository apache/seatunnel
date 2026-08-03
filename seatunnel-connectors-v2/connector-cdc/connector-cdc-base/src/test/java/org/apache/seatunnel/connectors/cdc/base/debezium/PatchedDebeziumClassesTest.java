/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pins the set of Debezium classes that connector-cdc-base recompiles and ships patched copies of.
 *
 * <p>Those classes exist in debezium-core too. connector-cdc-base is loaded into the class loader
 * of every CDC connector, and each connector now packages its own Debezium, so both copies would be
 * visible at runtime and the winner would depend on the order {@code File#listFiles} returns the
 * jars in {@code AbstractPluginDiscovery}. The shade configuration in connector-cdc/pom.xml drops
 * the stock copies from every connector jar so the patched ones stay the single definition.
 *
 * <p>That exclusion list is maintained by hand, so this test fails whenever the set of patched
 * classes changes. If it does, update the {@code io.debezium:debezium-core} filter in
 * connector-cdc/pom.xml and in connector-cdc-opengauss/pom.xml, which declares its own filter list.
 */
class PatchedDebeziumClassesTest {

    /**
     * Debezium classes overridden under src/main/java/io/debezium. Every entry here must also be
     * excluded from debezium-core by the CDC shade configuration.
     */
    private static final Set<String> EXPECTED_PATCHED_CLASSES =
            new TreeSet<>(
                    Arrays.asList(
                            "io/debezium/connector/base/ChangeEventQueue",
                            "io/debezium/heartbeat/DefaultHeartbeatConnectionProvider",
                            "io/debezium/heartbeat/HeartbeatFactory",
                            "io/debezium/relational/HistorizedRelationalDatabaseConnectorConfig",
                            "io/debezium/relational/TableId"));

    /**
     * Verifies the Debezium classes this module compiles are exactly the ones the shade filters
     * account for, so a newly patched class cannot silently end up duplicated at runtime.
     */
    @Test
    void patchedDebeziumClassesMatchTheShadeExclusionList() throws Exception {
        Path classesRoot =
                Paths.get(
                        SourceOptions.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        Path debeziumRoot = classesRoot.resolve("io").resolve("debezium");
        Assertions.assertTrue(
                Files.isDirectory(debeziumRoot),
                "Expected compiled Debezium overrides under " + debeziumRoot);

        Set<String> compiled;
        try (Stream<Path> paths = Files.walk(debeziumRoot)) {
            compiled =
                    paths.filter(Files::isRegularFile)
                            .map(path -> classesRoot.relativize(path).toString())
                            .filter(name -> name.endsWith(".class"))
                            .map(name -> name.substring(0, name.length() - ".class".length()))
                            // Nested and anonymous classes travel with their outer class.
                            .filter(name -> !name.contains("$"))
                            .map(name -> name.replace(File.separatorChar, '/'))
                            .collect(Collectors.toCollection(TreeSet::new));
        }

        Assertions.assertEquals(
                EXPECTED_PATCHED_CLASSES,
                compiled,
                "connector-cdc-base patches a different set of Debezium classes than the CDC shade"
                        + " filters exclude from debezium-core. Update the"
                        + " io.debezium:debezium-core filter in connector-cdc/pom.xml and"
                        + " connector-cdc-opengauss/pom.xml, then update this test.");
    }
}
