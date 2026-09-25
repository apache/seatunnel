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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Checks the packaged connector jar against connector-cdc-base, which plugin discovery always puts
 * on the classpath next to a CDC connector jar.
 */
class OceanBaseCdcPackagingIT {

    @Test
    void packagedJarDoesNotShadowDebeziumClassesFromCdcBase() throws Exception {
        Set<String> shadowed = debeziumClassEntries(packagedConnectorJar());
        shadowed.retainAll(debeziumClassEntries(cdcBaseJar()));
        Assertions.assertTrue(
                shadowed.isEmpty(),
                "connector-cdc-oceanbase must not bundle Debezium classes that connector-cdc-base"
                        + " provides, found "
                        + shadowed.size()
                        + ", e.g. "
                        + shadowed.stream().limit(5).collect(Collectors.toList()));
    }

    @Test
    void tableIdIsSerializableWhenConnectorJarComesFirst() throws Exception {
        URL[] urls = {packagedConnectorJar().toUri().toURL(), cdcBaseJar().toUri().toURL()};
        try (URLClassLoader loader = new URLClassLoader(urls, null)) {
            Object tableId =
                    Class.forName("io.debezium.relational.TableId", false, loader)
                            .getConstructor(String.class, String.class, String.class)
                            .newInstance("oceanbase_cdc", null, "products");
            // Same shape as the TableId-keyed maps held by the CDC deserialization schema.
            Map<Object, byte[]> tableChanges = new HashMap<>();
            tableChanges.put(tableId, new byte[0]);
            try (ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream())) {
                out.writeObject(tableChanges);
            }
        }
    }

    private static Path packagedConnectorJar() {
        String jar = System.getProperty("connector.jar");
        Assertions.assertNotNull(jar, "connector.jar is set by the failsafe configuration");
        Path path = Paths.get(jar);
        Assertions.assertTrue(Files.isRegularFile(path), "packaged artifact missing: " + path);
        return path;
    }

    private static Path cdcBaseJar() throws Exception {
        Path path =
                Paths.get(
                        IncrementalSource.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        Assertions.assertTrue(
                Files.isRegularFile(path), "connector-cdc-base is not a packaged jar: " + path);
        return path;
    }

    private static Set<String> debeziumClassEntries(Path jarPath) throws IOException {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            return jar.stream()
                    .map(JarEntry::getName)
                    .filter(name -> name.startsWith("io/debezium/") && name.endsWith(".class"))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
