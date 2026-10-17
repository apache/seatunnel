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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.debezium;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapter;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapterFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.mongodb.MongoDbConnector;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Covers the Debezium version ownership of connector-cdc-mongodb.
 *
 * <p>The regression this guards against is a silent divergence between the places that have to
 * agree: the {@code debezium.version} property in this module's pom.xml, the version
 * MongoDbDebeziumAdapter reports, and the Debezium artifacts actually resolved onto this
 * connector's class path. If they diverge, a job would run on a Debezium build that nothing in the
 * project claims to support.
 */
class MongoDbDebeziumAdapterTest {

    /**
     * Every Debezium artifact this connector packages. They are checked as one unit rather than
     * spot-checking debezium-core, because a connector's Debezium runtime and its Debezium
     * connector artifact can be introduced by different modules and drift apart independently.
     */
    private static final List<String> PACKAGED_DEBEZIUM_ARTIFACTS =
            Arrays.asList(
                    "debezium-api",
                    "debezium-core",
                    "debezium-embedded",
                    "debezium-connector-mongodb");

    /**
     * Verifies the META-INF/services registration resolves to exactly one adapter for the MongoDB
     * connector class, which is the contract {@link DebeziumAdapterFactory} enforces.
     */
    @Test
    void adapterIsDiscoverableAndUniqueForMongoDbConnector() {
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(
                        MongoDbConnector.class.getName(),
                        MongoDbDebeziumAdapterTest.class.getClassLoader());

        Assertions.assertInstanceOf(MongoDbDebeziumAdapter.class, adapter);
    }

    /**
     * Verifies the adapter claims the MongoDB connector class and nothing else, so that adding
     * further CDC connectors cannot produce an ambiguous match.
     */
    @Test
    void adapterSupportsOnlyItsOwnConnectorClass() {
        MongoDbDebeziumAdapter adapter = new MongoDbDebeziumAdapter();

        Assertions.assertTrue(adapter.supports(MongoDbConnector.class.getName()));
        Assertions.assertFalse(adapter.supports("io.debezium.connector.mysql.MySqlConnector"));
    }

    /**
     * Verifies every Debezium artifact on this connector's class path resolves to the single
     * version the adapter declares. This is the check that keeps per-connector version ownership
     * honest: a pom.xml bump that forgets the adapter, an adapter bump that forgets the pom.xml, or
     * a partial bump that leaves two Debezium versions on one class path all fail here.
     */
    @Test
    void packagedDebeziumArtifactsAllMatchDeclaredVersion() {
        String declaredVersion = new MongoDbDebeziumAdapter().getDebeziumVersion();
        Map<String, String> resolvedVersions = resolveDebeziumVersions();

        Assertions.assertEquals(
                Collections.singleton(declaredVersion),
                new HashSet<>(resolvedVersions.values()),
                "Debezium artifacts on the class path do not all match the version declared by "
                        + MongoDbDebeziumAdapter.class.getSimpleName()
                        + ": "
                        + resolvedVersions);
    }

    /**
     * Reads the version each Debezium artifact publishes about itself, so the assertion reflects
     * what Maven really resolved rather than a value restated in test code.
     */
    private static Map<String, String> resolveDebeziumVersions() {
        Map<String, String> versions = new LinkedHashMap<>();
        for (String artifactId : PACKAGED_DEBEZIUM_ARTIFACTS) {
            String resource = "META-INF/maven/io.debezium/" + artifactId + "/pom.properties";
            try (InputStream in =
                    MongoDbDebeziumAdapterTest.class
                            .getClassLoader()
                            .getResourceAsStream(resource)) {
                Assertions.assertNotNull(
                        in, artifactId + " is not on the class path of this module");
                Properties properties = new Properties();
                properties.load(in);
                versions.put(artifactId, properties.getProperty("version"));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read " + resource, e);
            }
        }
        return versions;
    }
}
