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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss.debezium;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapter;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapterFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.debezium.PostgresDebeziumAdapter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
 * Covers Debezium version ownership for connector-cdc-opengauss.
 *
 * <p>This module is the one place where two modules' Debezium versions actually meet.
 * connector-cdc-opengauss declares its own {@code debezium.version} for debezium-embedded, but
 * reaches debezium-connector-postgres transitively through connector-cdc-postgres, and it
 * recompiles Debezium's PostgreSQL connection internals (see
 * src/main/java/io/debezium/connector/postgresql). A Debezium bump applied to one module and not
 * the other therefore puts two Debezium versions on a single class path, with openGauss's patched
 * copies of Debezium internals compiled against the wrong one.
 *
 * <p>openGauss also deliberately registers no adapter of its own and reuses {@link
 * PostgresDebeziumAdapter}. The shade plugin merges META-INF/services entries, so an
 * openGauss-specific provider for the same {@code connector.class} would land in the same jar and
 * make every openGauss CDC job fail the exactly-one-match rule in {@link DebeziumAdapterFactory}.
 */
class OpengaussDebeziumAdapterTest {

    /** Debezium {@code connector.class} that openGauss CDC runs on. */
    private static final String POSTGRES_CONNECTOR_CLASS =
            "io.debezium.connector.postgresql.PostgresConnector";

    /**
     * Every Debezium artifact this connector packages. debezium-connector-postgres is contributed
     * by connector-cdc-postgres while the rest come from this module, which is exactly the split
     * that can drift.
     */
    private static final List<String> PACKAGED_DEBEZIUM_ARTIFACTS =
            Arrays.asList(
                    "debezium-api",
                    "debezium-core",
                    "debezium-embedded",
                    "debezium-connector-postgres");

    /**
     * Verifies exactly one adapter is visible for the PostgreSQL connector class on the openGauss
     * class path, and that it is the inherited connector-cdc-postgres one.
     */
    @Test
    void exactlyOneAdapterIsVisibleForPostgresConnector() {
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(
                        POSTGRES_CONNECTOR_CLASS,
                        OpengaussDebeziumAdapterTest.class.getClassLoader());

        Assertions.assertInstanceOf(PostgresDebeziumAdapter.class, adapter);
    }

    /**
     * Verifies this module's Debezium runtime and the Debezium PostgreSQL connector it inherits
     * resolve to the same version, and that the version matches what the reused adapter declares.
     * This is what keeps connector-cdc-opengauss and connector-cdc-postgres from being bumped
     * independently.
     */
    @Test
    void packagedDebeziumArtifactsAllMatchInheritedAdapterVersion() {
        String declaredVersion = new PostgresDebeziumAdapter().getDebeziumVersion();
        Map<String, String> resolvedVersions = resolveDebeziumVersions();

        Assertions.assertEquals(
                Collections.singleton(declaredVersion),
                new HashSet<>(resolvedVersions.values()),
                "connector-cdc-opengauss and connector-cdc-postgres must ship the same Debezium"
                        + " version, but the class path resolved: "
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
                    OpengaussDebeziumAdapterTest.class
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
