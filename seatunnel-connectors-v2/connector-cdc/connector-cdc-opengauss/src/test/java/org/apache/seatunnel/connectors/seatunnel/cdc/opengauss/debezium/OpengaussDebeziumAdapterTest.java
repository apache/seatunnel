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

import java.io.InputStream;
import java.util.Properties;

/**
 * Covers Debezium version ownership for connector-cdc-opengauss.
 *
 * <p>openGauss CDC runs Debezium's PostgreSQL connector and shades connector-cdc-postgres into its
 * own jar, so it deliberately registers no adapter of its own and reuses {@link
 * PostgresDebeziumAdapter}. The regression this guards against is a future openGauss-specific
 * adapter registered for the same {@code connector.class}: the shade plugin merges
 * META-INF/services entries, so both providers would land in one jar and every openGauss CDC job
 * would fail the exactly-one-match rule in {@link DebeziumAdapterFactory}.
 */
class OpengaussDebeziumAdapterTest {

    /** Debezium {@code connector.class} that openGauss CDC runs on. */
    private static final String POSTGRES_CONNECTOR_CLASS =
            "io.debezium.connector.postgresql.PostgresConnector";

    /**
     * Resource published by the debezium-core artifact itself. Reading the version from here means
     * the assertion reflects the dependency Maven really resolved, not a value restated in test
     * code.
     */
    private static final String DEBEZIUM_CORE_POM_PROPERTIES =
            "META-INF/maven/io.debezium/debezium-core/pom.properties";

    /**
     * Verifies exactly one adapter is visible for the PostgreSQL connector class on the openGauss
     * classpath, and that it is the inherited connector-cdc-postgres one.
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
     * Verifies this module's declared debezium.version stays in step with the Debezium it actually
     * resolves, which for openGauss also means staying in step with connector-cdc-postgres because
     * it recompiles Debezium PostgreSQL internals.
     */
    @Test
    void inheritedAdapterVersionMatchesPackagedDebezium() throws Exception {
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(
                        POSTGRES_CONNECTOR_CLASS,
                        OpengaussDebeziumAdapterTest.class.getClassLoader());

        Assertions.assertEquals(resolvedDebeziumCoreVersion(), adapter.getDebeziumVersion());
    }

    private static String resolvedDebeziumCoreVersion() throws Exception {
        try (InputStream in =
                OpengaussDebeziumAdapterTest.class
                        .getClassLoader()
                        .getResourceAsStream(DEBEZIUM_CORE_POM_PROPERTIES)) {
            Assertions.assertNotNull(in, "debezium-core is not on the classpath of this module");
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version");
        }
    }
}
