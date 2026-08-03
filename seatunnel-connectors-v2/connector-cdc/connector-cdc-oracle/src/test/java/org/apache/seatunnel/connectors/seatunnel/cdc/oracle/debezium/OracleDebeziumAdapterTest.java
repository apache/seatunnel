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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.debezium;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapter;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapterFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnector;

import java.io.InputStream;
import java.util.Properties;

/**
 * Covers the Debezium version ownership of connector-cdc-oracle.
 *
 * <p>The regression this guards against is a silent divergence between the three places that have
 * to agree: the {@code debezium.version} property in this module's pom.xml, the version {@link
 * OracleDebeziumAdapter} reports, and the Debezium actually resolved onto the connector's
 * classpath. If they diverge, a job would run on a Debezium build that nothing in the project
 * claims to support.
 */
class OracleDebeziumAdapterTest {

    /**
     * Resource published by the debezium-core artifact itself. Reading the version from here means
     * the assertion reflects the dependency Maven really resolved, not a value restated in test
     * code.
     */
    private static final String DEBEZIUM_CORE_POM_PROPERTIES =
            "META-INF/maven/io.debezium/debezium-core/pom.properties";

    /**
     * Verifies the META-INF/services registration resolves to exactly one adapter for the MySQL
     * connector class, which is the contract {@link DebeziumAdapterFactory} enforces.
     */
    @Test
    void adapterIsDiscoverableAndUniqueForOracleConnector() {
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(
                        OracleConnector.class.getName(),
                        OracleDebeziumAdapterTest.class.getClassLoader());

        Assertions.assertInstanceOf(OracleDebeziumAdapter.class, adapter);
    }

    /**
     * Verifies the adapter claims the Oracle connector class and nothing else, so that adding
     * further CDC connectors cannot produce an ambiguous match.
     */
    @Test
    void adapterSupportsOnlyItsOwnConnectorClass() {
        OracleDebeziumAdapter adapter = new OracleDebeziumAdapter();

        Assertions.assertTrue(adapter.supports(OracleConnector.class.getName()));
        Assertions.assertFalse(adapter.supports("io.debezium.connector.mysql.MySqlConnector"));
    }

    /**
     * Verifies the declared Debezium version is the one this connector actually packages. This is
     * the check that keeps per-connector version ownership honest.
     */
    @Test
    void declaredVersionMatchesPackagedDebezium() throws Exception {
        Assertions.assertEquals(
                resolvedDebeziumCoreVersion(), new OracleDebeziumAdapter().getDebeziumVersion());
    }

    private static String resolvedDebeziumCoreVersion() throws Exception {
        try (InputStream in =
                OracleDebeziumAdapterTest.class
                        .getClassLoader()
                        .getResourceAsStream(DEBEZIUM_CORE_POM_PROPERTIES)) {
            Assertions.assertNotNull(in, "debezium-core is not on the classpath of this module");
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version");
        }
    }
}
