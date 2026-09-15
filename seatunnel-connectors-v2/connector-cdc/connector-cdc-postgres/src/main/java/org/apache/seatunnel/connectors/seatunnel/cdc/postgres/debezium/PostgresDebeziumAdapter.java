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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.debezium;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapter;

/**
 * Declares the Debezium version that the PostgreSQL CDC connector is built and packaged against.
 *
 * <p>The version is owned by this module: {@code connector-cdc-base} declares Debezium as {@code
 * provided}, so the Debezium runtime a PostgreSQL CDC job loads comes from this connector's own
 * shaded jar at this module's {@code debezium.version} property.
 */
public class PostgresDebeziumAdapter implements DebeziumAdapter {

    /**
     * Debezium version this connector ships. Must stay equal to the {@code debezium.version}
     * property in this module's {@code pom.xml}; {@code PostgresDebeziumAdapterTest} compares it
     * against the Debezium actually resolved onto the classpath and fails the build if the two
     * drift apart.
     */
    private static final String DEBEZIUM_VERSION = "1.9.8.Final";

    /**
     * Debezium {@code connector.class} handled by this connector. Kept as a literal rather than a
     * class reference so that adapter discovery never requires the Debezium connector class itself
     * to be loadable from the class loader performing the lookup.
     */
    private static final String CONNECTOR_CLASS =
            "io.debezium.connector.postgresql.PostgresConnector";

    @Override
    public String getDebeziumVersion() {
        return DEBEZIUM_VERSION;
    }

    @Override
    public boolean supports(String connectorClassName) {
        return CONNECTOR_CLASS.equals(connectorClassName);
    }
}
