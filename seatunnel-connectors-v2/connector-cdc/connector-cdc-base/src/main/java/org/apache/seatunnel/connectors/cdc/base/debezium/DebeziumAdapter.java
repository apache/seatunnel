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

import org.apache.seatunnel.api.annotation.Experimental;

/**
 * SPI contract for connector-specific Debezium version adapters.
 *
 * <p>Implementations are loaded via {@link java.util.ServiceLoader} from each connector module's
 * {@code META-INF/services} registration. This interface defines the target shape for per-connector
 * Debezium version management; concrete wiring (schema-history isolation, classloader separation)
 * will be added in follow-up PRs once this SPI is established.
 *
 * <p>This interface is {@link Experimental} and may change without notice until the full runtime
 * wiring lands in a follow-up PR.
 */
@Experimental
public interface DebeziumAdapter {

    /** Returns the Debezium version string this adapter targets (e.g., {@code "1.9.8.Final"}). */
    String getDebeziumVersion();

    /**
     * Returns {@code true} if this adapter handles the given Debezium connector fully-qualified
     * class name (e.g., {@code "io.debezium.connector.mysql.MySqlConnector"}). This matches the
     * value of the {@code connector.class} Debezium property.
     */
    boolean supports(String connectorClassName);
}
