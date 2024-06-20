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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import io.debezium.connector.postgresql.CustomPostgresValueConverter;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;

import java.nio.charset.Charset;
import java.time.ZoneId;

public class PostgresConnectionUtils {

    /**
     * Create a new PostgresVauleConverterBuilder instance and offer type registry for JDBC
     * connection.
     *
     * <p>It is created in this package because some methods (e.g., includeUnknownDatatypes) of
     * PostgresConnectorConfig is protected.
     */
    public static PostgresConnection.PostgresValueConverterBuilder newPostgresValueConverterBuilder(
            PostgresConnectorConfig config, String connectionUsage, ZoneId zoneId) {
        try (PostgresConnection heartbeatConnection =
                new PostgresConnection(config.getJdbcConfig(), connectionUsage)) {
            final Charset databaseCharset = heartbeatConnection.getDatabaseCharset();
            return (typeRegistry) ->
                    CustomPostgresValueConverter.of(config, databaseCharset, typeRegistry, zoneId);
        }
    }

    public static PostgresConnection.PostgresValueConverterBuilder newPostgresValueConverterBuilder(
            PostgresConnectorConfig config, String connectionUsage, String serverTimezone) {
        try (PostgresConnection heartbeatConnection =
                new PostgresConnection(config.getJdbcConfig(), connectionUsage)) {
            final Charset databaseCharset = heartbeatConnection.getDatabaseCharset();
            return (typeRegistry) ->
                    CustomPostgresValueConverter.of(
                            config, databaseCharset, typeRegistry, ZoneId.of(serverTimezone));
        }
    }
}
