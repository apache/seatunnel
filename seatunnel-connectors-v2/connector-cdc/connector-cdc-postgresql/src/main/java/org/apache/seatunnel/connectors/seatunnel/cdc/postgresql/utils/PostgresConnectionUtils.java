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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.utils;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.connection.PostgresConnection;

import java.nio.charset.Charset;

/** Utils for Postgres connection. */
public class PostgresConnectionUtils {

    public static PostgresConnection createPostgresConnection(Configuration dbzConfiguration) {

        final PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(dbzConfiguration);

        PostgresConnection heartbeatConnection =
                new PostgresConnection(connectorConfig.getJdbcConfig());
        final Charset databaseCharset = heartbeatConnection.getDatabaseCharset();
        final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                (typeRegistry) ->
                        PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);

        return new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder);
    }
}
