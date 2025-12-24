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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConfiguration;

/** Utils for SqlServer connection. */
public class SqlServerConnectionUtils {

    public static SqlServerConnection createSqlServerConnection(Configuration dbzConfiguration) {
        final SqlServerConnectorConfig connectorConfig =
                new SqlServerConnectorConfig(dbzConfiguration);
        final SqlServerValueConverters valueConverters =
                new SqlServerValueConverters(
                        connectorConfig.getDecimalMode(),
                        connectorConfig.getTemporalPrecisionMode(),
                        connectorConfig.binaryHandlingMode());
        return new SqlServerConnection(
                JdbcConfiguration.adapt(connectorConfig.getJdbcConfig()),
                connectorConfig.getSourceTimestampMode(),
                valueConverters,
                SqlServerConnectionUtils.class::getClassLoader,
                connectorConfig.getSkippedOperations(),
                false);
    }
}
