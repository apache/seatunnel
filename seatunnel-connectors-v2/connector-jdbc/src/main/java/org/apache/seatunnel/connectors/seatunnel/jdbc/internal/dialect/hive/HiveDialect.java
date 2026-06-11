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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HiveDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return DatabaseIdentifier.HIVE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new HiveJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new HiveTypeMapper();
    }

    @Override
    public boolean supportsPrimaryKeyMetadata() {
        // Hive 3.x can throw from getPrimaryKeys while partition metadata is still available.
        return false;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        return Optional.empty();
    }

    @Override
    public ResultSetMetaData getResultSetMetaData(Connection conn, String query)
            throws SQLException {
        try (PreparedStatement preparedStatement = conn.prepareStatement(query);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            return resultSet.getMetaData();
        }
    }

    @Override
    public JdbcConnectionProvider getJdbcConnectionProvider(
            JdbcConnectionConfig jdbcConnectionConfig) {
        return new HiveJdbcConnectionProvider(jdbcConnectionConfig);
    }

    @Override
    public List<String> getPartitionKeys(Connection connection, TablePath tablePath)
            throws SQLException {
        List<String> partitionKeys = new ArrayList<>();
        boolean partitionSection = false;
        boolean partitionHeader = false;
        String describeSql = "DESCRIBE " + tableIdentifier(tablePath);
        try (PreparedStatement preparedStatement = connection.prepareStatement(describeSql);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                String columnName = StringUtils.trimToEmpty(resultSet.getString(1));
                if (StringUtils.isBlank(columnName)) {
                    continue;
                }
                if ("# Partition Information".equalsIgnoreCase(columnName)) {
                    partitionSection = true;
                    partitionHeader = true;
                    continue;
                }
                if (!partitionSection) {
                    continue;
                }
                if (columnName.startsWith("#")) {
                    if (!partitionHeader) {
                        break;
                    }
                    partitionHeader = false;
                    continue;
                }
                if (partitionHeader) {
                    partitionHeader = false;
                }
                partitionKeys.add(columnName);
            }
        }
        return partitionKeys;
    }
}
