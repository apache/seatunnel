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
        try (PreparedStatement preparedStatement =
                        conn.prepareStatement(limitMetadataQuery(query));
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

    /**
     * Adds a top-level metadata limit without introducing a subquery alias or moving Hive CTEs.
     *
     * <p>This small lexical scan preserves quoted identifiers, literals and comments. Queries with
     * an existing top-level LIMIT, multiple statements or incomplete syntax are left unchanged.
     * Package visibility permits direct regression coverage without reflection.
     */
    static String limitMetadataQuery(String sql) {
        int depth = 0;
        int lastTokenEnd = 0;
        boolean query = false;
        List<Integer> terminators = new ArrayList<>();
        for (int index = 0; index < sql.length(); ) {
            char current = sql.charAt(index);
            if (Character.isWhitespace(current)) {
                index++;
                continue;
            }
            if (sql.startsWith("--", index)) {
                index += 2;
                while (index < sql.length()
                        && sql.charAt(index) != '\n'
                        && sql.charAt(index) != '\r') {
                    index++;
                }
                continue;
            }
            if (sql.startsWith("/*", index)) {
                int end = sql.indexOf("*/", index + 2);
                if (end < 0) {
                    return sql;
                }
                index = end + 2;
                continue;
            }
            if (current == ';') {
                if (depth != 0) {
                    return sql;
                }
                terminators.add(index++);
                continue;
            }
            // A token after a statement terminator means this is not a single metadata query.
            if (!terminators.isEmpty()) {
                return sql;
            }
            if (Character.isJavaIdentifierStart(current)) {
                int start = index++;
                while (index < sql.length() && Character.isJavaIdentifierPart(sql.charAt(index))) {
                    index++;
                }
                String token = sql.substring(start, index);
                if (!query) {
                    if (!"SELECT".equalsIgnoreCase(token) && !"WITH".equalsIgnoreCase(token)) {
                        return sql;
                    }
                    query = true;
                } else if (depth == 0
                        && ("LIMIT".equalsIgnoreCase(token)
                                || "INSERT".equalsIgnoreCase(token)
                                || "UPDATE".equalsIgnoreCase(token)
                                || "DELETE".equalsIgnoreCase(token)
                                || "MERGE".equalsIgnoreCase(token)
                                || "CREATE".equalsIgnoreCase(token)
                                || "DROP".equalsIgnoreCase(token)
                                || "ALTER".equalsIgnoreCase(token))) {
                    // A CTE can precede a data-changing statement rather than a SELECT.
                    return sql;
                }
            } else if (current == '\'' || current == '"' || current == '`') {
                if (!query) {
                    return sql;
                }
                char quote = current;
                boolean closed = false;
                for (index++; index < sql.length(); index++) {
                    if (sql.charAt(index) == '\\') {
                        index++;
                    } else if (sql.charAt(index) == quote) {
                        if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                            index++;
                        } else {
                            index++;
                            closed = true;
                            break;
                        }
                    }
                }
                if (!closed) {
                    return sql;
                }
            } else {
                if (!query) {
                    return sql;
                }
                if (current == '(') {
                    depth++;
                } else if (current == ')' && --depth < 0) {
                    return sql;
                }
                index++;
            }
            lastTokenEnd = index;
        }
        if (!query || depth != 0) {
            return sql;
        }
        StringBuilder limitedSql = new StringBuilder(sql);
        for (int index = terminators.size() - 1; index >= 0; index--) {
            limitedSql.deleteCharAt(terminators.get(index));
        }
        // Insert before trailing comments; line comments must not consume the new LIMIT clause.
        return limitedSql.insert(lastTokenEnd, "\nLIMIT 1\n").toString();
    }
}
