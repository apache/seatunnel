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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public abstract class ChunkSplitter implements AutoCloseable, Serializable {

    protected JdbcSourceConfig config;
    protected final JdbcConnectionProvider connectionProvider;
    protected final JdbcDialect jdbcDialect;

    private final int fetchSize;
    private final boolean autoCommit;

    public ChunkSplitter(JdbcSourceConfig config) {
        this.config = config;
        this.autoCommit = config.getJdbcConnectionConfig().isAutoCommit();
        this.fetchSize = config.getFetchSize();
        this.connectionProvider =
                new SimpleJdbcConnectionProvider(config.getJdbcConnectionConfig());
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(), config.getCompatibleMode());
    }

    public static ChunkSplitter create(JdbcSourceConfig config) {
        log.info(
                "Switch to {} chunk splitter", config.isUseDynamicSplitter() ? "dynamic" : "fixed");
        return config.isUseDynamicSplitter()
                ? new DynamicChunkSplitter(config)
                : new FixedChunkSplitter(config);
    }

    @Override
    public synchronized void close() {
        if (connectionProvider != null) {
            connectionProvider.closeConnection();
        }
    }

    public Collection<JdbcSourceSplit> generateSplits(JdbcSourceTable table) throws SQLException {
        log.info("Start splitting table {} into chunks...", table.getTablePath());
        long start = System.currentTimeMillis();

        Collection<JdbcSourceSplit> splits;
        Optional<SeaTunnelRowType> splitKeyOptional = findSplitKey(table);
        if (!splitKeyOptional.isPresent()) {
            JdbcSourceSplit split = createSingleSplit(table);
            splits = Collections.singletonList(split);
        } else {
            if (splitKeyOptional.get().getTotalFields() != 1) {
                throw new UnsupportedOperationException("Currently, only support one split key");
            }
            splits = createSplits(table, splitKeyOptional.get());
        }

        long end = System.currentTimeMillis();
        log.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                table.getTablePath(),
                splits.size(),
                end - start);
        return splits;
    }

    protected abstract Collection<JdbcSourceSplit> createSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKeyType) throws SQLException;

    public PreparedStatement generateSplitStatement(JdbcSourceSplit split) throws SQLException {
        if (split.getSplitKeyName() == null) {
            return createSingleSplitStatement(split);
        }
        return createSplitStatement(split);
    }

    protected abstract PreparedStatement createSplitStatement(JdbcSourceSplit split)
            throws SQLException;

    protected PreparedStatement createPreparedStatement(String sql) throws SQLException {
        Connection connection = getOrEstablishConnection();
        // set autoCommit mode only if it was explicitly configured.
        // keep connection default otherwise.
        if (connection.getAutoCommit() != autoCommit) {
            connection.setAutoCommit(autoCommit);
        }
        if (StringUtils.isNotBlank(config.getWhereConditionClause())) {
            sql = String.format("SELECT * FROM (%s) tmp %s", sql, config.getWhereConditionClause());
        }
        log.debug("Prepared statement: {}", sql);
        return jdbcDialect.creatPreparedStatement(connection, sql, fetchSize);
    }

    protected Connection getOrEstablishConnection() throws SQLException {
        try {
            return connectionProvider.getOrEstablishConnection();
        } catch (ClassNotFoundException e) {
            throw new JdbcConnectorException(
                    CommonErrorCode.CLASS_NOT_FOUND,
                    "JDBC-Class not found. - " + e.getMessage(),
                    e);
        }
    }

    protected JdbcSourceSplit createSingleSplit(JdbcSourceTable table) {

        return new JdbcSourceSplit(
                table.getTablePath(),
                createSplitId(table.getTablePath(), 0),
                table.getQuery(),
                null,
                null,
                null,
                null);
    }

    protected PreparedStatement createSingleSplitStatement(JdbcSourceSplit split)
            throws SQLException {
        String splitQuery = split.getSplitQuery();
        if (StringUtils.isEmpty(splitQuery)) {
            splitQuery =
                    String.format(
                            "SELECT * FROM %s", jdbcDialect.tableIdentifier(split.getTablePath()));
        }
        return createPreparedStatement(splitQuery);
    }

    protected Object queryMin(JdbcSourceTable table, String columnName, Object excludedLowerBound)
            throws SQLException {
        String minQuery;
        columnName = jdbcDialect.quoteIdentifier(columnName);
        if (StringUtils.isNotBlank(table.getQuery())) {
            minQuery =
                    String.format(
                            "SELECT MIN(%s) FROM (%s) tmp WHERE %s > ?",
                            columnName, table.getQuery(), columnName);
        } else {
            minQuery =
                    String.format(
                            "SELECT MIN(%s) FROM %s WHERE %s > ?",
                            columnName,
                            jdbcDialect.tableIdentifier(table.getTablePath()),
                            columnName);
        }

        try (PreparedStatement ps = getOrEstablishConnection().prepareStatement(minQuery)) {
            ps.setObject(1, excludedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                } else {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", minQuery));
                }
            }
        }
    }

    protected Pair<Object, Object> queryMinMax(JdbcSourceTable table, String columnName)
            throws SQLException {
        String sqlQuery;
        columnName = jdbcDialect.quoteIdentifier(columnName);
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MIN(%s), MAX(%s) FROM (%s) tmp",
                            columnName, columnName, table.getQuery());
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MIN(%s), MAX(%s) FROM %s",
                            columnName,
                            columnName,
                            jdbcDialect.tableIdentifier(table.getTablePath()));
        }
        try (Statement stmt = getOrEstablishConnection().createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sqlQuery)) {
                if (resultSet.next()) {
                    Object min = resultSet.getObject(1);
                    Object max = resultSet.getObject(2);
                    return Pair.of(min, max);
                } else {
                    return Pair.of(null, null);
                }
            }
        }
    }

    protected Optional<SeaTunnelRowType> findSplitKey(JdbcSourceTable table) {
        TableSchema schema = table.getCatalogTable().getTableSchema();
        List<Column> columns = schema.getColumns();
        Map<String, Column> columnMap =
                columns.stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName, column -> column, (c1, c2) -> c1));
        if (table.getPartitionColumn() != null) {
            String partitionColumn = table.getPartitionColumn();
            Column column = columnMap.get(partitionColumn);
            if (column == null) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "Partitioned column(%s) don't exist in the table columns",
                                partitionColumn));
            }
            if (!isEvenlySplitColumn(column)) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format("%s is not numeric/string type", partitionColumn));
            }
            return Optional.of(
                    new SeaTunnelRowType(
                            new String[] {partitionColumn},
                            new SeaTunnelDataType[] {column.getDataType()}));
        }

        PrimaryKey pk = schema.getPrimaryKey();
        if (pk != null) {
            for (String pkField : pk.getColumnNames()) {
                Column column = columnMap.get(pkField);
                if (isEvenlySplitColumn(column)) {
                    return Optional.of(
                            new SeaTunnelRowType(
                                    new String[] {pkField},
                                    new SeaTunnelDataType[] {column.getDataType()}));
                }
            }
        }

        List<ConstraintKey> constraintKeys = schema.getConstraintKeys();
        if (constraintKeys != null) {
            List<ConstraintKey> uniqueKeys =
                    constraintKeys.stream()
                            .filter(
                                    constraintKey ->
                                            constraintKey.getConstraintType()
                                                    == ConstraintKey.ConstraintType.UNIQUE_KEY)
                            .collect(Collectors.toList());
            if (!uniqueKeys.isEmpty()) {
                for (ConstraintKey uniqueKey : uniqueKeys) {
                    for (ConstraintKey.ConstraintKeyColumn uniqueKeyColumn :
                            uniqueKey.getColumnNames()) {
                        String uniqueKeyColumnName = uniqueKeyColumn.getColumnName();
                        Column column = columnMap.get(uniqueKeyColumnName);
                        if (isEvenlySplitColumn(column)) {
                            return Optional.of(
                                    new SeaTunnelRowType(
                                            new String[] {uniqueKeyColumnName},
                                            new SeaTunnelDataType[] {column.getDataType()}));
                        }
                    }
                }
            }
        }

        log.warn("No split key found for table {}", table.getTablePath());
        return Optional.empty();
    }

    protected boolean isEvenlySplitColumn(Column splitColumn) {
        return isEvenlySplitColumn(splitColumn.getDataType());
    }

    protected boolean isEvenlySplitColumn(SeaTunnelDataType columnType) {
        // currently, we only support these types.
        switch (columnType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case STRING:
                return true;
            default:
                return false;
        }
    }

    protected String createSplitId(TablePath tablePath, int index) {
        return String.format("%s-%s", tablePath, index);
    }
}
