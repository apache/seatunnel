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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.eumerator.OracleChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.offset.RedoLogOffset;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OracleUtils {
    private OracleUtils() {}

    public static RedoLogOffset currentScn(OracleConnection connection) {
        try {
            Scn currentScn = connection.getCurrentScn();
            return new RedoLogOffset(currentScn.longValue());
        } catch (SQLException e) {
            throw new SeaTunnelException(e.getMessage(), e);
        }
    }

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quote(columnName), quote(columnName), quoteSchemaAndTable(tableId));
        return jdbc.queryAndMap(
                minMaxQuery,
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        minMaxQuery));
                    }
                    return SourceRecordUtils.rowToArray(rs, 2);
                });
    }

    public static String quote(String dbOrTableName) {
        return "\"" + dbOrTableName + "\"";
    }

    public static String quoteSchemaAndTable(TableId tableId) {
        StringBuilder quoted = new StringBuilder();

        if (tableId.schema() != null && !tableId.schema().isEmpty()) {
            quoted.append(quote(tableId.schema())).append(".");
        }

        quoted.append(quote(tableId.table()));
        return quoted.toString();
    }

    public static Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        final String analyzeTable =
                String.format(
                        "analyze table %s compute statistics for table",
                        quoteSchemaAndTable(tableId));
        final String rowCountQuery =
                String.format(
                        "select NUM_ROWS from all_tables where TABLE_NAME = '%s'", tableId.table());
        return jdbc.execute(analyzeTable)
                .queryAndMap(
                        rowCountQuery,
                        rs -> {
                            if (!rs.next()) {
                                throw new SQLException(
                                        String.format(
                                                "No result returned after running query [%s]",
                                                rowCountQuery));
                            }
                            return rs.getLong(1);
                        });
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(columnName), quoteSchemaAndTable(tableId), quote(columnName));
        return jdbc.prepareQueryAndMap(
                minQuery,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", minQuery));
                    }
                    return rs.getObject(1);
                });
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                + ") WHERE ROWNUM <= %s",
                        quotedColumn,
                        quotedColumn,
                        quoteSchemaAndTable(tableId),
                        quotedColumn,
                        quotedColumn,
                        chunkSize);
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, includedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    public static Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT %s FROM %s WHERE (%s - (SELECT MIN(%s) FROM %s)) %% %s = 0 ORDER BY %s",
                        quote(columnName),
                        quoteSchemaAndTable(tableId),
                        quote(columnName),
                        quote(columnName),
                        quoteSchemaAndTable(tableId),
                        inverseSamplingRate,
                        quote(columnName));
        return jdbc.queryAndMap(
                minQuery,
                resultSet -> {
                    List<Object> results = new ArrayList<>();
                    while (resultSet.next()) {
                        results.add(resultSet.getObject(1));
                    }
                    return results.toArray();
                });
    }

    public static String buildSplitScanQuery(
            TableId tableId,
            SeaTunnelRowType splitKeyType,
            boolean isFirstSplit,
            boolean isLastSplit) {
        return buildSplitQuery(tableId, splitKeyType, isFirstSplit, isLastSplit, -1, true);
    }

    private static String buildSplitQuery(
            TableId tableId,
            SeaTunnelRowType splitKeyType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int limitSize,
            boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(splitKeyType, sql, " <= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(splitKeyType, sql, " = ?");
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(splitKeyType, sql, " >= ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(splitKeyType, sql, " >= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(splitKeyType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(splitKeyType, sql, " <= ?");
            condition = sql.toString();
        }

        if (isScanningData) {
            return buildSelectWithRowLimits(
                    tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
        } else {
            final String orderBy = String.join(", ", splitKeyType.getFieldNames());
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(splitKeyType),
                    getMaxPrimaryKeyColumnsProjection(splitKeyType),
                    Optional.ofNullable(condition),
                    orderBy);
        }
    }

    private static String buildSelectWithBoundaryRowLimits(
            TableId tableId,
            int limit,
            String projection,
            String maxColumnProjection,
            Optional<String> condition,
            String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(maxColumnProjection);
        sql.append(" FROM (");
        sql.append("SELECT ");
        sql.append(projection);
        sql.append(" FROM ");
        sql.append(quoteSchemaAndTable(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
        sql.append(") T");
        return sql.toString();
    }

    private static void addPrimaryKeyColumnsToCondition(
            SeaTunnelRowType splitKeyType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = Arrays.stream(splitKeyType.getFieldNames()).iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(SeaTunnelRowType splitKeyType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = Arrays.stream(splitKeyType.getFieldNames()).iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next());
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String getMaxPrimaryKeyColumnsProjection(SeaTunnelRowType splitKeyType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = Arrays.stream(splitKeyType.getFieldNames()).iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append("MAX(" + fieldNamesIt.next() + ")");
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String buildSelectWithRowLimits(
            TableId tableId,
            int limit,
            String projection,
            Optional<String> condition,
            Optional<String> orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quoteSchemaAndTable(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        if (orderBy.isPresent()) {
            sql.append(" ORDER BY ").append(orderBy.get());
        }
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }
        return sql.toString();
    }

    public static OracleDatabaseSchema createOracleDatabaseSchema(
            OracleConnectorConfig dbzOracleConfig, OracleConnection oracleConnection) {
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(dbzOracleConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        OracleValueConverters oracleValueConverters =
                new OracleValueConverters(dbzOracleConfig, oracleConnection);
        StreamingAdapter.TableNameCaseSensitivity tableNameCaseSensitivity =
                dbzOracleConfig.getAdapter().getTableNameCaseSensitivity(oracleConnection);
        return new OracleDatabaseSchema(
                dbzOracleConfig,
                oracleValueConverters,
                schemaNameAdjuster,
                topicSelector,
                tableNameCaseSensitivity);
    }

    public static Offset getRedoLogPosition(SourceRecord dataRecord) {
        return getRedoLogPosition(dataRecord.sourceOffset());
    }

    public static RedoLogOffset getRedoLogPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new RedoLogOffset(offsetStrMap);
    }

    public static SeaTunnelRowType getSplitType(
            Table table, OracleSourceConfig oracleSourceConfig) {
        return getSplitType(
                OracleChunkSplitter.getChunkKeyColumn(
                        table, oracleSourceConfig.getChunkKeyColumn()));
    }

    public static SeaTunnelRowType getSplitType(Column splitColumn) {
        return new SeaTunnelRowType(
                new String[] {splitColumn.name()},
                new SeaTunnelDataType<?>[] {OracleTypeUtils.convertFromColumn(splitColumn)});
    }

    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }
}
