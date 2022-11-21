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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.rowToArray;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utils to prepare MySQL SQL statement.
 */
public class MySqlUtils {

    private MySqlUtils() {
    }

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
        throws SQLException {
        final String minMaxQuery =
            String.format(
                "SELECT MIN(%s), MAX(%s) FROM %s",
                quote(columnName), quote(columnName), quote(tableId));
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
                return rowToArray(rs, 2);
            });
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
        throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        final String useDatabaseStatement = String.format("USE %s;", quote(tableId.catalog()));
        final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", tableId.table());
        jdbc.executeWithoutCommitting(useDatabaseStatement);
        return jdbc.queryAndMap(
            rowCountQuery,
            rs -> {
                if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                    throw new SQLException(
                        String.format(
                            "No result returned after running query [%s]",
                            rowCountQuery));
                }
                return rs.getLong(5);
            });
    }

    public static Object queryMin(
        JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
        throws SQLException {
        final String minQuery =
            String.format(
                "SELECT MIN(%s) FROM %s WHERE %s > ?",
                quote(columnName), quote(tableId), quote(columnName));
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
                    + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                    + ") AS T",
                quotedColumn,
                quotedColumn,
                quote(tableId),
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

    public static String buildSplitScanQuery(
        TableId tableId, SeaTunnelRowType rowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, rowType, isFirstSplit, isLastSplit, -1, true);
    }

    private static String buildSplitQuery(
        TableId tableId,
        SeaTunnelRowType rowType,
        boolean isFirstSplit,
        boolean isLastSplit,
        int limitSize,
        boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " <= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(rowType, sql, " = ?");
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " >= ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " >= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(rowType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(rowType, sql, " <= ?");
            condition = sql.toString();
        }

        if (isScanningData) {
            return buildSelectWithRowLimits(
                tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
        } else {
            final String orderBy =
                String.join(", ", rowType.getFieldNames());
            return buildSelectWithBoundaryRowLimits(
                tableId,
                limitSize,
                getPrimaryKeyColumnsProjection(rowType),
                getMaxPrimaryKeyColumnsProjection(rowType),
                Optional.ofNullable(condition),
                orderBy);
        }
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

    public static SeaTunnelRowType getSplitType(Table table) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new SeaTunnelException(
                String.format(
                    "Incremental snapshot for tables requires primary key,"
                        + " but table %s doesn't have primary key.",
                    table.id()));
        }

        // use first field in primary key as the split key
        return getSplitType(primaryKeys.get(0));
    }

    /**
     * Creates a new {@link MySqlDatabaseSchema} to monitor the latest MySql database schemas.
     */
    public static MySqlDatabaseSchema createMySqlDatabaseSchema(
        MySqlConnectorConfig dbzMySqlConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(dbzMySqlConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(dbzMySqlConfig);
        return new MySqlDatabaseSchema(
            dbzMySqlConfig,
            valueConverters,
            topicSelector,
            schemaNameAdjuster,
            isTableIdCaseSensitive);
    }

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig dbzMySqlConfig) {
        TemporalPrecisionMode timePrecisionMode = dbzMySqlConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = dbzMySqlConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
            dbzMySqlConfig
                .getConfig()
                .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
            MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
            bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
            dbzMySqlConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
            decimalMode,
            timePrecisionMode,
            bigIntUnsignedMode,
            dbzMySqlConfig.binaryHandlingMode(),
            timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
            MySqlValueConverters::defaultParsingErrorHandler);
    }

    public static BinlogOffset getBinlogPosition(SourceRecord dataRecord) {
        return getBinlogPosition(dataRecord.sourceOffset());
    }

    public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new BinlogOffset(offsetStrMap);
    }

    public static SeaTunnelRowType getSplitType(Column splitColumn) {
        return new SeaTunnelRowType(new String[]{splitColumn.name()},
            new SeaTunnelDataType<?>[]{MySqlTypeUtils.convertFromColumn(splitColumn)});
    }

    public static Column getSplitColumn(Table table) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new SeaTunnelException(
                String.format(
                    "Incremental snapshot for tables requires primary key,"
                        + " but table %s doesn't have primary key.",
                    table.id()));
        }

        // use first field in primary key as the split key
        return primaryKeys.get(0);
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('`');
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
        throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private static void addPrimaryKeyColumnsToCondition(
        SeaTunnelRowType rowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = Arrays.stream(rowType.getFieldNames()).iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(SeaTunnelRowType rowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = Arrays.stream(rowType.getFieldNames()).iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next());
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String getMaxPrimaryKeyColumnsProjection(SeaTunnelRowType rowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = Arrays.stream(rowType.getFieldNames()).iterator();
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
        sql.append(quotedTableIdString(tableId));
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
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
        sql.append(") T");
        return sql.toString();
    }

    private static String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString('`');
    }
}
