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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.rowToArray;

/** Utils to prepare MySQL SQL statement. */
@Slf4j
public class MySqlUtils {

    private MySqlUtils() {}

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

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        final String useDatabaseStatement = String.format("USE %s;", quote(tableId.catalog()));
        final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", tableId.table());
        // Otherwise will case this error: Cannot execute without committing because auto-commit is
        // enabled
        jdbc.execute(useDatabaseStatement);
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

    public static Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT %s FROM %s WHERE MOD((%s - (SELECT MIN(%s) FROM %s)), %s) = 0 ORDER BY %s",
                        quote(columnName),
                        quote(tableId),
                        quote(columnName),
                        quote(columnName),
                        quote(tableId),
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

    public static Object[] skipReadAndSortSampleData(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws Exception {
        final String sampleQuery =
                String.format("SELECT %s FROM %s", quote(columnName), quote(tableId));

        Statement stmt = null;
        ResultSet rs = null;

        List<Object> results = new ArrayList<>();
        try {
            stmt =
                    jdbc.connection()
                            .createStatement(
                                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sampleQuery);

            int count = 0;
            while (rs.next()) {
                count++;
                if (count % 100000 == 0) {
                    log.info("Processing row index: {}", count);
                }
                if (count % inverseSamplingRate == 0) {
                    results.add(rs.getObject(1));
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Thread interrupted");
                }
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    log.error("Failed to close ResultSet", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.error("Failed to close Statement", e);
                }
            }
        }
        Object[] resultsArray = results.toArray();
        Arrays.sort(resultsArray);
        return resultsArray;
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

    // ------------------------------------------------------------------------------------------
    // Multi-column composite primary key query methods
    // ------------------------------------------------------------------------------------------

    /** Query the maximum tuple of the next chunk for composite primary key. */
    public static Object[] queryNextChunkMaxMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            int chunkSize,
            Object[] includedLowerBound)
            throws SQLException {
        String orderBy = buildOrderByClause(splitColumns);
        String lowerBoundCondition = buildLexicographicLowerBoundCondition(splitColumns, true);
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1 OFFSET %d",
                        buildColumnList(splitColumns),
                        quote(tableId),
                        lowerBoundCondition,
                        orderBy,
                        chunkSize - 1);
        return jdbc.prepareQueryAndMap(
                query,
                ps ->
                        bindLexicographicLowerBoundParams(
                                ps, 1, includedLowerBound, splitColumns.size()),
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Object[] result = new Object[splitColumns.size()];
                    for (int i = 0; i < splitColumns.size(); i++) {
                        result[i] = rs.getObject(i + 1);
                    }
                    return result;
                });
    }

    /** Query the minimum and maximum tuple of the split columns. */
    public static Object[] queryMinMaxMulti(
            JdbcConnection jdbc, TableId tableId, List<Column> splitColumns) throws SQLException {
        String columnList = buildColumnList(splitColumns);
        String orderByAsc = buildOrderByClause(splitColumns);
        String orderByDesc = buildOrderByClauseDesc(splitColumns);
        String query =
                String.format(
                        "(SELECT %s FROM %s ORDER BY %s LIMIT 1) UNION ALL "
                                + "(SELECT %s FROM %s ORDER BY %s LIMIT 1)",
                        columnList,
                        quote(tableId),
                        orderByAsc,
                        columnList,
                        quote(tableId),
                        orderByDesc);
        return jdbc.queryAndMap(
                query,
                rs -> {
                    Object[] minTuple = null;
                    Object[] maxTuple = null;
                    if (rs.next()) {
                        minTuple = new Object[splitColumns.size()];
                        for (int i = 0; i < splitColumns.size(); i++) {
                            minTuple[i] = rs.getObject(i + 1);
                        }
                    }
                    if (rs.next()) {
                        maxTuple = new Object[splitColumns.size()];
                        for (int i = 0; i < splitColumns.size(); i++) {
                            maxTuple[i] = rs.getObject(i + 1);
                        }
                    }
                    return new Object[] {minTuple, maxTuple};
                });
    }

    /** Query the minimum tuple greater than the excluded lower bound. */
    public static Object[] queryMinMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            Object[] excludedLowerBound)
            throws SQLException {
        String columnList = buildColumnList(splitColumns);
        String orderBy = buildOrderByClause(splitColumns);
        String condition = buildLexicographicLowerBoundCondition(splitColumns, false);
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1",
                        columnList, quote(tableId), condition, orderBy);
        return jdbc.prepareQueryAndMap(
                query,
                ps ->
                        bindLexicographicLowerBoundParams(
                                ps, 1, excludedLowerBound, splitColumns.size()),
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Object[] result = new Object[splitColumns.size()];
                    for (int i = 0; i < splitColumns.size(); i++) {
                        result[i] = rs.getObject(i + 1);
                    }
                    return result;
                });
    }

    // ------------------------------------------------------------------------------------------
    // Split scan query building
    // ------------------------------------------------------------------------------------------

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
        int numColumns = rowType.getTotalFields();

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (numColumns == 1) {
            // Single column: use existing simple per-column predicates
            if (isFirstSplit) {
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
        } else {
            // Multi-column: use lexicographic tuple comparison
            List<Column> columns = new ArrayList<>();
            for (String fieldName : rowType.getFieldNames()) {
                columns.add(Column.editor().name(fieldName).create());
            }
            if (isFirstSplit) {
                final StringBuilder sql = new StringBuilder();
                sql.append(buildLexicographicUpperBoundCondition(columns, true));
                if (isScanningData) {
                    sql.append(" AND ");
                    sql.append(buildLexicographicNotEqualCondition(columns));
                }
                condition = sql.toString();
            } else if (isLastSplit) {
                final StringBuilder sql = new StringBuilder();
                sql.append(buildLexicographicLowerBoundCondition(columns, true));
                condition = sql.toString();
            } else {
                final StringBuilder sql = new StringBuilder();
                sql.append(buildLexicographicLowerBoundCondition(columns, true));
                if (isScanningData) {
                    sql.append(" AND ");
                    sql.append(buildLexicographicNotEqualCondition(columns));
                }
                sql.append(" AND (");
                sql.append(buildLexicographicUpperBoundCondition(columns, true));
                sql.append(")");
                condition = sql.toString();
            }
        }

        if (isScanningData) {
            return buildSelectWithRowLimits(
                    tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
        } else {
            final String orderBy = String.join(", ", rowType.getFieldNames());
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(rowType),
                    getMaxPrimaryKeyColumnsProjection(rowType),
                    Optional.ofNullable(condition),
                    orderBy);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Read table split data statement (parameter binding)
    // ------------------------------------------------------------------------------------------

    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            SeaTunnelRowType splitKeyType,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            int primaryKeyNum = splitKeyType.getTotalFields();
            if (primaryKeyNum == 1) {
                // Single column: existing simple parameter binding
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
            } else {
                // Multi-column: lexicographic comparison parameter binding
                int lbParamCount = getLexicographicLowerBoundParamCount(primaryKeyNum);
                int ubParamCount = getLexicographicUpperBoundParamCount(primaryKeyNum);
                if (isFirstSplit) {
                    bindLexicographicUpperBoundParams(statement, 1, splitEnd, primaryKeyNum);
                    bindLexicographicNotEqualParams(
                            statement, 1 + ubParamCount, splitEnd, primaryKeyNum);
                } else if (isLastSplit) {
                    bindLexicographicLowerBoundParams(statement, 1, splitStart, primaryKeyNum);
                } else {
                    bindLexicographicLowerBoundParams(statement, 1, splitStart, primaryKeyNum);
                    bindLexicographicNotEqualParams(
                            statement, 1 + lbParamCount, splitEnd, primaryKeyNum);
                    bindLexicographicUpperBoundParams(
                            statement, 1 + lbParamCount + primaryKeyNum, splitEnd, primaryKeyNum);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Split key type methods
    // ------------------------------------------------------------------------------------------

    public static SeaTunnelRowType getSplitType(
            Table table, RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new SeaTunnelException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
        }
        // use all primary key columns as split key for composite primary key
        return getSplitType(primaryKeys, dbzConnectorConfig);
    }

    public static SeaTunnelRowType getSplitType(
            List<Column> splitColumns, RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        String[] fieldNames = new String[splitColumns.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[splitColumns.size()];
        for (int i = 0; i < splitColumns.size(); i++) {
            fieldNames[i] = splitColumns.get(i).name();
            fieldTypes[i] =
                    MySqlTypeUtils.convertFromColumn(splitColumns.get(i), dbzConnectorConfig);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
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

    public static SeaTunnelRowType getSplitType(
            Column splitColumn, RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        return new SeaTunnelRowType(
                new String[] {splitColumn.name()},
                new SeaTunnelDataType<?>[] {
                    MySqlTypeUtils.convertFromColumn(splitColumn, dbzConnectorConfig)
                });
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

    /** Get all primary key columns as split columns for composite primary key support. */
    public static List<Column> getSplitColumns(Table table) {
        return table.primaryKeyColumns();
    }

    // ------------------------------------------------------------------------------------------
    // Quote helpers
    // ------------------------------------------------------------------------------------------

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('`');
    }

    // ------------------------------------------------------------------------------------------
    // SQL clause builders for composite keys
    // ------------------------------------------------------------------------------------------

    private static String buildColumnList(List<Column> columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(columns.get(i).name()));
        }
        return sb.toString();
    }

    private static String buildOrderByClause(List<Column> columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(columns.get(i).name())).append(" ASC");
        }
        return sb.toString();
    }

    private static String buildOrderByClauseDesc(List<Column> columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(columns.get(i).name())).append(" DESC");
        }
        return sb.toString();
    }

    /**
     * Build a lexicographic lower bound condition for composite keys. For (col1, col2, ..., colN)
     * >= (v1, v2, ..., vN): col1 > v1 OR (col1 = v1 AND col2 > v2) OR ... OR (col1 = v1 AND ... AND
     * colN >= vN)
     */
    private static String buildLexicographicLowerBoundCondition(
            List<Column> columns, boolean inclusive) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append("(");
            for (int j = 0; j < i; j++) {
                sb.append(quote(columns.get(j).name())).append(" = ? AND ");
            }
            boolean isLast = (i == columns.size() - 1);
            String operator;
            if (isLast) {
                operator = inclusive ? " >= ?" : " > ?";
            } else {
                operator = " > ?";
            }
            sb.append(quote(columns.get(i).name())).append(operator);
            sb.append(")");
        }
        return sb.toString();
    }

    /**
     * Build a lexicographic upper bound condition for composite keys. For (col1, col2, ..., colN)
     * <= (v1, v2, ..., vN): col1 < v1 OR (col1 = v1 AND col2 < v2) OR ... OR (col1 = v1 AND ... AND
     * colN <= vN)
     */
    private static String buildLexicographicUpperBoundCondition(
            List<Column> columns, boolean inclusive) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append("(");
            for (int j = 0; j < i; j++) {
                sb.append(quote(columns.get(j).name())).append(" = ? AND ");
            }
            boolean isLast = (i == columns.size() - 1);
            String operator;
            if (isLast) {
                operator = inclusive ? " <= ?" : " < ?";
            } else {
                operator = " < ?";
            }
            sb.append(quote(columns.get(i).name())).append(operator);
            sb.append(")");
        }
        return sb.toString();
    }

    private static String buildLexicographicNotEqualCondition(List<Column> columns) {
        StringBuilder sb = new StringBuilder("NOT (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            sb.append(quote(columns.get(i).name())).append(" = ?");
        }
        sb.append(")");
        return sb.toString();
    }

    private static int getLexicographicLowerBoundParamCount(int numColumns) {
        return 2 * numColumns - 1;
    }

    private static int getLexicographicUpperBoundParamCount(int numColumns) {
        return 2 * numColumns - 1;
    }

    /** Bind lexicographic lower bound parameters to a PreparedStatement. */
    private static void bindLexicographicLowerBoundParams(
            PreparedStatement ps, int startIdx, Object[] values, int numColumns)
            throws SQLException {
        int idx = startIdx;
        for (int i = 0; i < numColumns; i++) {
            if (i < numColumns - 1) {
                ps.setObject(idx++, values[i]);
            }
            ps.setObject(idx++, values[i]);
        }
    }

    private static void bindLexicographicUpperBoundParams(
            PreparedStatement ps, int startIdx, Object[] values, int numColumns)
            throws SQLException {
        bindLexicographicLowerBoundParams(ps, startIdx, values, numColumns);
    }

    private static void bindLexicographicNotEqualParams(
            PreparedStatement ps, int startIdx, Object[] values, int numColumns)
            throws SQLException {
        for (int i = 0; i < numColumns; i++) {
            ps.setObject(startIdx + i, values[i]);
        }
    }

    // ------------------------------------------------------------------------------------------
    // SQL statement building helpers
    // ------------------------------------------------------------------------------------------

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        // Add MySQL metadata locks to prevent modification of table structure.
        connection.setAutoCommit(false);
        final PreparedStatement statement =
                connection.prepareStatement(
                        sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize <= 0) {
            statement.setFetchSize(Integer.MIN_VALUE);
        } else {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    private static void addPrimaryKeyColumnsToCondition(
            SeaTunnelRowType rowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = Arrays.stream(rowType.getFieldNames()).iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(quote(fieldNamesIt.next())).append(predicate);
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
