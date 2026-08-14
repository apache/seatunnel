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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ObjectUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_CEILING;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class DynamicChunkSplitter extends ChunkSplitter {

    private final boolean useCharsetBasedStringSplitter =
            StringSplitMode.CHARSET_BASED.equals(config.getStringSplitMode());

    public DynamicChunkSplitter(JdbcSourceConfig config) {
        super(config);
    }

    @Override
    protected Collection<JdbcSourceSplit> createSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKey) throws Exception {
        return createDynamicSplits(table, splitKey);
    }

    @Override
    protected PreparedStatement createSplitStatement(JdbcSourceSplit split, TableSchema schema)
            throws SQLException {
        return createDynamicSplitStatement(split, schema);
    }

    private Collection<JdbcSourceSplit> createDynamicSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKey) throws Exception {
        // Composite primary key: split on the full key tuple with tuple-ordered boundaries.
        if (splitKey.getTotalFields() > 1) {
            return createCompositeDynamicSplits(table, splitKey);
        }

        String splitKeyName = splitKey.getFieldNames()[0];
        SeaTunnelDataType splitKeyType = splitKey.getFieldType(0);
        if (SqlType.STRING.equals(splitKeyType.getSqlType())
                && config.getStringSplitStrategy() != null) {
            return createStringStrategySplits(table, splitKeyName, splitKeyType);
        }

        List<ChunkRange> chunks = splitTableIntoChunks(table, splitKeyName, splitKeyType);

        List<JdbcSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            JdbcSourceSplit split =
                    new JdbcSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            table.getQuery(),
                            splitKeyName,
                            splitKeyType,
                            chunk.getChunkStart(),
                            chunk.getChunkEnd());
            splits.add(split);
        }
        return splits;
    }

    private static final String COMPOSITE_KEY_SEPARATOR = ",";

    /**
     * Split a table on the full composite key tuple. Boundaries are tuple-ordered ((start, end] per
     * chunk, matching {@link #buildCompositeCondition}), so composite-key tables whose first column
     * repeats heavily still split into balanced chunks via the remaining key columns.
     *
     * <p><b>Performance trade-off:</b> unlike the single-column paths which compute chunk
     * boundaries arithmetically (closed-form evenly/unevenly sized chunks), this path walks the
     * boundary with one database round trip per chunk ({@link #queryNextChunkMaxComposite} scans at
     * most {@code split.size} rows per call). For a large table with many chunks this is more round
     * trips than the single-column arithmetic path, in exchange for correct, balanced chunks on
     * composite keys where arithmetic is impossible (the first column has low cardinality).
     * Boundary queries are index-friendly (expanded OR/AND), so each round trip is cheap.
     */
    private Collection<JdbcSourceSplit> createCompositeDynamicSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKey) throws SQLException {
        String[] columns = splitKey.getFieldNames();
        Object[][] minMax = queryMinMaxComposite(table, columns);
        Object[] min = minMax[0];
        Object[] max = minMax[1];
        if (min == null || max == null) {
            // empty table, return a single full-table split
            return Collections.singletonList(createCompositeSplit(table, 0, splitKey, null, null));
        }

        // first chunk end
        Object[] firstEnd = queryNextChunkMaxComposite(table, columns, config.getSplitSize(), min);
        if (firstEnd == null || compareArrays(firstEnd, max) >= 0) {
            // all data fits in one chunk
            return Collections.singletonList(createCompositeSplit(table, 0, splitKey, null, null));
        }

        List<JdbcSourceSplit> splits = new ArrayList<>();
        Object[] chunkStart = null;
        Object[] chunkEnd = firstEnd;
        int index = 0;
        while (chunkEnd != null && compareArrays(chunkEnd, max) < 0) {
            splits.add(createCompositeSplit(table, index++, splitKey, chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            chunkEnd =
                    queryNextChunkMaxComposite(table, columns, config.getSplitSize(), chunkStart);
            if (chunkEnd != null && Arrays.equals(chunkStart, chunkEnd)) {
                // we don't allow equal chunk start and end,
                // should query the next one larger than chunkEnd
                chunkEnd = queryMinComposite(table, columns, chunkEnd);
            }
        }
        // add the ending split
        splits.add(createCompositeSplit(table, index, splitKey, chunkStart, null));
        return splits;
    }

    private JdbcSourceSplit createCompositeSplit(
            JdbcSourceTable table,
            int index,
            SeaTunnelRowType splitKey,
            Object[] start,
            Object[] end) {
        String joinedNames = String.join(COMPOSITE_KEY_SEPARATOR, splitKey.getFieldNames());
        return new JdbcSourceSplit(
                table.getTablePath(),
                createSplitId(table.getTablePath(), index),
                table.getQuery(),
                joinedNames,
                splitKey,
                start,
                end);
    }

    /**
     * Queries the tuple-ordered minimum and maximum of the composite key using {@code ORDER BY ...
     * ASC/DESC LIMIT 1} (index-friendly, works for any comparable type and avoids MIN()/MAX()
     * aggregates which would scan without index benefit).
     *
     * @return {@code [minRow, maxRow]}, each element null when the table is empty
     */
    private Object[][] queryMinMaxComposite(JdbcSourceTable table, String[] columns)
            throws SQLException {
        StringBuilder selectCols = new StringBuilder();
        StringBuilder orderAsc = new StringBuilder();
        StringBuilder orderDesc = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                selectCols.append(", ");
                orderAsc.append(", ");
                orderDesc.append(", ");
            }
            String quoted = jdbcDialect.quoteIdentifier(columns[i]);
            selectCols.append(quoted);
            orderAsc.append(quoted).append(" ASC");
            orderDesc.append(quoted).append(" DESC");
        }

        // Composite splitting and a custom query are mutually exclusive today: findSplitKey
        // returns empty (single split) or a single-column key before ever reaching the composite
        // branch whenever table.getQuery() is set, so the from-clause is always the table path.
        String fromClause = jdbcDialect.tableIdentifier(table.getTablePath());

        String minQuery =
                "SELECT "
                        + selectCols
                        + " FROM "
                        + fromClause
                        + " ORDER BY "
                        + orderAsc
                        + jdbcDialect.getLimitClause(1);
        String maxQuery =
                "SELECT "
                        + selectCols
                        + " FROM "
                        + fromClause
                        + " ORDER BY "
                        + orderDesc
                        + jdbcDialect.getLimitClause(1);

        Connection conn = getOrEstablishConnection();
        Object[] minRow = executeRowQuery(conn, minQuery, columns.length);
        Object[] maxRow = executeRowQuery(conn, maxQuery, columns.length);
        return new Object[][] {minRow, maxRow};
    }

    /**
     * Walks the composite key boundary: returns the last row of the first {@code chunkSize} rows
     * strictly greater than {@code includedLowerBound} (tuple-ordered), using an expanded OR/AND
     * condition so the composite index is used instead of a row-constructor comparison. Each call
     * scans at most {@code chunkSize} rows.
     *
     * @return the next chunk-end tuple, or null when no row is greater than the bound
     */
    private Object[] queryNextChunkMaxComposite(
            JdbcSourceTable table, String[] columns, int chunkSize, Object[] includedLowerBound)
            throws SQLException {
        StringBuilder columnList = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                columnList.append(", ");
                orderBy.append(", ");
            }
            String quoted = jdbcDialect.quoteIdentifier(columns[i]);
            columnList.append(quoted);
            orderBy.append(quoted);
        }

        // Expanded OR condition for index usage:
        // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c >= ?)
        StringBuilder where = new StringBuilder("(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                where.append(" OR ");
            }
            where.append("(");
            for (int j = 0; j <= i; j++) {
                if (j > 0) {
                    where.append(" AND ");
                }
                String quoted = jdbcDialect.quoteIdentifier(columns[j]);
                if (j < i) {
                    where.append(quoted).append(" = ?");
                } else {
                    String op = (i == columns.length - 1) ? ">=" : ">";
                    where.append(quoted).append(" ").append(op).append(" ?");
                }
            }
            where.append(")");
        }
        where.append(")");

        // Composite splitting and a custom query are mutually exclusive today: findSplitKey
        // returns empty (single split) or a single-column key before ever reaching the composite
        // branch whenever table.getQuery() is set, so the from-clause is always the table path.
        String fromClause = jdbcDialect.tableIdentifier(table.getTablePath());

        String sql =
                "SELECT "
                        + columnList
                        + " FROM "
                        + fromClause
                        + " WHERE "
                        + where
                        + " ORDER BY "
                        + orderBy
                        + " ASC"
                        // Fetch only the chunkSize-th row (the next chunk boundary) instead of
                        // transferring all chunkSize rows; the server still scans them to position
                        // the cursor, but only one row crosses the wire.
                        + jdbcDialect.getOffsetLimitClause(chunkSize - 1, 1);

        Connection conn = getOrEstablishConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // Bind: v1, v1,v2, v1,v2,v3 ... (cumulative per OR branch)
            int paramIndex = 1;
            for (int i = 0; i < columns.length; i++) {
                for (int j = 0; j <= i; j++) {
                    ps.setObject(paramIndex++, includedLowerBound[j]);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Object[] row = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    return row;
                }
                return null;
            }
        }
    }

    /**
     * Returns the first tuple strictly greater than {@code excludedLowerBound} (tuple-ordered).
     * Used to advance past a duplicated chunk boundary value so chunks never degenerate into
     * zero-row ranges.
     *
     * @return the next greater tuple, or null when none exists
     */
    private Object[] queryMinComposite(
            JdbcSourceTable table, String[] columns, Object[] excludedLowerBound)
            throws SQLException {
        StringBuilder columnList = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                columnList.append(", ");
                orderBy.append(", ");
            }
            String quoted = jdbcDialect.quoteIdentifier(columns[i]);
            columnList.append(quoted);
            orderBy.append(quoted);
        }

        // Composite splitting and a custom query are mutually exclusive today: findSplitKey
        // returns empty (single split) or a single-column key before ever reaching the composite
        // branch whenever table.getQuery() is set, so the from-clause is always the table path.
        String fromClause = jdbcDialect.tableIdentifier(table.getTablePath());

        // Expanded OR/AND form of (col1, col2, ...) > (?, ?, ...) without row-value-constructor
        // syntax (portable across dialects, e.g. SQL Server).
        String sql =
                "SELECT "
                        + columnList
                        + " FROM "
                        + fromClause
                        + " WHERE "
                        + buildExpandedTupleCondition(columns, ">", ">")
                        + " ORDER BY "
                        + orderBy
                        + " ASC"
                        + jdbcDialect.getLimitClause(1);

        Connection conn = getOrEstablishConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int paramIndex = 1;
            for (int i = 0; i < columns.length; i++) {
                for (int j = 0; j <= i; j++) {
                    ps.setObject(paramIndex++, excludedLowerBound[j]);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Object[] result = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        result[i] = rs.getObject(i + 1);
                    }
                    return result;
                }
                return null;
            }
        }
    }

    /**
     * Executes a simple row query (no parameters) and returns the first row as an Object array, or
     * null when the result set is empty.
     */
    private Object[] executeRowQuery(Connection conn, String sql, int columnCount)
            throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                return row;
            }
            return null;
        }
    }

    /**
     * Lexicographically compares two composite-key tuples element-wise using {@link
     * ObjectUtils#compare}, returning a negative/zero/positive value.
     */
    private int compareArrays(Object[] a, Object[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = ObjectUtils.compare(a[i], b[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    private PreparedStatement createDynamicSplitStatement(JdbcSourceSplit split, TableSchema schema)
            throws SQLException {
        if (isHashStringSplit(split)) {
            return createStringColumnSplitStatement(split);
        }
        String splitQuery = createDynamicSplitQuerySQL(split, schema);
        PreparedStatement statement = createPreparedStatement(splitQuery);
        prepareDynamicSplitStatement(statement, split);
        return statement;
    }

    private boolean isHashStringSplit(JdbcSourceSplit split) {
        return SqlType.STRING.equals(split.getSplitKeyType().getSqlType())
                && split.getSplitStart() instanceof Integer
                && split.getSplitEnd() == null;
    }

    private PreparedStatement createStringColumnSplitStatement(JdbcSourceSplit split)
            throws SQLException {
        PreparedStatement statement = createPreparedStatement(split.getSplitQuery());
        statement.setInt(1, (Integer) split.getSplitStart());
        return statement;
    }

    private Collection<JdbcSourceSplit> createStringStrategySplits(
            JdbcSourceTable table, String splitKeyName, SeaTunnelDataType splitKeyType)
            throws Exception {
        StringSplitStrategy strategy = resolveStringSplitStrategy(table, splitKeyName);
        switch (strategy) {
            case NONE:
                return Collections.singletonList(
                        createSingleStringSplit(table, splitKeyName, splitKeyType));
            case HASH:
                if (jdbcDialect.supportHashSplitter()) {
                    return createStringColumnSplits(
                            table, splitKeyName, splitKeyType, config.getSplitSize());
                }
                return Collections.singletonList(
                        createSingleStringSplit(table, splitKeyName, splitKeyType));
            case RANGE:
                if (config.getStringSplitStrategy() == StringSplitStrategy.AUTO) {
                    try {
                        return createStringRangeSplits(table, splitKeyName, splitKeyType);
                    } catch (Exception e) {
                        log.warn(
                                "Range string split failed for table {}, fallback to hash split",
                                table.getTablePath(),
                                e);
                        if (jdbcDialect.supportHashSplitter()) {
                            return createStringColumnSplits(
                                    table, splitKeyName, splitKeyType, config.getSplitSize());
                        }
                        return Collections.singletonList(
                                createSingleStringSplit(table, splitKeyName, splitKeyType));
                    }
                }
                return createStringRangeSplits(table, splitKeyName, splitKeyType);
            default:
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Unsupported string split strategy: " + config.getStringSplitStrategy());
        }
    }

    private JdbcSourceSplit createSingleStringSplit(
            JdbcSourceTable table, String splitKeyName, SeaTunnelDataType splitKeyType) {
        return new JdbcSourceSplit(
                table.getTablePath(),
                createSplitId(table.getTablePath(), 0),
                table.getQuery(),
                splitKeyName,
                splitKeyType,
                null,
                null);
    }

    @VisibleForTesting
    Collection<JdbcSourceSplit> createStringColumnSplits(
            JdbcSourceTable table,
            String splitKeyName,
            SeaTunnelDataType splitKeyType,
            int chunkSize)
            throws SQLException {
        log.info("Use string hash chunks for table {}", table.getTablePath());
        long approximateRowCnt = queryApproximateRowCnt(table);
        int shardCount = Math.max((int) (approximateRowCnt / Math.max(chunkSize, 1)) + 1, 1);
        List<JdbcSourceSplit> splits = new ArrayList<>(shardCount);
        Column column =
                table.getCatalogTable().getTableSchema().getColumns().stream()
                        .filter(c -> c.getName().equals(splitKeyName))
                        .findAny()
                        .get();
        for (int i = 0; i < shardCount; i++) {
            String splitQuery;
            if (StringUtils.isNotBlank(table.getQuery())) {
                splitQuery =
                        String.format(
                                "SELECT * FROM (%s) st_jdbc_splitter WHERE %s = ?",
                                applyUserWhereCondition(table.getQuery()),
                                jdbcDialect.hashModForField(
                                        column.getSourceType(), splitKeyName, shardCount));
            } else if (StringUtils.isNotBlank(config.getWhereConditionClause())) {
                String userQuery =
                        String.format(
                                "SELECT * FROM %s",
                                jdbcDialect.tableIdentifier(table.getTablePath()));
                splitQuery =
                        String.format(
                                "SELECT * FROM (%s) st_jdbc_splitter WHERE %s = ?",
                                applyUserWhereCondition(userQuery),
                                jdbcDialect.hashModForField(
                                        column.getSourceType(), splitKeyName, shardCount));
            } else {
                splitQuery =
                        String.format(
                                "SELECT * FROM %s WHERE %s = ?",
                                jdbcDialect.tableIdentifier(table.getTablePath()),
                                jdbcDialect.hashModForField(
                                        column.getSourceType(), splitKeyName, shardCount));
            }

            splits.add(
                    new JdbcSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            splitQuery,
                            splitKeyName,
                            splitKeyType,
                            i,
                            null));
        }
        return splits;
    }

    private Collection<JdbcSourceSplit> createStringRangeSplits(
            JdbcSourceTable table, String splitKeyName, SeaTunnelDataType splitKeyType)
            throws SQLException {
        Pair<Object, Object> splitColumnRange = queryMinMax(table, splitKeyName);
        String min =
                splitColumnRange.getLeft() == null ? null : splitColumnRange.getLeft().toString();
        String max =
                splitColumnRange.getRight() == null ? null : splitColumnRange.getRight().toString();
        if (min == null || max == null || min.equals(max)) {
            return Collections.singletonList(
                    createSingleStringSplit(table, splitKeyName, splitKeyType));
        }

        long approximateRowCnt = queryApproximateRowCnt(table);
        int shardCount =
                Math.max((int) (approximateRowCnt / Math.max(config.getSplitSize(), 1)) + 1, 1);
        String[] rangeResult = AsciiStringRangeSplitter.split(min, max, shardCount);
        List<JdbcSourceSplit> splits = new ArrayList<>(rangeResult.length - 1);
        for (int i = 0; i < rangeResult.length - 1; i++) {
            splits.add(
                    new JdbcSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            table.getQuery(),
                            splitKeyName,
                            splitKeyType,
                            i == 0 ? null : rangeResult[i],
                            i == rangeResult.length - 2 ? null : rangeResult[i + 1]));
        }
        return splits;
    }

    private List<ChunkRange> splitTableIntoChunks(
            JdbcSourceTable table, String splitColumnName, SeaTunnelDataType splitColumnType)
            throws Exception {
        Pair<Object, Object> minMax = queryMinMax(table, splitColumnName);
        Object min = minMax.getLeft();
        Object max = minMax.getRight();
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        int chunkSize = config.getSplitSize();

        switch (splitColumnType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
                return evenlyColumnSplitChunks(table, splitColumnName, min, max, chunkSize);
            case STRING:
                if (useCharsetBasedStringSplitter) {
                    return charsetBasedColumnSplitChunks(
                            table, splitColumnName, min, max, chunkSize);
                } else {
                    return evenlyColumnSplitChunks(table, splitColumnName, min, max, chunkSize);
                }
            case DATE:
                return dateColumnSplitChunks(table, splitColumnName, min, max, chunkSize);
            default:
                throw CommonError.unsupportedDataType(
                        "JDBC", splitColumnType.getSqlType().toString(), splitColumnName);
        }
    }

    private List<ChunkRange> charsetBasedColumnSplitChunks(
            JdbcSourceTable table,
            String splitColumnName,
            Object objectMin,
            Object objectMax,
            int chunkSize)
            throws Exception {
        boolean paddingAtEnd = true;
        boolean isCaseInsensitive = false;
        String collationSequence =
                jdbcDialect.getCollationSequence(
                        getOrEstablishConnection(), config.getStringSplitModeCollate());
        if (collationSequence.matches(".*[aA][Aa].*")) {
            isCaseInsensitive = true;
            collationSequence = filterOutUppercase(collationSequence);
        }
        int radix = collationSequence.length() + 1;
        String minStr = objectMin.toString();
        String maxStr = objectMax.toString();
        int maxLength = Math.max(minStr.length(), maxStr.length());
        BigInteger min =
                CollationBasedSplitter.encodeStringToNumericRange(
                        minStr,
                        maxLength,
                        paddingAtEnd,
                        isCaseInsensitive,
                        collationSequence,
                        radix);
        BigInteger max =
                CollationBasedSplitter.encodeStringToNumericRange(
                        maxStr,
                        maxLength,
                        paddingAtEnd,
                        isCaseInsensitive,
                        collationSequence,
                        radix);
        TablePath tablePath = table.getTablePath();
        double distributionFactorUpper = config.getSplitEvenDistributionFactorUpperBound();
        double distributionFactorLower = config.getSplitEvenDistributionFactorLowerBound();
        int sampleShardingThreshold = config.getSplitSampleShardingThreshold();
        boolean sampleShardingAllow = config.isSplitSampleShardingAllow();
        log.info(
                "Splitting table {} into chunks, split column: {}, min: {}, max: {}, chunk size: {}, "
                        + "distribution factor upper: {}, distribution factor lower: {}, sample sharding threshold: {},"
                        + " sample sharding enable: {}",
                tablePath,
                splitColumnName,
                min,
                max,
                chunkSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                sampleShardingAllow);

        long approximateRowCnt = queryApproximateRowCnt(table);

        double distributionFactor =
                calculateDistributionFactor(tablePath, min, max, approximateRowCnt);

        boolean dataIsEvenlyDistributed =
                ObjectUtils.doubleCompare(distributionFactor, distributionFactorLower) >= 0
                        && ObjectUtils.doubleCompare(distributionFactor, distributionFactorUpper)
                                <= 0;

        if (dataIsEvenlyDistributed) {
            // the minimum dynamic chunk size is at least 1
            final int dynamicChunkSize = Math.max((int) (distributionFactor * chunkSize), 1);
            return splitStringEvenlySizedChunks(
                    tablePath,
                    min,
                    max,
                    approximateRowCnt,
                    chunkSize,
                    dynamicChunkSize,
                    maxLength,
                    radix,
                    collationSequence);
        } else {
            return getChunkRangesWithUnevenlyData(
                    table,
                    splitColumnName,
                    min,
                    max,
                    chunkSize,
                    tablePath,
                    sampleShardingThreshold,
                    sampleShardingAllow,
                    approximateRowCnt);
        }
    }

    private List<ChunkRange> evenlyColumnSplitChunks(
            JdbcSourceTable table, String splitColumnName, Object min, Object max, int chunkSize)
            throws Exception {
        TablePath tablePath = table.getTablePath();
        double distributionFactorUpper = config.getSplitEvenDistributionFactorUpperBound();
        double distributionFactorLower = config.getSplitEvenDistributionFactorLowerBound();
        int sampleShardingThreshold = config.getSplitSampleShardingThreshold();
        boolean sampleShardingAllow = config.isSplitSampleShardingAllow();

        log.info(
                "Splitting table {} into chunks, split column: {}, min: {}, max: {}, chunk size: {}, "
                        + "distribution factor upper: {}, distribution factor lower: {}, sample sharding threshold: {},"
                        + " sample sharding enable: {}",
                tablePath,
                splitColumnName,
                min,
                max,
                chunkSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                sampleShardingAllow);

        long approximateRowCnt = queryApproximateRowCnt(table);
        double distributionFactor =
                calculateDistributionFactor(tablePath, min, max, approximateRowCnt);

        boolean dataIsEvenlyDistributed =
                ObjectUtils.doubleCompare(distributionFactor, distributionFactorLower) >= 0
                        && ObjectUtils.doubleCompare(distributionFactor, distributionFactorUpper)
                                <= 0;

        if (dataIsEvenlyDistributed) {
            // the minimum dynamic chunk size is at least 1
            final int dynamicChunkSize = Math.max((int) (distributionFactor * chunkSize), 1);
            return splitEvenlySizedChunks(
                    tablePath, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
        } else {
            return getChunkRangesWithUnevenlyData(
                    table,
                    splitColumnName,
                    min,
                    max,
                    chunkSize,
                    tablePath,
                    sampleShardingThreshold,
                    sampleShardingAllow,
                    approximateRowCnt);
        }
    }

    private List<ChunkRange> getChunkRangesWithUnevenlyData(
            JdbcSourceTable table,
            String splitColumnName,
            Object min,
            Object max,
            int chunkSize,
            TablePath tablePath,
            int sampleShardingThreshold,
            boolean sampleShardingAllow,
            long approximateRowCnt)
            throws Exception {
        int shardCount = (int) (approximateRowCnt / chunkSize);
        int inverseSamplingRate = config.getSplitInverseSamplingRate();
        if (sampleShardingAllow && sampleShardingThreshold < shardCount) {
            // It is necessary to ensure that the number of data rows sampled by the
            // sampling rate is greater than the number of shards.
            // Otherwise, if the sampling rate is too low, it may result in an insufficient
            // number of data rows for the shards, leading to an inadequate number of
            // shards.
            // Therefore, inverseSamplingRate should be less than chunkSize
            if (inverseSamplingRate > chunkSize) {
                log.warn(
                        "The inverseSamplingRate is {}, which is greater than chunkSize {}, so we set inverseSamplingRate to chunkSize",
                        inverseSamplingRate,
                        chunkSize);
                inverseSamplingRate = chunkSize;
            }
            log.info(
                    "Use sampling sharding for table {}, the sampling rate is {}",
                    tablePath,
                    inverseSamplingRate);
            Object[] sample =
                    jdbcDialect.sampleDataFromColumn(
                            getOrEstablishConnection(),
                            table,
                            splitColumnName,
                            inverseSamplingRate,
                            config.getFetchSize());
            log.info(
                    "Sample data from table {} end, the sample size is {}",
                    tablePath,
                    sample.length);
            return efficientShardingThroughSampling(
                    tablePath, sample, approximateRowCnt, shardCount);
        }
        return splitUnevenlySizedChunks(table, splitColumnName, min, max, chunkSize);
    }

    private Long queryApproximateRowCnt(JdbcSourceTable table) throws SQLException {
        return jdbcDialect.approximateRowCntStatement(getOrEstablishConnection(), table);
    }

    private double calculateDistributionFactor(
            TablePath tablePath, Object min, Object max, long approximateRowCnt) {

        if (!min.getClass().equals(max.getClass())) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported operation type, the MIN value type %s is different with MAX value type %s.",
                            min.getClass().getSimpleName(), max.getClass().getSimpleName()));
        }
        if (approximateRowCnt == 0) {
            return Double.MAX_VALUE;
        }
        BigDecimal difference = ObjectUtils.minus(max, min);
        // factor = (max - min + 1) / rowCount
        final BigDecimal subRowCnt = difference.add(BigDecimal.valueOf(1));
        double distributionFactor =
                subRowCnt.divide(new BigDecimal(approximateRowCnt), 4, ROUND_CEILING).doubleValue();
        log.info(
                "The distribution factor of table {} is {} according to the min split key {}, max split key {} and approximate row count {}",
                tablePath,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    private List<ChunkRange> splitStringEvenlySizedChunks(
            TablePath tablePath,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize,
            int maxLength,
            int radix,
            String collationSequence) {
        log.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tablePath,
                approximateRowCnt,
                chunkSize,
                dynamicChunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, dynamicChunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(
                    ChunkRange.of(
                            chunkStart == null
                                    ? null
                                    : CollationBasedSplitter.decodeNumericRangeToString(
                                            chunkStart.toString(),
                                            maxLength,
                                            radix,
                                            collationSequence),
                            chunkEnd == null
                                    ? null
                                    : CollationBasedSplitter.decodeNumericRangeToString(
                                            chunkEnd.toString(),
                                            maxLength,
                                            radix,
                                            collationSequence)));
            chunkStart = chunkEnd;
            try {
                chunkEnd = ObjectUtils.plus(chunkEnd, dynamicChunkSize);
            } catch (ArithmeticException e) {
                // Stop chunk split to avoid dead loop when number overflows.
                break;
            }
        }
        // add the ending split
        if (chunkStart != null) {
            splits.add(
                    ChunkRange.of(
                            CollationBasedSplitter.decodeNumericRangeToString(
                                    chunkStart.toString(), maxLength, radix, collationSequence),
                            null));
        } else {
            splits.add(ChunkRange.of(null, null));
        }
        return splits;
    }

    private List<ChunkRange> splitEvenlySizedChunks(
            TablePath tablePath,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        log.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tablePath,
                approximateRowCnt,
                chunkSize,
                dynamicChunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, dynamicChunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            try {
                chunkEnd = ObjectUtils.plus(chunkEnd, dynamicChunkSize);
            } catch (ArithmeticException e) {
                // Stop chunk split to avoid dead loop when number overflows.
                break;
            }
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    public static List<ChunkRange> efficientShardingThroughSampling(
            TablePath tablePath, Object[] sampleData, long approximateRowCnt, int shardCount) {
        log.info(
                "Use efficient sharding through sampling optimization for table {}, the approximate row count is {}, the shardCount is {}",
                tablePath,
                approximateRowCnt,
                shardCount);

        final List<ChunkRange> splits = new ArrayList<>();

        if (shardCount == 0) {
            splits.add(ChunkRange.of(null, null));
            return splits;
        }

        double approxSamplePerShard = (double) sampleData.length / shardCount;

        Object lastEnd = null;
        if (approxSamplePerShard <= 1) {
            splits.add(ChunkRange.of(null, sampleData[0]));
            lastEnd = sampleData[0];
            for (int i = 1; i < sampleData.length; i++) {
                // avoid split duplicate data
                if (!sampleData[i].equals(lastEnd)) {
                    splits.add(ChunkRange.of(lastEnd, sampleData[i]));
                    lastEnd = sampleData[i];
                }
            }

            splits.add(ChunkRange.of(lastEnd, null));

        } else {
            for (int i = 0; i < shardCount; i++) {
                Object chunkStart = lastEnd;
                Object chunkEnd =
                        (i < shardCount - 1)
                                ? sampleData[(int) ((i + 1) * approxSamplePerShard)]
                                : null;
                // avoid split duplicate data
                if (i == 0 || i == shardCount - 1 || !Objects.equals(chunkEnd, chunkStart)) {
                    splits.add(ChunkRange.of(chunkStart, chunkEnd));
                    lastEnd = chunkEnd;
                }
            }
        }
        return splits;
    }

    private List<ChunkRange> splitUnevenlySizedChunks(
            JdbcSourceTable table, String splitColumnName, Object min, Object max, int chunkSize)
            throws SQLException {
        log.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}",
                table.getTablePath(),
                chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(min, table, splitColumnName, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && objectCompare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++, table.getTablePath());
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(chunkEnd, table, splitColumnName, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    /**
     * split by date type column
     *
     * @param table
     * @param splitColumnName
     * @param min
     * @param max
     * @param chunkSize
     * @return
     * @throws SQLException
     */
    private List<ChunkRange> dateColumnSplitChunks(
            JdbcSourceTable table, String splitColumnName, Object min, Object max, int chunkSize)
            throws SQLException {
        log.info("Use date chunks for table {}", table.getTablePath());
        final List<ChunkRange> splits = new ArrayList<>();
        Date sqlDateMin = null;
        Date sqlDateMax = null;
        if (min instanceof Date) {
            sqlDateMin = (Date) min;
            sqlDateMax = (Date) max;
        } else if (min instanceof Timestamp) {
            sqlDateMin = new Date(((Timestamp) min).getTime());
            sqlDateMax = new Date(((Timestamp) max).getTime());
        }
        List<LocalDate> dateRange =
                getDateRange(sqlDateMin.toLocalDate(), sqlDateMax.toLocalDate());
        if (dateRange.size() > 20 * 365) {
            // TODO: If dateRange granter than 20 year, need get the real date in the table
        }

        Long rowCnt = queryApproximateRowCnt(table);
        int step = 1;
        if (rowCnt / dateRange.size() < chunkSize) {
            int splitNum = (int) (rowCnt / chunkSize) + 1;
            step = dateRange.size() / splitNum;
        }

        for (int i = 0; i < dateRange.size(); i = i + step) {
            if (i == 0) {
                splits.add(ChunkRange.of(null, dateRange.get(i)));
            } else {
                splits.add(ChunkRange.of(dateRange.get(i - step), dateRange.get(i)));
            }

            if ((i + step) >= dateRange.size()) {
                splits.add(ChunkRange.of(dateRange.get(i), null));
            }
        }
        return splits;
    }

    // obtaining date range
    private static List<LocalDate> getDateRange(LocalDate startDate, LocalDate endDate) {
        List<LocalDate> dateRange = new ArrayList<>();

        LocalDate currentDate = startDate;
        while (!currentDate.isAfter(endDate)) {
            dateRange.add(currentDate);
            currentDate = currentDate.plusDays(1);
        }

        return dateRange;
    }

    private Object nextChunkEnd(
            Object previousChunkEnd,
            JdbcSourceTable table,
            String splitColumnName,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                jdbcDialect.queryNextChunkMax(
                        getOrEstablishConnection(),
                        table,
                        splitColumnName,
                        chunkSize,
                        previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(table, splitColumnName, chunkEnd);
        }
        if (objectCompare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    private static void maySleep(int count, TablePath tablePath) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            log.info("DynamicChunkSplitter has split {} chunks for table {}", count, tablePath);
        }
    }

    private int objectCompare(Object obj1, Object obj2) {
        return ObjectUtils.compare(obj1, obj2);
    }

    @VisibleForTesting
    String createDynamicSplitQuerySQL(JdbcSourceSplit split, TableSchema schema) {
        boolean isComposite = isCompositeSplit(split);

        final String condition;
        if (isComposite) {
            condition = buildCompositeCondition(split);
        } else {
            condition = buildSingleColumnCondition(split, schema);
        }

        String splitQuery = split.getSplitQuery();
        if (StringUtils.isNotBlank(splitQuery)) {
            splitQuery =
                    String.format("SELECT * FROM (%s) tmp", applyUserWhereCondition(splitQuery));
        } else {
            if (StringUtils.isNotBlank(config.getWhereConditionClause())) {
                String userQuery =
                        String.format(
                                "SELECT * FROM %s",
                                jdbcDialect.tableIdentifier(split.getTablePath()));
                splitQuery =
                        String.format("SELECT * FROM (%s) tmp", applyUserWhereCondition(userQuery));
            } else {
                splitQuery =
                        String.format(
                                "SELECT * FROM %s",
                                jdbcDialect.tableIdentifier(split.getTablePath()));
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append(splitQuery);
        if (!StringUtils.isEmpty(condition)) {
            sql.append(" WHERE ").append(condition);
        }
        return sql.toString();
    }

    private String buildSingleColumnCondition(JdbcSourceSplit split, TableSchema schema) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {split.getSplitKeyName()},
                        new SeaTunnelDataType[] {split.getSplitKeyType()});
        boolean isFirstSplit = split.getSplitStart() == null;
        boolean isLastSplit = split.getSplitEnd() == null;

        if (isFirstSplit && isLastSplit) {
            return null;
        } else if (isFirstSplit) {
            StringBuilder sql = new StringBuilder();
            addKeyColumnsToCondition(schema, rowType, sql, " <= ?");
            sql.append(" AND NOT (");
            addKeyColumnsToCondition(schema, rowType, sql, " = ?");
            sql.append(")");
            return sql.toString();
        } else if (isLastSplit) {
            StringBuilder sql = new StringBuilder();
            addKeyColumnsToCondition(schema, rowType, sql, " >= ?");
            return sql.toString();
        } else {
            StringBuilder sql = new StringBuilder();
            addKeyColumnsToCondition(schema, rowType, sql, " >= ?");
            sql.append(" AND NOT (");
            addKeyColumnsToCondition(schema, rowType, sql, " = ?");
            sql.append(")");
            sql.append(" AND ");
            addKeyColumnsToCondition(schema, rowType, sql, " <= ?");
            return sql.toString();
        }
    }

    /**
     * Builds the WHERE predicate for a composite-key split: lexicographic {@code > start} and
     * {@code <= end} tuple comparisons, expressed as portable expanded OR/AND conditions (no
     * row-value-constructor syntax, so it works on SQL Server too). Returns null for a single
     * full-table split (both bounds null).
     */
    private String buildCompositeCondition(JdbcSourceSplit split) {
        Object[] startArr = (Object[]) split.getSplitStart();
        Object[] endArr = (Object[]) split.getSplitEnd();
        boolean isFirstSplit = startArr == null;
        boolean isLastSplit = endArr == null;

        if (isFirstSplit && isLastSplit) {
            return null;
        }

        String[] columnNames = ((SeaTunnelRowType) split.getSplitKeyType()).getFieldNames();

        if (isFirstSplit) {
            // (col1 < ?) OR (col1 = ? AND col2 <= ?) ... — lexicographic <= without
            // row-value-constructor syntax
            return buildExpandedTupleCondition(columnNames, "<", "<=");
        } else if (isLastSplit) {
            // (col1 > ?) OR (col1 = ? AND col2 > ?) ... — lexicographic >
            return buildExpandedTupleCondition(columnNames, ">", ">");
        } else {
            // (cols) > start AND (cols) <= end, both expanded
            return buildExpandedTupleCondition(columnNames, ">", ">")
                    + " AND "
                    + buildExpandedTupleCondition(columnNames, "<", "<=");
        }
    }

    /**
     * Builds an expanded OR/AND condition equivalent to a row-value-constructor comparison {@code
     * (col1, col2, ...) OP (?, ?, ...)} without using the row-value-constructor syntax, which SQL
     * Server (T-SQL) and a few other dialects do not support. For example for {@code ">"}: {@code
     * (col1 > ?) OR (col1 = ? AND col2 > ?) OR (col1 = ? AND col2 = ? AND col3 > ?)}.
     *
     * @param columns quoted key column names
     * @param prefixOp comparison operator used by the intermediate OR branches ({@code ">"} or
     *     {@code "<"})
     * @param lastOp comparison operator used by the final OR branch that binds the full tuple
     * @return the expanded, portable comparison condition
     */
    private String buildExpandedTupleCondition(String[] columns, String prefixOp, String lastOp) {
        StringBuilder where = new StringBuilder("(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                where.append(" OR ");
            }
            where.append("(");
            for (int j = 0; j <= i; j++) {
                if (j > 0) {
                    where.append(" AND ");
                }
                String quoted = jdbcDialect.quoteIdentifier(columns[j]);
                if (j < i) {
                    where.append(quoted).append(" = ?");
                } else {
                    where.append(quoted)
                            .append(" ")
                            .append(i == columns.length - 1 ? lastOp : prefixOp)
                            .append(" ?");
                }
            }
            where.append(")");
        }
        where.append(")");
        return where.toString();
    }

    private void addKeyColumnsToCondition(
            TableSchema schema, SeaTunnelRowType rowType, StringBuilder sql, String predicate) {
        Map<String, Column> columns =
                schema.getColumns().stream().collect(Collectors.toMap(c -> c.getName(), c -> c));
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = jdbcDialect.quoteIdentifier(rowType.getFieldName(i));
            fieldName =
                    jdbcDialect.convertType(
                            fieldName, columns.get(rowType.getFieldName(i)).getSourceType());
            sql.append(fieldName).append(predicate);
            if (i < rowType.getTotalFields() - 1) {
                sql.append(" AND ");
            }
        }
    }

    /**
     * Returns whether a split carries a composite (multi-column) key, tracked explicitly on the
     * split key type rather than inferred from the runtime type of a nullable boundary field.
     */
    private static boolean isCompositeSplit(JdbcSourceSplit split) {
        return split.getSplitKeyType() instanceof SeaTunnelRowType
                && ((SeaTunnelRowType) split.getSplitKeyType()).getTotalFields() > 1;
    }

    private static void prepareDynamicSplitStatement(
            PreparedStatement statement, JdbcSourceSplit split) throws SQLException {
        boolean isFirstSplit = split.getSplitStart() == null;
        boolean isLastSplit = split.getSplitEnd() == null;
        if (isFirstSplit && isLastSplit) {
            return;
        }

        boolean isComposite = isCompositeSplit(split);

        if (isComposite) {
            prepareCompositeStatement(statement, split);
        } else {
            prepareSingleColumnStatement(statement, split);
        }
    }

    private static void prepareCompositeStatement(
            PreparedStatement statement, JdbcSourceSplit split) throws SQLException {
        Object[] startArr = (Object[]) split.getSplitStart();
        Object[] endArr = (Object[]) split.getSplitEnd();
        boolean isFirstSplit = startArr == null;
        boolean isLastSplit = endArr == null;

        int paramIndex = 1;
        if (isFirstSplit) {
            // WHERE (col1 < ?) OR (col1 = ? AND col2 <= ?) ... — bind end tuple cumulatively
            paramIndex = bindExpandedTuple(statement, endArr, paramIndex);
        } else if (isLastSplit) {
            // WHERE (col1 > ?) OR (col1 = ? AND col2 > ?) ... — bind start tuple cumulatively
            paramIndex = bindExpandedTuple(statement, startArr, paramIndex);
        } else {
            // WHERE (cols) > start AND (cols) <= end — bind both tuples cumulatively
            paramIndex = bindExpandedTuple(statement, startArr, paramIndex);
            bindExpandedTuple(statement, endArr, paramIndex);
        }
    }

    /**
     * Binds a composite-key boundary tuple to the placeholders of the expanded OR/AND condition
     * built by {@link #buildExpandedTupleCondition}: the i-th OR branch consumes {@code
     * values[0..i]}, so the tuple is bound cumulatively.
     *
     * @return the next free parameter index
     */
    private static int bindExpandedTuple(
            PreparedStatement statement, Object[] values, int startIndex) throws SQLException {
        int paramIndex = startIndex;
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j <= i; j++) {
                statement.setObject(paramIndex++, values[j]);
            }
        }
        return paramIndex;
    }

    private static void prepareSingleColumnStatement(
            PreparedStatement statement, JdbcSourceSplit split) throws SQLException {
        boolean isFirstSplit = split.getSplitStart() == null;
        boolean isLastSplit = split.getSplitEnd() == null;

        Object[] splitStart = new Object[] {split.getSplitStart()};
        Object[] splitEnd = new Object[] {split.getSplitEnd()};
        int splitKeyNumbers = 1;
        if (isFirstSplit) {
            for (int i = 0; i < splitKeyNumbers; i++) {
                statement.setObject(i + 1, splitEnd[i]);
                statement.setObject(i + 1 + splitKeyNumbers, splitEnd[i]);
            }
            log.info(
                    "Dynamic split (first) - params: [{}={}, {}={}]",
                    1,
                    splitEnd[0],
                    2,
                    splitEnd[0]);
        } else if (isLastSplit) {
            for (int i = 0; i < splitKeyNumbers; i++) {
                statement.setObject(i + 1, splitStart[i]);
            }
            log.info("Dynamic split (last) - params: [{}={}]", 1, splitStart[0]);
        } else {
            for (int i = 0; i < splitKeyNumbers; i++) {
                statement.setObject(i + 1, splitStart[i]);
                statement.setObject(i + 1 + splitKeyNumbers, splitEnd[i]);
                statement.setObject(i + 1 + 2 * splitKeyNumbers, splitEnd[i]);
            }
            log.info(
                    "Dynamic split (middle) - params: [{}={}, {}={}, {}={}]",
                    1,
                    splitStart[0],
                    2,
                    splitEnd[0],
                    3,
                    splitEnd[0]);
        }
    }

    @Data
    @EqualsAndHashCode
    public static class ChunkRange implements Serializable {
        private final Object chunkStart;
        private final Object chunkEnd;

        public static ChunkRange all() {
            return new ChunkRange(null, null);
        }

        public static ChunkRange of(Object chunkStart, Object chunkEnd) {
            return new ChunkRange(chunkStart, chunkEnd);
        }

        private ChunkRange(Object chunkStart, Object chunkEnd) {
            if (chunkStart != null || chunkEnd != null) {
                checkArgument(
                        !Objects.equals(chunkStart, chunkEnd),
                        "Chunk start %s shouldn't be equal to chunk end %s",
                        chunkStart,
                        chunkEnd);
            }
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
        }
    }
}
