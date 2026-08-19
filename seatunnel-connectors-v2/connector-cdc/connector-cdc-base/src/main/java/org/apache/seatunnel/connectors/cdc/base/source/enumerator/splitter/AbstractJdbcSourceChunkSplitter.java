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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.math.BigDecimal.ROUND_CEILING;
import static org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils.doubleCompare;

@Slf4j
public abstract class AbstractJdbcSourceChunkSplitter implements JdbcSourceChunkSplitter {

    private final JdbcSourceConfig sourceConfig;
    private final JdbcDataSourceDialect dialect;

    public AbstractJdbcSourceChunkSplitter(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {
        // When concurrent read is disabled, skip split analysis and return a single full-table
        // split. This avoids expensive MIN/MAX scans on tables without proper indexes.
        if (!sourceConfig.isEnableConcurrentRead()) {
            log.info(
                    "Concurrent read is disabled for table {}, using single split without analysis.",
                    tableId);
            return Collections.singletonList(
                    createSnapshotSplit(null, tableId, 0, null, null, null));
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            log.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            List<Column> splitColumns = getSplitColumns(jdbc, dialect, tableId);
            List<SnapshotSplit> splits = new ArrayList<>();

            if (splitColumns.isEmpty()) {
                if (sourceConfig.isExactlyOnce()) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Exactly once is enabled, but not found primary key or unique key for table %s",
                                    tableId));
                }
                SnapshotSplit singleSplit = createSnapshotSplit(jdbc, tableId, 0, null, null, null);
                splits.add(singleSplit);
                log.warn(
                        "No evenly split column found for table {}, use single split {}",
                        tableId,
                        singleSplit);
            } else if (splitColumns.size() == 1) {
                // Single-column path (existing behavior)
                Column splitColumn = splitColumns.get(0);
                log.info("Chosen split column {} for table {}", splitColumn.name(), tableId);
                final List<ChunkRange> chunks;
                try {
                    chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to split chunks for table " + tableId, e);
                }

                SeaTunnelRowType splitType = getSplitType(splitColumn);
                for (int i = 0; i < chunks.size(); i++) {
                    ChunkRange chunk = chunks.get(i);
                    SnapshotSplit split =
                            createSnapshotSplit(
                                    jdbc,
                                    tableId,
                                    i,
                                    splitType,
                                    chunk.getChunkStart(),
                                    chunk.getChunkEnd());
                    splits.add(split);
                }
            } else {
                // Multi-column composite primary key path
                log.info(
                        "Chosen {} split columns {} for table {}",
                        splitColumns.size(),
                        splitColumns.stream().map(Column::name).toArray(),
                        tableId);
                final List<ChunkRange> chunks;
                try {
                    chunks = splitTableIntoChunksMulti(jdbc, tableId, splitColumns);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to split chunks for table " + tableId, e);
                }

                SeaTunnelRowType splitType = getSplitType(splitColumns);
                for (int i = 0; i < chunks.size(); i++) {
                    ChunkRange chunk = chunks.get(i);
                    SnapshotSplit split =
                            createSnapshotSplitMulti(
                                    jdbc,
                                    tableId,
                                    i,
                                    splitType,
                                    (Object[]) chunk.getChunkStart(),
                                    (Object[]) chunk.getChunkEnd());
                    splits.add(split);
                }
            }

            long end = System.currentTimeMillis();
            log.info(
                    "Split table {} into {} chunks, time cost: {}ms.",
                    tableId,
                    splits.size(),
                    end - start);
            return splits;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Generate Splits for table %s error", tableId), e);
        }
    }

    private List<ChunkRange> splitTableIntoChunks(
            JdbcConnection jdbc, TableId tableId, Column splitColumn) throws Exception {
        final String splitColumnName = splitColumn.name();
        final Object[] minMax = queryMinMax(jdbc, tableId, splitColumn);
        final Object min = minMax[0];
        final Object max = minMax[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final int chunkSize = sourceConfig.getSplitSize();
        final double distributionFactorUpper = sourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = sourceConfig.getDistributionFactorLower();
        final int sampleShardingThreshold = sourceConfig.getSampleShardingThreshold();
        boolean sampleShardingAllow = sourceConfig.isSampleShardingAllow();

        log.info(
                "Splitting table {} into chunks, split column: {}, min: {}, max: {}, chunk size: {}, "
                        + "distribution factor upper: {}, distribution factor lower: {}, sample sharding threshold: {},"
                        + " sample sharding enable: {}",
                tableId,
                splitColumnName,
                min,
                max,
                chunkSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                sampleShardingAllow);

        if (isEvenlySplitColumn(splitColumn)) {
            long approximateRowCnt = queryApproximateRowCnt(jdbc, tableId);
            double distributionFactor =
                    calculateDistributionFactor(tableId, min, max, approximateRowCnt);

            boolean dataIsEvenlyDistributed =
                    doubleCompare(distributionFactor, distributionFactorLower) >= 0
                            && doubleCompare(distributionFactor, distributionFactorUpper) <= 0;

            if (dataIsEvenlyDistributed) {
                // the minimum dynamic chunk size is at least 1
                final int dynamicChunkSize = Math.max((int) (distributionFactor * chunkSize), 1);
                return splitEvenlySizedChunks(
                        tableId, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
            } else {
                int shardCount = (int) (approximateRowCnt / chunkSize);
                int inverseSamplingRate = sourceConfig.getInverseSamplingRate();
                if (sampleShardingAllow && sampleShardingThreshold < shardCount) {
                    if (inverseSamplingRate > chunkSize) {
                        log.warn(
                                "The inverseSamplingRate is {}, which is greater than chunkSize {}, so we set inverseSamplingRate to chunkSize",
                                inverseSamplingRate,
                                chunkSize);
                        inverseSamplingRate = chunkSize;
                    }
                    log.info(
                            "Use sampling sharding for table {}, the sampling rate is {}",
                            tableId,
                            inverseSamplingRate);
                    Object[] sample =
                            sampleDataFromColumn(jdbc, tableId, splitColumn, inverseSamplingRate);
                    log.info(
                            "Sample data from table {} end, the sample size is {}",
                            tableId,
                            sample.length);
                    return efficientShardingThroughSampling(
                            tableId, sample, approximateRowCnt, shardCount);
                }
                return splitUnevenlySizedChunks(jdbc, tableId, splitColumn, min, max, chunkSize);
            }
        } else {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumn, min, max, chunkSize);
        }
    }

    /**
     * Split table into chunks for composite primary key using multi-column lexicographic ordering.
     */
    private List<ChunkRange> splitTableIntoChunksMulti(
            JdbcConnection jdbc, TableId tableId, List<Column> splitColumns) throws Exception {
        final Object[] minMax = queryMinMaxMulti(jdbc, tableId, splitColumns);
        final Object[] min = (Object[]) minMax[0];
        final Object[] max = (Object[]) minMax[1];
        if (min == null || max == null || Arrays.equals(min, max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final int chunkSize = sourceConfig.getSplitSize();

        log.info(
                "Splitting table {} into chunks with composite key, columns: {}, min: {}, max: {}, chunk size: {}",
                tableId,
                splitColumns.stream().map(Column::name).toArray(),
                Arrays.toString(min),
                Arrays.toString(max),
                chunkSize);

        // For composite keys, always use unevenly-sized chunks
        return splitUnevenlySizedChunksMulti(jdbc, tableId, splitColumns, min, max, chunkSize);
    }

    /** Split table into unevenly sized chunks by continuously calculating next chunk max value. */
    protected List<ChunkRange> splitUnevenlySizedChunks(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            Object min,
            Object max,
            int chunkSize)
            throws SQLException {
        log.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}", tableId, chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(jdbc, min, tableId, splitColumn, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && ObjectCompare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumn, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    protected Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            Column splitColumn,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(jdbc, tableId, splitColumn, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(jdbc, tableId, splitColumn, chunkEnd);
        }
        if (ObjectCompare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    protected List<ChunkRange> efficientShardingThroughSampling(
            TableId tableId, Object[] sampleData, long approximateRowCnt, int shardCount) {
        log.info(
                "Use efficient sharding through sampling optimization for table {}, the approximate row count is {}, the shardCount is {}",
                tableId,
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

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    protected List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        log.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tableId,
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
        while (ObjectCompare(chunkEnd, max) <= 0) {
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

    // ------------------------------------------------------------------------------------------

    /** Returns the distribution factor of the given table. */
    @SuppressWarnings("MagicNumber")
    protected double calculateDistributionFactor(
            TableId tableId, Object min, Object max, long approximateRowCnt) {

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
                tableId,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    protected SnapshotSplit createSnapshotSplit(
            JdbcConnection jdbc,
            TableId tableId,
            int chunkId,
            SeaTunnelRowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        return new SnapshotSplit(
                splitId(tableId, chunkId), tableId, splitKeyType, splitStart, splitEnd);
    }

    // ------------------------------------------------------------------------------------------
    // Multi-column composite primary key abstract methods
    // ------------------------------------------------------------------------------------------

    /**
     * Get the split columns for the table. For composite primary keys, returns all PK columns.
     * Default implementation returns single column from getSplitColumn.
     */
    protected List<Column> getSplitColumns(
            JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
            throws SQLException {
        Column splitColumn = getSplitColumn(jdbc, dialect, tableId);
        if (splitColumn != null) {
            return Collections.singletonList(splitColumn);
        }
        return Collections.emptyList();
    }

    protected Column getSplitColumn(
            JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
            throws SQLException {
        Column splitColumn = null;
        Table table = dialect.queryTableSchema(jdbc, tableId).getTable();

        // first , compare user defined split column is in the primary key or unique key
        Map<String, String> splitColumnsConfig = new HashMap<>();
        try {
            splitColumnsConfig = sourceConfig.getSplitColumn();
        } catch (Exception e) {
            log.error("Config snapshotSplitColumn get exception in {}:{}", tableId, e);
        }
        String tableSc =
                splitColumnsConfig.getOrDefault(tableId.catalog() + "." + tableId.table(), null);

        if (StringUtils.isNotEmpty(tableSc)) {
            // Is tableSc（table split column） the unique key
            AtomicBoolean isUniqueKey = new AtomicBoolean(false);
            dialect.getUniqueKeys(jdbc, tableId)
                    .forEach(
                            ck ->
                                    ck.getColumnNames()
                                            .forEach(
                                                    ckc -> {
                                                        if (tableSc.equals(ckc.getColumnName())) {
                                                            isUniqueKey.set(true);
                                                        }
                                                    }));

            if (isUniqueKey.get()) {
                Column column = table.columnWithName(tableSc);
                if (isEvenlySplitColumn(column)) {
                    return column;
                } else {
                    log.warn(
                            "Config snapshotSplitColumn type in {} is not TINYINT、SMALLINT、INT、BIGINT、DECIMAL、STRING",
                            tableId);
                }
            } else {
                log.warn("Config snapshotSplitColumn not unique key for table {}", tableId);
            }
        } else {
            log.info("Config snapshotSplitColumn not exists for table {}", tableId);
        }

        Optional<PrimaryKey> primaryKey = dialect.getPrimaryKey(jdbc, tableId);
        if (primaryKey.isPresent()) {
            Column firstColumn = table.columnWithName(primaryKey.get().getColumnNames().get(0));
            if (isEvenlySplitColumn(firstColumn)) {
                splitColumn = columnComparable(splitColumn, firstColumn);
                if (sqlTypePriority(splitColumn) == 1) {
                    return splitColumn;
                }
            }
        } else {
            log.warn("No primary key found for table {}", tableId);
        }

        List<ConstraintKey> uniqueKeys = dialect.getUniqueKeys(jdbc, tableId);
        if (!uniqueKeys.isEmpty()) {
            for (ConstraintKey uniqueKey : uniqueKeys) {
                Column firstColumn =
                        table.columnWithName(uniqueKey.getColumnNames().get(0).getColumnName());
                if (isEvenlySplitColumn(firstColumn)) {
                    splitColumn = columnComparable(splitColumn, firstColumn);
                    if (sqlTypePriority(splitColumn) == 1) {
                        return splitColumn;
                    }
                }
            }
        } else {
            log.warn("No unique key found for table {}", tableId);
        }
        if (splitColumn != null) {
            return splitColumn;
        }

        log.warn("No evenly split column found for table {}", tableId);
        return null;
    }

    // ------------------------------------------------------------------------------------------
    // Multi-column methods to be overridden by database-specific implementations
    // ------------------------------------------------------------------------------------------

    /**
     * Query the minimum and maximum tuple for composite primary key.
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param splitColumns split columns.
     * @return Object[] where [0] is min tuple, [1] is max tuple.
     */
    protected Object[] queryMinMaxMulti(
            JdbcConnection jdbc, TableId tableId, List<Column> splitColumns) throws SQLException {
        throw new UnsupportedOperationException(
                "Multi-column queryMinMax is not implemented for this database");
    }

    /** Query the next chunk max tuple for composite primary key. */
    protected Object[] queryNextChunkMaxMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            int chunkSize,
            Object[] includedLowerBound)
            throws SQLException {
        throw new UnsupportedOperationException(
                "Multi-column queryNextChunkMax is not implemented for this database");
    }

    /** Query the minimum tuple greater than the excluded lower bound. */
    protected Object[] queryMinMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            Object[] excludedLowerBound)
            throws SQLException {
        throw new UnsupportedOperationException(
                "Multi-column queryMin is not implemented for this database");
    }

    /** Split table into unevenly sized chunks for composite primary key. */
    protected List<ChunkRange> splitUnevenlySizedChunksMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            Object[] min,
            Object[] max,
            int chunkSize)
            throws SQLException {
        throw new UnsupportedOperationException(
                "Multi-column splitUnevenlySizedChunks is not implemented for this database");
    }

    /** Create a snapshot split for multi-column composite primary key. */
    protected SnapshotSplit createSnapshotSplitMulti(
            JdbcConnection jdbc,
            TableId tableId,
            int chunkId,
            SeaTunnelRowType splitKeyType,
            Object[] chunkStart,
            Object[] chunkEnd) {
        return new SnapshotSplit(
                splitId(tableId, chunkId), tableId, splitKeyType, chunkStart, chunkEnd);
    }

    /** Get the split key type for composite primary key. */
    protected SeaTunnelRowType getSplitType(List<Column> splitColumns) {
        return getSplitType(splitColumns.get(0));
    }

    protected SeaTunnelRowType getSplitType(Table table) {
        return getSplitType(table.primaryKeyColumns().get(0));
    }

    protected String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    protected int ObjectCompare(Object obj1, Object obj2) {
        return ObjectUtils.compare(obj1, obj2);
    }

    @SuppressWarnings("MagicNumber")
    private static void maySleep(int count, TableId tableId) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            log.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    private int sqlTypePriority(Column splitColumn) {
        switch (fromDbzColumn(splitColumn).getSqlType()) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
                return 3;
            case BIGINT:
                return 4;
            case DECIMAL:
                return 5;
            case STRING:
                return 6;
            default:
                return Integer.MAX_VALUE;
        }
    }

    private Column columnComparable(Column then, Column other) {
        if (then == null) {
            return other;
        }
        if (sqlTypePriority(then) > sqlTypePriority(other)) {
            return other;
        }
        return then;
    }
}
