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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

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
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            log.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            Column splitColumn = getSplitColumn(jdbc, dialect, tableId);
            List<SnapshotSplit> splits = new ArrayList<>();
            if (splitColumn == null) {
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
            } else {
                final List<ChunkRange> chunks;
                try {
                    chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to split chunks for table " + tableId, e);
                }

                // convert chunks into splits
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

        log.info(
                "Splitting table {} into chunks, split column: {}, min: {}, max: {}, chunk size: {}, "
                        + "distribution factor upper: {}, distribution factor lower: {}, sample sharding threshold: {}",
                tableId,
                splitColumnName,
                min,
                max,
                chunkSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold);

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
                if (sampleShardingThreshold < shardCount) {
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

    protected Column getSplitColumn(
            JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
            throws SQLException {
        Column splitColumn = null;
        Table table = dialect.queryTableSchema(jdbc, tableId).getTable();

        // first , compare user defined split column is in the primary key or unique key
        Properties splitColumnProperties = new Properties();
        try {
            splitColumnProperties = sourceConfig.getSplitColumn();
        } catch (Exception e) {
            log.error("Config snapshot.split.column get exception in {}:{}", tableId, e);
        }
        String tableSc =
                (String) splitColumnProperties.get(tableId.catalog() + "." + tableId.table());
        Boolean isUniqueKey = dialect.isUniqueKey(jdbc, tableId, tableSc);
        if (isUniqueKey) {
            Column column = table.columnWithName(tableSc);
            if (isEvenlySplitColumn(column)) {
                return column;
            }else{
                log.warn("Config snapshot.split.column type in {} is not TINYINT、SMALLINT、INT、BIGINT、DECIMAL、STRING", tableId);
            }
        } else {
            log.warn("Config snapshot.split.column not exists or not unique key for table {}", tableId);
        }

        Optional<PrimaryKey> primaryKey = dialect.getPrimaryKey(jdbc, tableId);
        if (primaryKey.isPresent()) {
            List<String> pkColumns = primaryKey.get().getColumnNames();

            for (String pkColumn : pkColumns) {
                Column column = table.columnWithName(pkColumn);
                if (isEvenlySplitColumn(column)) {
                    splitColumn = columnComparable(splitColumn, column);
                    if (sqlTypePriority(splitColumn) == 1) {
                        return splitColumn;
                    }
                }
            }
        } else {
            log.warn("No primary key found for table {}", tableId);
        }

        List<ConstraintKey> uniqueKeys = dialect.getUniqueKeys(jdbc, tableId);
        if (!uniqueKeys.isEmpty()) {
            for (ConstraintKey uniqueKey : uniqueKeys) {
                List<ConstraintKey.ConstraintKeyColumn> uniqueKeyColumns =
                        uniqueKey.getColumnNames();
                for (ConstraintKey.ConstraintKeyColumn uniqueKeyColumn : uniqueKeyColumns) {
                    Column column = table.columnWithName(uniqueKeyColumn.getColumnName());
                    if (isEvenlySplitColumn(column)) {
                        splitColumn = columnComparable(splitColumn, column);
                        if (sqlTypePriority(splitColumn) == 1) {
                            return splitColumn;
                        }
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
