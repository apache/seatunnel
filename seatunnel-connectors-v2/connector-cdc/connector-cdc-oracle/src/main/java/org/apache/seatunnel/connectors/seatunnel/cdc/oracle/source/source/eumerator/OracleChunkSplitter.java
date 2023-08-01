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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.eumerator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkRange;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.JdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils.OracleTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils.OracleUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import oracle.sql.ROWID;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_CEILING;
import static org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils.doubleCompare;

public class OracleChunkSplitter implements JdbcSourceChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(OracleChunkSplitter.class);

    private final JdbcSourceConfig sourceConfig;
    private final JdbcDataSourceDialect dialect;

    public OracleChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            LOG.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            Table table = dialect.queryTableSchema(jdbc, tableId).getTable();
            Column splitColumn =
                    getChunkKeyColumn(
                            table, ((OracleSourceConfig) sourceConfig).getChunkKeyColumn());
            final List<ChunkRange> chunks;
            try {
                chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
            } catch (SQLException e) {
                throw new SeaTunnelException("Failed to split chunks for table " + tableId, e);
            }
            // convert chunks into splits
            List<SnapshotSplit> splits = new ArrayList<>();
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
            long end = System.currentTimeMillis();
            LOG.info(
                    "Split table {} into {} chunks, time cost: {}ms.",
                    tableId,
                    splits.size(),
                    end - start);
            return splits;

        } catch (Exception e) {
            throw new SeaTunnelException(
                    String.format("Generate Splits for table %s error", tableId), e);
        }
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return OracleUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return OracleUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws SQLException {
        return OracleUtils.sampleDataFromColumn(jdbc, tableId, columnName, inverseSamplingRate);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OracleUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return OracleUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId,
            SeaTunnelRowType splitKeyType,
            boolean isFirstSplit,
            boolean isLastSplit) {
        return OracleUtils.buildSplitScanQuery(tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return OracleTypeUtils.convertFromColumn(splitColumn);
    }

    public static Column getChunkKeyColumn(Table table, @Nullable String chunkKeyColumn) {
        List<Column> primaryKeys = table.primaryKeyColumns();

        if (chunkKeyColumn != null) {
            Optional<Column> targetPkColumn =
                    primaryKeys.stream()
                            .filter(col -> chunkKeyColumn.equals(col.name()))
                            .findFirst();
            if (targetPkColumn.isPresent()) {
                return targetPkColumn.get();
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Chunk key column '%s' doesn't exist in the primary key [%s] of the table %s.",
                            chunkKeyColumn,
                            primaryKeys.stream().map(Column::name).collect(Collectors.joining(",")),
                            table.id()));
        }

        // Use the ROWID column as the chunk key column by default for oracle cdc connector
        return Column.editor().jdbcType(Types.VARCHAR).name(ROWID.class.getSimpleName()).create();
    }

    private List<ChunkRange> splitTableIntoChunks(
            JdbcConnection jdbc, TableId tableId, Column splitColumn) throws SQLException {
        final String splitColumnName = splitColumn.name();
        final Object[] minMax = queryMinMax(jdbc, tableId, splitColumnName);
        final Object min = minMax[0];
        final Object max = minMax[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }
        final int chunkSize = sourceConfig.getSplitSize();
        final double distributionFactorUpper = sourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = sourceConfig.getDistributionFactorLower();

        // use ROWID get splitUnevenlySizedChunks by default
        if (splitColumn.name().equals(ROWID.class.getSimpleName())) {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumnName, min, max, chunkSize);
        }

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
                        tableId, min, max, approximateRowCnt, dynamicChunkSize);
            } else {
                int shardCount = (int) (approximateRowCnt / chunkSize);
                if (sourceConfig.getSampleShardingThreshold() < shardCount) {
                    Object[] sample =
                            sampleDataFromColumn(
                                    jdbc,
                                    tableId,
                                    splitColumnName,
                                    sourceConfig.getInverseSamplingRate());
                    // In order to prevent data loss due to the absence of the minimum value in the
                    // sampled data, the minimum value is directly added here.
                    Object[] newSample = new Object[sample.length + 1];
                    newSample[0] = min;
                    System.arraycopy(sample, 0, newSample, 1, sample.length);
                    return efficientShardingThroughSampling(
                            tableId, newSample, approximateRowCnt, shardCount);
                }
                return splitUnevenlySizedChunks(
                        jdbc, tableId, splitColumnName, min, max, chunkSize);
            }
        } else {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumnName, min, max, chunkSize);
        }
    }

    /** Split table into unevenly sized chunks by continuously calculating next chunk max value. */
    private List<ChunkRange> splitUnevenlySizedChunks(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            Object min,
            Object max,
            int chunkSize)
            throws SQLException {
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}", tableId, chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(jdbc, min, tableId, splitColumnName, max, chunkSize);
        int count = 0;

        while (chunkEnd != null && isChunkEndLeMax(chunkEnd, max)) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on Oracle server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumnName, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId, Object min, Object max, long approximateRowCnt, int chunkSize) {
        LOG.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}",
                tableId,
                approximateRowCnt,
                chunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, chunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            chunkEnd = ObjectUtils.plus(chunkEnd, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            String splitColumnName,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(jdbc, tableId, splitColumnName, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(jdbc, tableId, splitColumnName, chunkEnd);
        }
        if (isChunkEndGeMax(chunkEnd, max)) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    /** ChunkEnd less than or equal to max. */
    private boolean isChunkEndLeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            <= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0;
        }
        return chunkEndMaxCompare;
    }

    /** ChunkEnd greater than or equal to max. */
    private boolean isChunkEndGeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            >= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) >= 0;
        }
        return chunkEndMaxCompare;
    }

    private static void maySleep(int count, TableId tableId) {
        // every 10 queries to sleep 0.1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    /** Returns the distribution factor of the given table. */
    private double calculateDistributionFactor(
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
        LOG.info(
                "The distribution factor of table {} is {} according to the min split key {}, max split key {} and approximate row count {}",
                tableId,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    private SnapshotSplit createSnapshotSplit(
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

    private static String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    private List<ChunkRange> efficientShardingThroughSampling(
            TableId tableId, Object[] sampleData, long approximateRowCnt, int shardCount) {
        LOG.info(
                "Use efficient sharding through sampling optimization for table {}, the approximate row count is {}, the shardCount is {}",
                tableId,
                approximateRowCnt,
                shardCount);

        final List<ChunkRange> splits = new ArrayList<>();

        // Calculate the shard boundaries
        for (int i = 0; i < shardCount; i++) {
            Object chunkStart = sampleData[(int) ((long) i * sampleData.length / shardCount)];
            Object chunkEnd =
                    i < shardCount - 1
                            ? sampleData[(int) (((long) i + 1) * sampleData.length / shardCount)]
                            : null;
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
        }

        return splits;
    }
}
