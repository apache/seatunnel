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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.eumerator;

import static org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils.doubleCompare;
import static java.math.BigDecimal.ROUND_CEILING;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkRange;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.JdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
public class MySqlChunkSplitter implements JdbcSourceChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlChunkSplitter.class);

    private final JdbcSourceConfig sourceConfig;
    private final JdbcDataSourceDialect dialect;

    public MySqlChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {

            LOG.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            Table table = dialect.queryTableSchema(jdbc, tableId).getTable();
            Column splitColumn = getSplitColumn(table);
            final List<ChunkRange> chunks;
            try {
                chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to split chunks for table " + tableId, e);
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
            LOG.info("Split table {} into {} chunks, time cost: {}ms.",
                    tableId,
                    splits.size(),
                    end - start);
            return splits;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Generate Splits for table %s error", tableId), e);
        }
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return MySqlUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return MySqlUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return MySqlUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return MySqlUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
        TableId tableId, SeaTunnelRowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return MySqlUtils.buildSplitScanQuery(tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return MySqlTypeUtils.convertFromColumn(splitColumn);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks, using
     * evenly-sized chunks which is much efficient, using unevenly-sized chunks which will request
     * many queries and is not efficient.
     */
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
                return splitUnevenlySizedChunks(
                        jdbc, tableId, splitColumnName, min, max, chunkSize);
            }
        } else {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumnName, min, max, chunkSize);
        }
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        LOG.info(
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
        while (chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumnName, max, chunkSize);
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
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    private SnapshotSplit createSnapshotSplit(
            JdbcConnection jdbc,
            TableId tableId,
            int chunkId,
            SeaTunnelRowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        return new SnapshotSplit(
            splitId(tableId, chunkId),
            tableId,
            splitKeyType,
            chunkStart,
            chunkEnd,
            null);
    }

    // ------------------------------------------------------------------------------------------
    /** Returns the distribution factor of the given table. */
    @SuppressWarnings("MagicNumber")
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

    private static String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
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
            LOG.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    public static Column getSplitColumn(Table table) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format("Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
        }

        // use first field in primary key as the split key
        return primaryKeys.get(0);
    }
}
