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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.enumerator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkRange;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
@Slf4j
public class MySqlChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    private RelationalDatabaseConnectorConfig dbzConnectorConfig;

    public MySqlChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
        this.dbzConnectorConfig = sourceConfig.getDbzConnectorConfig();
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
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws Exception {
        return MySqlUtils.skipReadAndSortSampleData(jdbc, tableId, columnName, inverseSamplingRate);
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
            Table table, SeaTunnelRowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return MySqlUtils.buildSplitScanQuery(table.id(), splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return MySqlTypeUtils.convertFromColumn(splitColumn, dbzConnectorConfig);
    }

    // ------------------------------------------------------------------------------------------
    // Multi-column composite primary key overrides
    // ------------------------------------------------------------------------------------------

    @Override
    protected List<Column> getSplitColumns(
            JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
            throws SQLException {
        Table table = dialect.queryTableSchema(jdbc, tableId).getTable();
        return MySqlUtils.getSplitColumns(table);
    }

    @Override
    protected Object[] queryMinMaxMulti(
            JdbcConnection jdbc, TableId tableId, List<Column> splitColumns) throws SQLException {
        return MySqlUtils.queryMinMaxMulti(jdbc, tableId, splitColumns);
    }

    @Override
    protected Object[] queryNextChunkMaxMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            int chunkSize,
            Object[] includedLowerBound)
            throws SQLException {
        return MySqlUtils.queryNextChunkMaxMulti(
                jdbc, tableId, splitColumns, chunkSize, includedLowerBound);
    }

    @Override
    protected Object[] queryMinMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            Object[] excludedLowerBound)
            throws SQLException {
        return MySqlUtils.queryMinMulti(jdbc, tableId, splitColumns, excludedLowerBound);
    }

    @Override
    protected SeaTunnelRowType getSplitType(List<Column> splitColumns) {
        return MySqlUtils.getSplitType(splitColumns, dbzConnectorConfig);
    }

    @Override
    protected List<ChunkRange> splitUnevenlySizedChunksMulti(
            JdbcConnection jdbc,
            TableId tableId,
            List<Column> splitColumns,
            Object[] min,
            Object[] max,
            int chunkSize)
            throws SQLException {
        log.info(
                "Use unevenly-sized chunks for table {} with composite key, the chunk size is {}",
                tableId,
                chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object[] chunkStart = null;
        Object[] chunkEnd =
                nextChunkEndMulti(jdbc, min, tableId, splitColumns, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && compareObjectArrays(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd =
                    nextChunkEndMulti(
                            jdbc, chunkEnd, tableId, splitColumns, max, chunkSize);
        }
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object[] nextChunkEndMulti(
            JdbcConnection jdbc,
            Object[] previousChunkEnd,
            TableId tableId,
            List<Column> splitColumns,
            Object[] max,
            int chunkSize)
            throws SQLException {
        Object[] chunkEnd =
                queryNextChunkMaxMulti(jdbc, tableId, splitColumns, chunkSize, previousChunkEnd);
        if (chunkEnd != null && Arrays.equals(previousChunkEnd, chunkEnd)) {
            chunkEnd = queryMinMulti(jdbc, tableId, splitColumns, chunkEnd);
        }
        if (chunkEnd == null || compareObjectArrays(chunkEnd, max) >= 0) {
            return null;
        }
        return chunkEnd;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareObjectArrays(Object[] a, Object[] b) {
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            int cmp = ((Comparable) a[i]).compareTo(b[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    @SuppressWarnings("MagicNumber")
    private static void maySleep(int count, TableId tableId) {
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            log.info(
                    "MySqlChunkSplitter has split {} chunks for table {}",
                    count,
                    tableId);
        }
    }
}
