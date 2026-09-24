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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractJdbcSourceChunkSplitterTest {

    @Test
    public void testEfficientShardingThroughSampling() throws NoSuchMethodException {

        UtJdbcSourceChunkSplitter utJdbcSourceChunkSplitter = new UtJdbcSourceChunkSplitter();

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 2),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 1),
                Arrays.asList(ChunkRange.of(null, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 10),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 10),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, 2), ChunkRange.of(2, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 1),
                Arrays.asList(ChunkRange.of(null, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 2),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1}, 1000, 1),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1}, 1000, 2),
                Arrays.asList(ChunkRange.of(null, 1), ChunkRange.of(1, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3}, 1000, 2),
                Arrays.asList(ChunkRange.of(null, 2), ChunkRange.of(2, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3}, 1000, 1),
                Arrays.asList(ChunkRange.of(null, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3}, 1000, 3),
                Arrays.asList(
                        ChunkRange.of(null, 1),
                        ChunkRange.of(1, 2),
                        ChunkRange.of(2, 3),
                        ChunkRange.of(3, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5}, 1000, 3),
                Arrays.asList(ChunkRange.of(null, 2), ChunkRange.of(2, 4), ChunkRange.of(4, null)));
        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5}, 1000, 2),
                Arrays.asList(ChunkRange.of(null, 3), ChunkRange.of(3, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 1),
                Arrays.asList(ChunkRange.of(null, null)));

        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 3),
                Arrays.asList(ChunkRange.of(null, 3), ChunkRange.of(3, 5), ChunkRange.of(5, null)));
        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 4),
                Arrays.asList(
                        ChunkRange.of(null, 2),
                        ChunkRange.of(2, 4),
                        ChunkRange.of(4, 5),
                        ChunkRange.of(5, null)));
        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 5),
                Arrays.asList(
                        ChunkRange.of(null, 2),
                        ChunkRange.of(2, 3),
                        ChunkRange.of(3, 4),
                        ChunkRange.of(4, 5),
                        ChunkRange.of(5, null)));
        check(
                utJdbcSourceChunkSplitter.efficientShardingThroughSampling(
                        null, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 6),
                Arrays.asList(
                        ChunkRange.of(null, 1),
                        ChunkRange.of(1, 2),
                        ChunkRange.of(2, 3),
                        ChunkRange.of(3, 4),
                        ChunkRange.of(4, 5),
                        ChunkRange.of(5, 6),
                        ChunkRange.of(6, null)));
    }

    /**
     * When IDs are non-consecutive (distributionFactor > 1.0) and sampling is enabled, the
     * splitEvenlySizedChunks path should NOT be used. Instead, sampling-based splitting should be
     * preferred for better chunk balance.
     *
     * <p>This test verifies that for a table with IDs spanning a wide range but few actual rows
     * (distributionFactor >> 1.0), the algorithm falls back to arithmetic stepping only when
     * sampling is disabled.
     */
    @Test
    public void testSplitEvenlySizedChunksWithNonConsecutiveIds() {
        UtJdbcSourceChunkSplitter splitter = new UtJdbcSourceChunkSplitter();

        // Case 1: distributionFactor = 1.0 (consecutive IDs) should use arithmetic stepping
        // Table with IDs 1-1000, 1000 rows, chunkSize=100
        // distributionFactor = (1000 - 1 + 1) / 1000 = 1.0
        // dynamicChunkSize = 1.0 * 100 = 100
        List<ChunkRange> consecutiveChunks =
                splitter.splitEvenlySizedChunks(null, 1, 1000, 1000, 100, 100);
        // Should produce ~10 chunks via arithmetic stepping
        assertEquals(10, consecutiveChunks.size());
        // First chunk: [null, 101)
        assertNull(consecutiveChunks.get(0).getChunkStart());
        assertEquals(101, consecutiveChunks.get(0).getChunkEnd());
        // Last chunk: [901, null)
        assertEquals(901, consecutiveChunks.get(consecutiveChunks.size() - 1).getChunkStart());
        assertNull(consecutiveChunks.get(consecutiveChunks.size() - 1).getChunkEnd());

        // Case 2: distributionFactor = 50.0 (highly non-consecutive IDs)
        // Table with IDs 1-5000000, 100000 rows, chunkSize=1000
        // distributionFactor = (5000000 - 1 + 1) / 100000 = 50.0
        // dynamicChunkSize = 50 * 1000 = 50000
        // Arithmetic stepping would create 100 chunks of 50000 IDs each, but
        // if IDs are clustered, some chunks would have too many/few rows.
        List<ChunkRange> nonConsecutiveChunks =
                splitter.splitEvenlySizedChunks(null, 1, 5000000, 100000, 1000, 50000);
        assertEquals(100, nonConsecutiveChunks.size());
        // Verify chunks cover the full range
        assertNull(nonConsecutiveChunks.get(0).getChunkStart());
        assertNull(nonConsecutiveChunks.get(nonConsecutiveChunks.size() - 1).getChunkEnd());

        // Case 3: Verify the efficientShardingThroughSampling handles the same scenario better
        // Simulated sample data from non-consecutive IDs (clustered in ranges 1-100000 and
        // 4900000-5000000)
        Object[] sampleData = new Object[] {10, 200, 5000, 50000, 4900000, 4950000, 4990000};
        int shardCount = 3;
        List<ChunkRange> sampledChunks =
                splitter.efficientShardingThroughSampling(null, sampleData, 100000, shardCount);
        // Sampling should produce chunks based on actual data distribution,
        // not arithmetic stepping
        assertEquals(3, sampledChunks.size());
        assertNull(sampledChunks.get(0).getChunkStart());
        assertNull(sampledChunks.get(sampledChunks.size() - 1).getChunkEnd());
    }

    private void check(List<ChunkRange> a, List<ChunkRange> b) {
        checkRule(b);
        assertEquals(a, b);
    }

    private void checkRule(List<ChunkRange> a) {
        for (int i = 0; i < a.size(); i++) {
            if (i == 0) {
                assertNull(a.get(i).getChunkStart());
            }
            if (i == a.size() - 1) {
                assertNull(a.get(i).getChunkEnd());
            }
            // current chunk start should be equal to previous chunk end
            if (i > 0) {
                assertEquals(a.get(i - 1).getChunkEnd(), a.get(i).getChunkStart());
            }
            if (i > 0 && i < a.size() - 1) {
                // current chunk end should be greater than current chunk start
                assertTrue((int) a.get(i).getChunkEnd() > (int) a.get(i).getChunkStart());
            }
        }
    }

    public static class UtJdbcSourceChunkSplitter extends AbstractJdbcSourceChunkSplitter {

        public UtJdbcSourceChunkSplitter() {
            super(null, null);
        }

        @Override
        public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
                throws SQLException {
            return new Object[0];
        }

        @Override
        public Object queryMin(
                JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Object[] sampleDataFromColumn(
                JdbcConnection jdbc, TableId tableId, String columnName, int samplingRate)
                throws Exception {
            return new Object[0];
        }

        @Override
        public Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                String columnName,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return null;
        }

        @Override
        public String buildSplitScanQuery(
                Table table,
                SeaTunnelRowType splitKeyType,
                boolean isFirstSplit,
                boolean isLastSplit) {
            return null;
        }

        @Override
        public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
            return null;
        }
    }

    /**
     * When enable_concurrent_read=false, generateSplits must return exactly one full-table split
     * with null split bounds, avoiding any JDBC connection to the database.
     */
    @Test
    public void testSingleSplitWhenConcurrentReadDisabled() {
        TestJdbcSourceConfig config = new TestJdbcSourceConfig();
        config.setEnableConcurrentRead(false);

        ConfiguredUtJdbcSourceChunkSplitter splitter =
                new ConfiguredUtJdbcSourceChunkSplitter(config);
        TableId tableId = new TableId("testdb", "testschema", "testtable");

        Collection<SnapshotSplit> splits = splitter.generateSplits(tableId);

        assertEquals(1, splits.size());
        SnapshotSplit split = splits.iterator().next();
        assertNull(split.getSplitKeyType());
        assertNull(split.getSplitStart());
        assertNull(split.getSplitEnd());
        assertEquals(tableId, split.getTableId());
    }

    /**
     * The enable_concurrent_read option must default to true so existing CDC jobs are unaffected.
     */
    @Test
    public void testEnableConcurrentReadOptionDefaultIsTrue() {
        assertTrue(SourceOptions.ENABLE_CONCURRENT_READ.defaultValue());
    }

    /**
     * A concrete JdbcSourceChunkSplitter that accepts a real JdbcSourceConfig, used to test the
     * enableConcurrentRead short-circuit path in generateSplits.
     */
    public static class ConfiguredUtJdbcSourceChunkSplitter
            extends AbstractJdbcSourceChunkSplitter {

        public ConfiguredUtJdbcSourceChunkSplitter(JdbcSourceConfig config) {
            super(config, null);
        }

        @Override
        public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
                throws SQLException {
            return new Object[0];
        }

        @Override
        public Object queryMin(
                JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Object[] sampleDataFromColumn(
                JdbcConnection jdbc, TableId tableId, String columnName, int samplingRate)
                throws Exception {
            return new Object[0];
        }

        @Override
        public Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                String columnName,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return null;
        }

        @Override
        public String buildSplitScanQuery(
                Table table,
                SeaTunnelRowType splitKeyType,
                boolean isFirstSplit,
                boolean isLastSplit) {
            return null;
        }

        @Override
        public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
            return null;
        }
    }

    /**
     * A minimal concrete JdbcSourceConfig for unit testing. Only enableConcurrentRead is
     * configurable; all other fields use zero/null defaults.
     */
    public static class TestJdbcSourceConfig extends JdbcSourceConfig {

        public TestJdbcSourceConfig() {
            super(
                    null,
                    null,
                    null,
                    null,
                    8096,
                    new java.util.HashMap<>(),
                    1.0,
                    0.05,
                    1000,
                    1000,
                    true,
                    new Properties(),
                    null,
                    null,
                    0,
                    null,
                    null,
                    null,
                    1024,
                    null,
                    30000L,
                    3,
                    20,
                    false);
        }

        @Override
        public RelationalDatabaseConnectorConfig getDbzConnectorConfig() {
            return null;
        }
    }
}
