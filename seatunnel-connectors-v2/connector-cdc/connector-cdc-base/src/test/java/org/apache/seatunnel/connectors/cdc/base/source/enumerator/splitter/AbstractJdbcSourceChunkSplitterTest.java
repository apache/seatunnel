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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
     * Verifies the decision logic in splitTableIntoChunks for non-consecutive IDs. When
     * distributionFactor > 1.0 and sampling is enabled, it should use sampling. If sampling fails
     * (returns insufficient data), it should fallback to splitUnevenlySizedChunks.
     */
    @Test
    public void testSplitTableIntoChunksDecisionLogic() throws Exception {
        TestJdbcSourceConfig config =
                new TestJdbcSourceConfig() {
                    @Override
                    public double getDistributionFactorUpper() {
                        return 100.0;
                    }

                    @Override
                    public double getDistributionFactorLower() {
                        return 0.05;
                    }

                    @Override
                    public int getSplitSize() {
                        return 100;
                    }

                    @Override
                    public boolean isSampleShardingAllow() {
                        return true;
                    }

                    @Override
                    public int getSampleShardingThreshold() {
                        return 1000; // Large default threshold that should be ignored for gap fixes
                    }
                };

        TableId tableId = new TableId("test", "test", "test");
        Column splitColumn = Column.editor().name("id").create();

        final boolean[] samplingCalled = {false};
        final boolean[] unevenlyCalled = {false};
        final boolean[] evenlyCalled = {false};

        AbstractJdbcSourceChunkSplitter splitterSuccess =
                new ConfiguredUtJdbcSourceChunkSplitter(config) {
                    @Override
                    public Object[] queryMinMax(
                            JdbcConnection jdbc, TableId tableId, Column columnName) {
                        return new Object[] {1, 50000};
                    }

                    @Override
                    public boolean isEvenlySplitColumn(Column splitColumn) {
                        return true;
                    }

                    @Override
                    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) {
                        return 1000L;
                    }

                    @Override
                    public Object[] sampleDataFromColumn(
                            JdbcConnection jdbc,
                            TableId tableId,
                            String columnName,
                            int samplingRate) {
                        samplingCalled[0] = true;
                        return new Object[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
                    }

                    @Override
                    protected List<ChunkRange> splitEvenlySizedChunks(
                            TableId tableId,
                            Object min,
                            Object max,
                            long approximateRowCnt,
                            int chunkSize,
                            int dynamicChunkSize) {
                        evenlyCalled[0] = true;
                        return Collections.emptyList();
                    }

                    @Override
                    protected List<ChunkRange> splitUnevenlySizedChunks(
                            JdbcConnection jdbc,
                            TableId tableId,
                            Column splitColumn,
                            Object min,
                            Object max,
                            int chunkSize) {
                        unevenlyCalled[0] = true;
                        return Collections.emptyList();
                    }

                    @Override
                    protected List<ChunkRange> efficientShardingThroughSampling(
                            TableId tableId,
                            Object[] sampleData,
                            long approximateRowCnt,
                            int shardCount) {
                        return Collections.emptyList();
                    }
                };

        splitterSuccess.splitTableIntoChunks(null, tableId, splitColumn);
        assertTrue(samplingCalled[0], "Should invoke sampling for sparse IDs");
        assertFalse(evenlyCalled[0], "Should not invoke arithmetic fallback");
        assertFalse(unevenlyCalled[0], "Should not invoke uneven query fallback");

        samplingCalled[0] = false;
        evenlyCalled[0] = false;
        unevenlyCalled[0] = false;

        AbstractJdbcSourceChunkSplitter splitterFallback =
                new ConfiguredUtJdbcSourceChunkSplitter(config) {
                    @Override
                    public Object[] queryMinMax(
                            JdbcConnection jdbc, TableId tableId, Column columnName) {
                        return new Object[] {1, 50000};
                    }

                    @Override
                    public boolean isEvenlySplitColumn(Column splitColumn) {
                        return true;
                    }

                    @Override
                    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) {
                        return 1000L;
                    }

                    @Override
                    public Object[] sampleDataFromColumn(
                            JdbcConnection jdbc,
                            TableId tableId,
                            String columnName,
                            int samplingRate) {
                        samplingCalled[0] = true;
                        return new Object[] {1, 50000};
                    }

                    @Override
                    protected List<ChunkRange> splitEvenlySizedChunks(
                            TableId tableId,
                            Object min,
                            Object max,
                            long approximateRowCnt,
                            int chunkSize,
                            int dynamicChunkSize) {
                        evenlyCalled[0] = true;
                        return Collections.emptyList();
                    }

                    @Override
                    protected List<ChunkRange> splitUnevenlySizedChunks(
                            JdbcConnection jdbc,
                            TableId tableId,
                            Column splitColumn,
                            Object min,
                            Object max,
                            int chunkSize) {
                        unevenlyCalled[0] = true;
                        return Collections.emptyList();
                    }
                };

        splitterFallback.splitTableIntoChunks(null, tableId, splitColumn);
        assertTrue(samplingCalled[0], "Should attempt sampling for sparse IDs");
        assertFalse(evenlyCalled[0], "Should NOT fallback to arithmetic splitting if sparse");
        assertTrue(
                unevenlyCalled[0],
                "Should fallback to exact query splitting when sampler is defeated");
    }

    /**
     * Verifies the actual chunk boundaries produced for a gap-skewed table (e.g., simulating
     * #10270). This tests both the sampling-succeeds and sampling-falls-back boundary math
     * end-to-end.
     */
    @Test
    public void testSplitTableIntoChunksBoundaryCorrectnessWithSparseKeys() throws Exception {
        TestJdbcSourceConfig config =
                new TestJdbcSourceConfig() {
                    @Override
                    public double getDistributionFactorUpper() {
                        return 100.0;
                    }

                    @Override
                    public double getDistributionFactorLower() {
                        return 0.05;
                    }

                    @Override
                    public int getSplitSize() {
                        return 2;
                    }

                    @Override
                    public boolean isSampleShardingAllow() {
                        return true;
                    }
                };

        TableId tableId = new TableId("test", "test", "test");
        Column splitColumn = Column.editor().name("id").create();

        // 1. Test fallback to exact-boundary query when sample is too sparse
        // PKs: {1, 2, 5, 6, 100} -> min=1, max=100, approxRowCnt=5, chunkSize=2
        // shardCount = ceil(5/2) = 3
        // distributionFactor = (100 - 1 + 1)/5 = 20.0 (evenly distributed, but > 1.0 gap skew)
        // inverseSamplingRate = min(1000, chunkSize=2) = 2
        // Simulated sample for MOD((id-1), 2) = 0 -> {1, 5}, length 2 < shardCount(3) -> falls back
        // to uneven
        AbstractJdbcSourceChunkSplitter splitterFallback =
                new ConfiguredUtJdbcSourceChunkSplitter(config) {
                    @Override
                    public Object[] queryMinMax(
                            JdbcConnection jdbc, TableId tableId, Column columnName) {
                        return new Object[] {1, 100};
                    }

                    @Override
                    public boolean isEvenlySplitColumn(Column splitColumn) {
                        return true;
                    }

                    @Override
                    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) {
                        return 5L;
                    }

                    @Override
                    public Object[] sampleDataFromColumn(
                            JdbcConnection jdbc,
                            TableId tableId,
                            String columnName,
                            int samplingRate) {
                        // Return the sparse sample simulating MOD() filtering
                        return new Object[] {1, 5};
                    }

                    @Override
                    public Object queryNextChunkMax(
                            JdbcConnection jdbc,
                            TableId tableId,
                            String columnName,
                            int chunkSize,
                            Object includedLowerBound) {
                        // Exact boundary querying mock for {1, 2, 5, 6, 100}
                        int lower = includedLowerBound == null ? 0 : (Integer) includedLowerBound;
                        if (lower < 2) return 2;
                        if (lower < 6) return 6;
                        if (lower < 100) return 100;
                        return null;
                    }
                };

        List<ChunkRange> fallbackChunks =
                splitterFallback.splitTableIntoChunks(null, tableId, splitColumn);
        assertEquals(3, fallbackChunks.size());
        assertEquals(ChunkRange.of(null, 2), fallbackChunks.get(0));
        assertEquals(ChunkRange.of(2, 6), fallbackChunks.get(1));
        assertEquals(ChunkRange.of(6, null), fallbackChunks.get(2));

        // 2. Test successful sampling-based boundary generation
        // Same table, but chunkSize=1 -> shardCount=5 -> inverseSamplingRate=1
        // sample contains all rows: {1, 2, 5, 6, 100}, length 5 >= shardCount(5) -> efficient
        // sharding
        TestJdbcSourceConfig configSample =
                new TestJdbcSourceConfig() {
                    @Override
                    public double getDistributionFactorUpper() {
                        return 100.0;
                    }

                    @Override
                    public double getDistributionFactorLower() {
                        return 0.05;
                    }

                    @Override
                    public int getSplitSize() {
                        return 1;
                    }

                    @Override
                    public boolean isSampleShardingAllow() {
                        return true;
                    }
                };

        AbstractJdbcSourceChunkSplitter splitterSample =
                new ConfiguredUtJdbcSourceChunkSplitter(configSample) {
                    @Override
                    public Object[] queryMinMax(
                            JdbcConnection jdbc, TableId tableId, Column columnName) {
                        return new Object[] {1, 100};
                    }

                    @Override
                    public boolean isEvenlySplitColumn(Column splitColumn) {
                        return true;
                    }

                    @Override
                    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) {
                        return 5L;
                    }

                    @Override
                    public Object[] sampleDataFromColumn(
                            JdbcConnection jdbc,
                            TableId tableId,
                            String columnName,
                            int samplingRate) {
                        // inverseSamplingRate=1 means all 5 rows are sampled
                        return new Object[] {1, 2, 5, 6, 100};
                    }
                };

        List<ChunkRange> sampleChunks =
                splitterSample.splitTableIntoChunks(null, tableId, splitColumn);
        assertEquals(6, sampleChunks.size());
        assertEquals(ChunkRange.of(null, 1), sampleChunks.get(0));
        assertEquals(ChunkRange.of(1, 2), sampleChunks.get(1));
        assertEquals(ChunkRange.of(2, 5), sampleChunks.get(2));
        assertEquals(ChunkRange.of(5, 6), sampleChunks.get(3));
        assertEquals(ChunkRange.of(6, 100), sampleChunks.get(4));
        assertEquals(ChunkRange.of(100, null), sampleChunks.get(5));
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
