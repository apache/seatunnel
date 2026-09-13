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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ObjectUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicChunkSplitterTest {

    @Test
    public void testPostgresGenerateSplitQuerySQL() {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .build();
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .sourceType("int4")
                                                .dataType(BasicType.INT_TYPE)
                                                .build()))
                        .build();

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);

        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db1", "schema1", "table1"),
                        "split1",
                        null,
                        "id",
                        BasicType.INT_TYPE,
                        1,
                        10);
        String splitQuerySQL = splitter.createDynamicSplitQuerySQL(split, tableSchema);
        Assertions.assertEquals(
                "SELECT * FROM \"db1\".\"schema1\".\"table1\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitQuerySQL);

        split =
                new JdbcSourceSplit(
                        TablePath.of("db1", "schema1", "table1"),
                        "split1",
                        "select * from table1",
                        "id",
                        BasicType.INT_TYPE,
                        1,
                        10);
        splitQuerySQL = splitter.createDynamicSplitQuerySQL(split, tableSchema);
        Assertions.assertEquals(
                "SELECT * FROM (select * from table1) tmp WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitQuerySQL);

        tableSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .sourceType("uuid")
                                                .dataType(BasicType.INT_TYPE)
                                                .build()))
                        .build();
        split =
                new JdbcSourceSplit(
                        TablePath.of("db1", "schema1", "table1"),
                        "split1",
                        "select * from table1",
                        "id",
                        BasicType.INT_TYPE,
                        1,
                        10);
        splitQuerySQL = splitter.createDynamicSplitQuerySQL(split, tableSchema);
        Assertions.assertEquals(
                "SELECT * FROM (select * from table1) tmp WHERE \"id\"::text >= ? AND NOT (\"id\"::text = ?) AND \"id\"::text <= ?",
                splitQuerySQL);
    }

    @Test
    public void testSampleShardingAllowConfigParsing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("table_path", "test.table1");
        JdbcSourceConfig defaultConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));

        Assertions.assertTrue(
                defaultConfig.isSplitSampleShardingAllow(),
                "Default value of split.allow-sampling should be true");

        configMap.put("split.allow-sampling", false);
        JdbcSourceConfig disabledConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        Assertions.assertFalse(
                disabledConfig.isSplitSampleShardingAllow(),
                "split.allow-sampling should be false when explicitly set");
    }

    @Test
    public void testEfficientShardingThroughSampling() throws NoSuchMethodException {
        TablePath tablePath = new TablePath("db", "xe", "table");

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1}, 1000, 1),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 4),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 5),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 6),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, 6),
                        DynamicChunkSplitter.ChunkRange.of(6, null)));
    }

    /**
     * When enable_concurrent_read=false, generateSplits must return exactly one full-table split
     * with no split key, avoiding any MIN/MAX analysis on the database.
     */
    @Test
    public void testSingleSplitWhenConcurrentReadDisabled() throws Exception {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .enableConcurrentRead(false)
                        .build();

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder().tablePath(TablePath.of("db", "schema", "table")).build();

        Collection<JdbcSourceSplit> splits = splitter.generateSplits(table);

        assertEquals(1, splits.size());
        JdbcSourceSplit split = splits.iterator().next();
        assertNull(split.getSplitKeyName());
        assertNull(split.getSplitStart());
        assertNull(split.getSplitEnd());
    }

    /** The enable_concurrent_read option must default to true so existing jobs are unaffected. */
    @Test
    public void testEnableConcurrentReadOptionDefaultIsTrue() {
        assertTrue(JdbcSourceOptions.ENABLE_CONCURRENT_READ.defaultValue());
    }

    /**
     * Covers bounded range fallback when the approximate row count is zero or negative. Verifies
     * integer, short, and byte ranges split correctly, oversized ranges collapse to one chunk,
     * unsafe numeric ranges collapse to one chunk, and bad arguments throw.
     */
    @Test
    public void testSplitEvenlySizedChunksByRangeWhenApproximateRowCountUnavailable() {
        TablePath tablePath = TablePath.of("db", "xe", "table");

        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(tablePath, 1, 5, 2, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, (short) 1, (short) 5, 2, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, (short) 3),
                        DynamicChunkSplitter.ChunkRange.of((short) 3, (short) 5),
                        DynamicChunkSplitter.ChunkRange.of((short) 5, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, (byte) 1, (byte) 5, 2, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, (byte) 3),
                        DynamicChunkSplitter.ChunkRange.of((byte) 3, (byte) 5),
                        DynamicChunkSplitter.ChunkRange.of((byte) 5, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(tablePath, 1, 100, 2, 10),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, (short) 1, (short) 5, 100000, 10),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, (byte) 1, (byte) 5, 100000, 10),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, 1.0E20D, 1.0E20D + 1.0E10D, 1, 10),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));
        check(
                DynamicChunkSplitter.splitEvenlySizedChunksByRange(
                        tablePath, Double.NaN, 5.0D, 2, 10),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> DynamicChunkSplitter.splitEvenlySizedChunksByRange(tablePath, 1, 5, 0, 10));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> DynamicChunkSplitter.splitEvenlySizedChunksByRange(tablePath, 1, 5, 2, 0));
    }

    private void check(
            List<DynamicChunkSplitter.ChunkRange> a, List<DynamicChunkSplitter.ChunkRange> b) {
        checkRule(b);
        assertEquals(a, b);
    }

    private void checkRule(List<DynamicChunkSplitter.ChunkRange> a) {
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
                assertTrue(
                        ObjectUtils.compare(a.get(i).getChunkEnd(), a.get(i).getChunkStart()) > 0);
            }
        }
    }

    /** Without a where condition the table must pass through unchanged. */
    @Test
    public void testApplyWhereConditionReturnsSameTableWhenNoWhereCondition() {
        JdbcSourceConfig config = buildWhereConditionConfig(null);
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("db", "schema", "table"))
                        .query("SELECT id, name FROM table")
                        .build();

        assertSame(table, splitter.applyWhereCondition(table));
    }

    /** The user query must be wrapped with the where condition for split metadata queries. */
    @Test
    public void testApplyWhereConditionWrapsUserQuery() {
        JdbcSourceConfig config = buildWhereConditionConfig("where id > 100");
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("db", "schema", "table"))
                        .query("SELECT id, name FROM table")
                        .build();

        JdbcSourceTable wrapped = splitter.applyWhereCondition(table);

        assertEquals(
                "SELECT * FROM (SELECT id, name FROM table) tmp WHERE id > 100",
                wrapped.getQuery());
        assertSame(table.getTablePath(), wrapped.getTablePath());
        assertEquals(table.getPartitionColumn(), wrapped.getPartitionColumn());
        assertSame(table.getCatalogTable(), wrapped.getCatalogTable());
    }

    /** A where-referenced column missing from a narrow custom query must be auto-added. */
    @Test
    public void testApplyWhereConditionAutoAddsMissingFieldForNarrowQuery() {
        JdbcSourceConfig config = buildWhereConditionConfig("where status > 1");
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("db", "schema", "table"))
                        .query("SELECT id, name FROM table")
                        .build();

        JdbcSourceTable wrapped = splitter.applyWhereCondition(table);

        // Without the auto-add, the wrapped subquery would not expose "status" and the
        // split-metadata queries would fail with a "column not found" SQL error.
        assertEquals(
                "SELECT * FROM (SELECT id, name , status FROM table) tmp WHERE status > 1",
                wrapped.getQuery());
    }

    /** When no query is configured the table identifier must be used as the wrapped base. */
    @Test
    public void testApplyWhereConditionFallsBackToTableIdentifierWithoutQuery() {
        JdbcSourceConfig config = buildWhereConditionConfig("where id > 100");
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder().tablePath(TablePath.of("db", "schema", "table")).build();

        JdbcSourceTable wrapped = splitter.applyWhereCondition(table);

        // The base query is "SELECT * FROM <tableIdentifier>"; for the default Postgres
        // dialect the tableIdentifier is the fully quoted path. We only assert that the
        // wrapper is applied and the original table path is preserved.
        assertEquals(
                "SELECT * FROM (SELECT * FROM \"db\".\"schema\".\"table\") tmp WHERE id > 100",
                wrapped.getQuery());
        assertSame(table.getTablePath(), wrapped.getTablePath());
    }

    private static JdbcSourceConfig buildWhereConditionConfig(String whereCondition) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:postgresql://localhost:5432/test");
        options.put("driver", "org.postgresql.Driver");
        if (whereCondition != null) {
            options.put(JdbcSourceOptions.WHERE_CONDITION.key(), whereCondition);
        }
        return JdbcSourceConfig.of(ReadonlyConfig.fromMap(options));
    }
}
