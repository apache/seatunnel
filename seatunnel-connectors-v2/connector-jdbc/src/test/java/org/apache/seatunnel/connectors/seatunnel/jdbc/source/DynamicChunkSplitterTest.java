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
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ObjectUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
     * The hash-shard parallelism cap is on by default and must bound the chunk count at the reader
     * parallelism that the source plugin sets explicitly, without changing the count in any other
     * configuration.
     */
    @Test
    public void testHashShardParallelismCapIsOnByDefaultAndOnlyCapsExplicitParallelism() {
        assertTrue(JdbcSourceOptions.SPLIT_HASH_SHARD_PARALLELISM_CAP.defaultValue());
        TablePath tablePath = TablePath.of("db", "tbl", "orders");

        assertEquals(
                4, splitterWithHashShardCap(4, true).capHashShardCount(tablePath, "code", 100));
        assertEquals(
                100, splitterWithHashShardCap(4, false).capHashShardCount(tablePath, "code", 100));
        assertEquals(
                100,
                splitterWithHashShardCap(null, true).capHashShardCount(tablePath, "code", 100));
        assertEquals(3, splitterWithHashShardCap(8, true).capHashShardCount(tablePath, "code", 3));
        assertEquals(1, splitterWithHashShardCap(4, true).capHashShardCount(tablePath, "code", 1));
    }

    private DynamicChunkSplitter splitterWithHashShardCap(Integer parallelism, boolean cap) {
        return new DynamicChunkSplitter(
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .explicitReaderParallelism(parallelism)
                        .hashShardParallelismCap(cap)
                        .build());
    }

    /**
     * The default-on cap must bound a real hash chunk plan at the {@code parallelism} written in
     * the source configuration: 950 rows with {@code split.size = 10} would plan 96 hash chunks,
     * and the four generated predicates must use the capped count in the modulus, with the residues
     * 0..3 bound once each, so the same rows stay covered by fewer whole-table scans.
     */
    @Test
    public void testHashShardCapFromPluginParallelismCapsGeneratedPredicates() throws Exception {
        Map<String, Object> options = hashSplitOptions();
        options.put("parallelism", 4);
        JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(options));

        // The upgrade contract: a source that only sets parallelism is capped by default.
        assertTrue(config.isHashShardParallelismCap(), "the hash shard cap must be on by default");
        assertEquals(Integer.valueOf(4), config.getExplicitReaderParallelism());

        DynamicChunkSplitter splitter = new EstimatedRowCountSplitter(config, 950L);
        Collection<JdbcSourceSplit> splits = splitter.generateSplits(hashSplitTable());

        assertCappedHashSplits(splits, 4);
    }

    /**
     * When the source plugin does not set {@code parallelism}, the effective reader count may come
     * from the env block, which the connector cannot observe, so the planned chunk count must not
     * change: 96 hash chunks are kept even though the cap is on by default.
     */
    @Test
    public void testHashShardCapSkippedWhenPluginDoesNotSetParallelism() throws Exception {
        JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(hashSplitOptions()));

        assertTrue(config.isHashShardParallelismCap(), "the hash shard cap must be on by default");
        assertNull(
                config.getExplicitReaderParallelism(),
                "an absent plugin parallelism must stay unset instead of guessing the reader count");

        DynamicChunkSplitter splitter = new EstimatedRowCountSplitter(config, 950L);
        Collection<JdbcSourceSplit> splits = splitter.generateSplits(hashSplitTable());

        assertCappedHashSplits(splits, 96);
    }

    /**
     * Setting {@code split.hash-shard-parallelism-cap = false} must restore the previous planning:
     * the explicit parallelism is still visible in the config, but it no longer bounds the chunk
     * count.
     */
    @Test
    public void testHashShardCapOptOutKeepsPriorChunkCount() throws Exception {
        Map<String, Object> options = hashSplitOptions();
        options.put("parallelism", 4);
        options.put("split.hash-shard-parallelism-cap", false);
        JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(options));

        assertFalse(config.isHashShardParallelismCap());
        assertEquals(Integer.valueOf(4), config.getExplicitReaderParallelism());

        DynamicChunkSplitter splitter = new EstimatedRowCountSplitter(config, 950L);
        Collection<JdbcSourceSplit> splits = splitter.generateSplits(hashSplitTable());

        assertCappedHashSplits(splits, 96);
    }

    /**
     * A restore must read the saved hash chunk predicates instead of replanning them: the
     * checkpointed splits survive state serialization with their capped modulus and residue, and
     * the reader binds the saved residue into the saved query. Replanning on restore would either
     * change the chunk count under the same saved split ids or depend on live statistics.
     */
    @Test
    public void testHashShardCapGeneratedSplitsSurviveCheckpointRestore() throws Exception {
        Map<String, Object> options = hashSplitOptions();
        options.put("parallelism", 4);
        JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(options));
        EstimatedRowCountSplitter splitter = new EstimatedRowCountSplitter(config, 950L);

        List<JdbcSourceSplit> planned = new ArrayList<>(splitter.generateSplits(hashSplitTable()));
        assertCappedHashSplits(planned, 4);

        Map<Integer, List<JdbcSourceSplit>> pendingSplits = new HashMap<>();
        pendingSplits.put(0, planned);
        JdbcSourceState state = new JdbcSourceState(new ArrayList<TablePath>(), pendingSplits);
        DefaultSerializer<JdbcSourceState> serializer = new DefaultSerializer<>();
        JdbcSourceState restoredState = serializer.deserialize(serializer.serialize(state));

        List<JdbcSourceSplit> restored = restoredState.getPendingSplits().get(0);
        assertEquals(planned, restored, "the restored hash chunks must equal the planned ones");
        assertCappedHashSplits(restored, 4);

        // The reader path binds the restored residue into the restored predicate.
        assertNotNull(splitter.generateSplitStatement(restored.get(2), hashSplitSchema()));
        assertEquals(hashSplitSql(4), splitter.preparedSql);
        assertEquals(
                Integer.valueOf(2),
                splitter.intParameters.get(1),
                "the third restored chunk must read residue 2 of the saved capped modulus");
    }

    private static Map<String, Object> hashSplitOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/testdb");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("table_path", "testdb.test_table");
        options.put("split.string-strategy", "hash");
        options.put("split.size", "10");
        return options;
    }

    private static TableSchema hashSplitSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0L, false, null, null))
                .column(PhysicalColumn.of("code", BasicType.STRING_TYPE, 0L, false, null, null))
                .build();
    }

    private static JdbcSourceTable hashSplitTable() {
        TablePath tablePath = TablePath.of("testdb", "test_table");
        return JdbcSourceTable.builder()
                .tablePath(tablePath)
                .partitionColumn("code")
                .catalogTable(
                        CatalogTable.of(
                                TableIdentifier.of("catalog", tablePath),
                                hashSplitSchema(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                ""))
                .build();
    }

    private static String hashSplitSql(int shardCount) {
        return String.format(
                "SELECT * FROM `testdb`.`test_table` WHERE ABS(CRC32(`code`) %% %d) = ?",
                shardCount);
    }

    /**
     * Asserts the generated hash chunks use one modulus for every predicate and cover the residues
     * {@code 0..shardCount-1} exactly once, so a capped plan is a partition of the same rows as the
     * uncapped one and no row is read twice or skipped.
     */
    private static void assertCappedHashSplits(Collection<JdbcSourceSplit> splits, int shardCount) {
        assertEquals(shardCount, splits.size());
        Set<Integer> residues = new HashSet<>();
        for (JdbcSourceSplit split : splits) {
            assertEquals("code", split.getSplitKeyName());
            assertEquals(hashSplitSql(shardCount), split.getSplitQuery());
            assertEquals("testdb.test_table-" + residues.size(), split.getSplitId());
            Integer residue = (Integer) split.getSplitStart();
            assertEquals(residues.size(), residue.intValue());
            assertNull(split.getSplitEnd());
            assertTrue(residues.add(residue), "residue " + residue + " is generated twice");
        }
        assertEquals(shardCount, residues.size());
        for (int residue = 0; residue < shardCount; residue++) {
            assertTrue(residues.contains(residue), "residue " + residue + " is not covered");
        }
    }

    /**
     * A splitter whose approximate row count is fixed, so the hash chunk count is deterministic
     * without a database, and whose split statements capture the generated SQL and bound
     * parameters.
     */
    private static final class EstimatedRowCountSplitter extends DynamicChunkSplitter {

        private final Connection connection;
        private String preparedSql;
        private final Map<Integer, Integer> intParameters = new HashMap<>();

        private EstimatedRowCountSplitter(JdbcSourceConfig config, long approximateRowCount)
                throws SQLException {
            super(config);
            connection = mock(Connection.class);
            Statement statement = mock(Statement.class);
            ResultSet resultSet = mock(ResultSet.class);
            ResultSetMetaData metaData = mock(ResultSetMetaData.class);
            when(connection.createStatement()).thenReturn(statement);
            when(statement.execute(anyString())).thenReturn(true);
            when(statement.executeQuery(contains("SHOW TABLE STATUS"))).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getMetaData()).thenReturn(metaData);
            when(metaData.getColumnCount()).thenReturn(18);
            when(resultSet.getLong(5)).thenReturn(approximateRowCount);
        }

        @Override
        protected Connection getOrEstablishConnection() {
            return connection;
        }

        @Override
        protected PreparedStatement createPreparedStatement(String sql) {
            this.preparedSql = sql;
            InvocationHandler handler =
                    (proxy, method, args) -> {
                        if ("setInt".equals(method.getName())) {
                            intParameters.put((Integer) args[0], (Integer) args[1]);
                        }
                        return null;
                    };
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            handler);
        }
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

    @Test
    public void testOceanBaseOracleSkipsSamplingSharding() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:oceanbase://localhost:2881/test");
        options.put("driver", "com.oceanbase.jdbc.Driver");
        options.put(JdbcSourceOptions.COMPATIBLE_MODE.key(), "oracle");
        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(JdbcSourceConfig.of(ReadonlyConfig.fromMap(options)));

        Assertions.assertFalse(splitter.shouldUseSamplingSharding(true));
    }

    @Test
    public void testSamplingShardingFollowsUserOption() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:postgresql://localhost:5432/test");
        options.put("driver", "org.postgresql.Driver");
        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(JdbcSourceConfig.of(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(splitter.shouldUseSamplingSharding(true));
        Assertions.assertFalse(splitter.shouldUseSamplingSharding(false));
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

    /**
     * Range and hash split queries never match NULL, so a nullable key column chosen automatically
     * would leave the rows holding NULL in it unread. Such a column is skipped, and a table left
     * without a split key is read as one split.
     */
    @Test
    public void testAutomaticSplitKeySkipsNullableKeyColumns() throws Exception {
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(postgresConfig());

        JdbcSourceTable nullableUniqueKeyOnly =
                table(null, null, Collections.singletonList(uniqueKey("uk_code", "code")));
        assertFalse(splitter.findSplitKey(nullableUniqueKeyOnly).isPresent());
        Collection<JdbcSourceSplit> splits = splitter.generateSplits(nullableUniqueKeyOnly);
        assertEquals(1, splits.size());
        assertNull(splits.iterator().next().getSplitKeyName());

        JdbcSourceTable nullableAndNotNullUniqueKeys =
                table(
                        null,
                        null,
                        Arrays.asList(uniqueKey("uk_code", "code"), uniqueKey("uk_no", "no")));
        assertEquals(
                "no", splitter.findSplitKey(nullableAndNotNullUniqueKeys).get().getFieldName(0));

        JdbcSourceTable nullablePrimaryKeyColumn =
                table(
                        null,
                        PrimaryKey.of("pk_code", Collections.singletonList("code")),
                        Collections.singletonList(uniqueKey("uk_no", "no")));
        assertEquals("no", splitter.findSplitKey(nullablePrimaryKeyColumn).get().getFieldName(0));
    }

    @Test
    public void testAutomaticSplitKeyKeepsNotNullKeyColumns() {
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(postgresConfig());

        JdbcSourceTable primaryKey =
                table(
                        null,
                        PrimaryKey.of("pk_id", Collections.singletonList("id")),
                        Collections.singletonList(uniqueKey("uk_no", "no")));
        assertEquals("id", splitter.findSplitKey(primaryKey).get().getFieldName(0));

        JdbcSourceTable notNullUniqueKey =
                table(null, null, Collections.singletonList(uniqueKey("uk_no", "no")));
        assertEquals("no", splitter.findSplitKey(notNullUniqueKey).get().getFieldName(0));
    }

    /** An explicit {@code partition_column} is used as configured, also when it is nullable. */
    @Test
    public void testExplicitPartitionColumnMayBeNullable() {
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(postgresConfig());

        JdbcSourceTable table = table("code", null, Collections.emptyList());
        assertEquals("code", splitter.findSplitKey(table).get().getFieldName(0));
    }

    /**
     * A {@code query} without {@code partition_column} gets no automatic split key, even when the
     * table has a NOT NULL primary key, and is read as one split; so the nullability check of the
     * automatic choice is only needed for table reads.
     */
    @Test
    public void testQueryWithoutPartitionColumnIsReadAsOneSplit() throws Exception {
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(postgresConfig());

        JdbcSourceTable query =
                table(
                                null,
                                PrimaryKey.of("pk_id", Collections.singletonList("id")),
                                Collections.singletonList(uniqueKey("uk_no", "no")))
                        .withQuery("SELECT id, code, no FROM tbl");
        assertFalse(splitter.findSplitKey(query).isPresent());
        Collection<JdbcSourceSplit> splits = splitter.generateSplits(query);
        assertEquals(1, splits.size());
        assertNull(splits.iterator().next().getSplitKeyName());

        JdbcSourceTable queryWithPartitionColumn =
                table("code", null, Collections.emptyList())
                        .withQuery("SELECT id, code, no FROM tbl");
        assertEquals("code", splitter.findSplitKey(queryWithPartitionColumn).get().getFieldName(0));
    }

    private static JdbcSourceConfig postgresConfig() {
        return JdbcSourceConfig.builder()
                .jdbcConnectionConfig(
                        JdbcConnectionConfig.builder()
                                .url("jdbc:postgresql://localhost:5432/test")
                                .driverName("org.postgresql.Driver")
                                .build())
                .build();
    }

    private static ConstraintKey uniqueKey(String name, String column) {
        return ConstraintKey.of(
                ConstraintKey.ConstraintType.UNIQUE_KEY,
                name,
                Collections.singletonList(
                        ConstraintKey.ConstraintKeyColumn.of(
                                column, ConstraintKey.ColumnSortType.ASC)));
    }

    /**
     * A table whose INT columns {@code id} and {@code no} are NOT NULL and {@code code} is
     * nullable.
     */
    private static JdbcSourceTable table(
            String partitionColumn, PrimaryKey primaryKey, List<ConstraintKey> uniqueKeys) {
        TableSchema.Builder schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0L, false, null, null))
                        .column(PhysicalColumn.of("code", BasicType.INT_TYPE, 0L, true, null, null))
                        .column(PhysicalColumn.of("no", BasicType.INT_TYPE, 0L, false, null, null))
                        .constraintKey(uniqueKeys);
        if (primaryKey != null) {
            schema.primaryKey(primaryKey);
        }
        TablePath tablePath = TablePath.of("db", "tbl");
        return JdbcSourceTable.builder()
                .tablePath(tablePath)
                .partitionColumn(partitionColumn)
                .catalogTable(
                        CatalogTable.of(
                                TableIdentifier.of("catalog", tablePath),
                                schema.build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                ""))
                .build();
    }
}
