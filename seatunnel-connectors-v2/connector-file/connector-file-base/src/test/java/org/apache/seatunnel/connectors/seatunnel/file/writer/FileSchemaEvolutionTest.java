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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractWriteStrategy;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Unit tests for schema evolution logic in AbstractWriteStrategy. Uses a minimal NoOpWriteStrategy
 * inner class that avoids any filesystem I/O.
 */
public class FileSchemaEvolutionTest {

    private static final TableIdentifier TABLE_ID =
            TableIdentifier.of("catalog", "db", "test_table");

    /** Minimal row type: id (INT), name (STRING), age (INT) */
    private static final SeaTunnelRowType BASE_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "age"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                    });

    private static FileSinkConfig schemaEvolutionEnabledConfig() {
        Config config =
                ConfigFactory.parseString(
                        "path = \"/tmp/test\"\n"
                                + "file_format_type = \"parquet\"\n"
                                + "schema_evolution_enabled = true");
        return new FileSinkConfig(config, BASE_ROW_TYPE);
    }

    private static FileSinkConfig schemaEvolutionDisabledConfig() {
        Config config =
                ConfigFactory.parseString(
                        "path = \"/tmp/test\"\n" + "file_format_type = \"parquet\"");
        return new FileSinkConfig(config, BASE_ROW_TYPE);
    }

    /**
     * Config where sink_columns uses mixed case (e.g., "AGE" instead of "age"). This tests the
     * case-insensitive matching in updateSinkColumnNames: CDC events carry schema-exact names, but
     * user-configured sink_columns may have different capitalisation.
     */
    private static FileSinkConfig schemaEvolutionEnabledConfigWithUppercaseSinkColumns() {
        // "AGE" uppercase in config, but schema has "age" — real mismatch scenario
        Config config =
                ConfigFactory.parseString(
                        "path = \"/tmp/test\"\n"
                                + "file_format_type = \"parquet\"\n"
                                + "sink_columns = [\"id\", \"NAME\", \"AGE\"]\n"
                                + "schema_evolution_enabled = true");
        return new FileSinkConfig(config, BASE_ROW_TYPE);
    }

    // ── Helper to build a NoOpWriteStrategy with a given FileSinkConfig ──────────

    private static NoOpWriteStrategy buildStrategy(FileSinkConfig config) {
        NoOpWriteStrategy strategy = new NoOpWriteStrategy(config);
        // Manually set seaTunnelRowType (normally set via setCatalogTable)
        strategy.setSeaTunnelRowTypeForTest(BASE_ROW_TYPE);
        return strategy;
    }

    // ── ADD_COLUMN ────────────────────────────────────────────────────────────────

    @Test
    public void testAddColumnAppendsToSinkColumnNamesAndRebuildsIndex() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        PhysicalColumn newCol =
                PhysicalColumn.builder()
                        .name("email")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        AlterTableAddColumnEvent event = AlterTableAddColumnEvent.add(TABLE_ID, newCol);
        strategy.applySchemaChange(event);

        List<String> names = strategy.getSinkColumnNames();
        Assertions.assertTrue(names.contains("email"), "email should be in sinkColumnNames");
        Assertions.assertEquals(4, names.size(), "should now have 4 columns");

        // Index for email should be 3 (last position in new rowType)
        SeaTunnelRowType updated = strategy.getSeaTunnelRowType();
        int emailIdx = Arrays.asList(updated.getFieldNames()).indexOf("email");
        Assertions.assertTrue(strategy.getSinkColumnsIndexInRow().contains(emailIdx));
    }

    @Test
    public void testAddColumnFirstPrependsToSinkColumnNames() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        PhysicalColumn newCol =
                PhysicalColumn.builder()
                        .name("prefix")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        AlterTableAddColumnEvent event = AlterTableAddColumnEvent.addFirst(TABLE_ID, newCol);
        strategy.applySchemaChange(event);

        Assertions.assertEquals("prefix", strategy.getSinkColumnNames().get(0));
    }

    // ── DROP_COLUMN ───────────────────────────────────────────────────────────────

    @Test
    public void testDropColumnRemovesFromSinkColumnNamesAndRebuildsIndex() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(TABLE_ID, "age");
        strategy.applySchemaChange(event);

        Assertions.assertFalse(
                strategy.getSinkColumnNames().contains("age"), "age should be removed");
        Assertions.assertEquals(2, strategy.getSinkColumnNames().size());

        // No index for 'age' any more
        SeaTunnelRowType updated = strategy.getSeaTunnelRowType();
        Assertions.assertFalse(
                Arrays.asList(updated.getFieldNames()).contains("age"),
                "age should not be in updated rowType");
    }

    // ── RENAME_COLUMN (AlterTableChangeColumnEvent) ───────────────────────────────

    @Test
    public void testRenameColumnUpdatesNameInSinkColumnNames() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        PhysicalColumn renamedCol =
                PhysicalColumn.builder()
                        .name("full_name")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        AlterTableChangeColumnEvent event =
                AlterTableChangeColumnEvent.change(TABLE_ID, "name", renamedCol);
        strategy.applySchemaChange(event);

        Assertions.assertFalse(
                strategy.getSinkColumnNames().contains("name"), "old name should be removed");
        Assertions.assertTrue(
                strategy.getSinkColumnNames().contains("full_name"), "new name should be present");

        SeaTunnelRowType updated = strategy.getSeaTunnelRowType();
        Assertions.assertTrue(
                Arrays.asList(updated.getFieldNames()).contains("full_name"),
                "full_name should be in updated rowType");
    }

    // ── UPDATE_COLUMN / MODIFY_COLUMN ─────────────────────────────────────────────

    @Test
    public void testModifyColumnTypeDoesNotChangeSinkColumnNames() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        // Change 'age' from INT to LONG
        PhysicalColumn modifiedCol =
                PhysicalColumn.builder()
                        .name("age")
                        .dataType(BasicType.LONG_TYPE)
                        .nullable(true)
                        .build();
        AlterTableModifyColumnEvent event =
                AlterTableModifyColumnEvent.modify(TABLE_ID, modifiedCol);
        strategy.applySchemaChange(event);

        // Column names should be unchanged
        Assertions.assertEquals(Arrays.asList("id", "name", "age"), strategy.getSinkColumnNames());

        // But the type in rowType should be updated to LONG
        int ageIdx = Arrays.asList(strategy.getSeaTunnelRowType().getFieldNames()).indexOf("age");
        Assertions.assertEquals(
                BasicType.LONG_TYPE, strategy.getSeaTunnelRowType().getFieldType(ageIdx));
    }

    // ── Batch / AlterTableColumnsEvent ────────────────────────────────────────────

    @Test
    public void testBatchColumnsEventAppliedAsOneSchemaChange() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        PhysicalColumn newScore =
                PhysicalColumn.builder()
                        .name("score")
                        .dataType(BasicType.INT_TYPE)
                        .nullable(true)
                        .build();

        AlterTableColumnsEvent batch =
                new AlterTableColumnsEvent(
                        TABLE_ID,
                        Arrays.asList(
                                new AlterTableDropColumnEvent(TABLE_ID, "age"),
                                AlterTableAddColumnEvent.add(TABLE_ID, newScore)));
        strategy.applySchemaChange(batch);

        // age dropped, score added
        Assertions.assertFalse(strategy.getSinkColumnNames().contains("age"));
        Assertions.assertTrue(strategy.getSinkColumnNames().contains("score"));
        // finishAndCloseFile called exactly once (batch counts as single schema change)
        Assertions.assertEquals(1, strategy.finishAndCloseFileCalls);
    }

    // ── Disabled flag: applySchemaChange is a no-op ───────────────────────────────

    @Test
    public void testDisabledFlagMakesApplySchemaChangeNoOp() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionDisabledConfig());

        // Initial state: columns [id, name, age]
        List<String> originalNames = new ArrayList<>(strategy.getSinkColumnNames());
        List<Integer> originalIndices = new ArrayList<>(strategy.getSinkColumnsIndexInRow());

        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(TABLE_ID, "age");
        strategy.applySchemaChange(event);

        // Nothing should have changed
        Assertions.assertEquals(originalNames, strategy.getSinkColumnNames());
        Assertions.assertEquals(originalIndices, strategy.getSinkColumnsIndexInRow());
        Assertions.assertEquals(0, strategy.finishAndCloseFileCalls);
    }

    // ── finishAndCloseFile called on each schema change ───────────────────────────

    @Test
    public void testFinishAndCloseFileCalledOnSchemaChange() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        strategy.applySchemaChange(new AlterTableDropColumnEvent(TABLE_ID, "age"));
        Assertions.assertEquals(1, strategy.finishAndCloseFileCalls);

        // Rebuild seaTunnelRowType to have only id + name for the second event
        PhysicalColumn newCol =
                PhysicalColumn.builder()
                        .name("extra")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        strategy.applySchemaChange(AlterTableAddColumnEvent.add(TABLE_ID, newCol));
        Assertions.assertEquals(2, strategy.finishAndCloseFileCalls);
    }

    // ── getFieldSafe bounds check ─────────────────────────────────────────────────

    @Test
    public void testGetFieldSafeReturnsNullWhenIndexOutOfBounds() {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"val0", "val1", 42});

        // In-bounds: normal access
        Assertions.assertEquals("val0", strategy.callGetFieldSafe(row, 0));
        Assertions.assertEquals(42, strategy.callGetFieldSafe(row, 2));

        // Out-of-bounds: returns null instead of ArrayIndexOutOfBoundsException
        Assertions.assertNull(strategy.callGetFieldSafe(row, 5));
        Assertions.assertNull(strategy.callGetFieldSafe(row, 3));
    }

    // ── IndexInRow stays consistent after ADD then DROP ───────────────────────────

    @Test
    public void testIndexInRowConsistencyAfterAddThenDrop() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());

        // Add email
        PhysicalColumn emailCol =
                PhysicalColumn.builder()
                        .name("email")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        strategy.applySchemaChange(AlterTableAddColumnEvent.add(TABLE_ID, emailCol));

        // Drop id
        strategy.applySchemaChange(new AlterTableDropColumnEvent(TABLE_ID, "id"));

        // Remaining: name, age, email
        Assertions.assertFalse(strategy.getSinkColumnNames().contains("id"));
        Assertions.assertTrue(strategy.getSinkColumnNames().contains("name"));
        Assertions.assertTrue(strategy.getSinkColumnNames().contains("age"));
        Assertions.assertTrue(strategy.getSinkColumnNames().contains("email"));

        // Every index in sinkColumnsIndexInRow should be within bounds of the updated rowType
        int numFields = strategy.getSeaTunnelRowType().getTotalFields();
        for (int idx : strategy.getSinkColumnsIndexInRow()) {
            Assertions.assertTrue(
                    idx >= 0 && idx < numFields,
                    "Index " + idx + " out of range [0, " + numFields + ")");
        }
    }

    // ── Case-insensitive DROP ─────────────────────────────────────────────────────

    @Test
    public void testDropColumnCaseInsensitiveSinkColumns() throws IOException {
        // sink_columns configured as ["id", "NAME", "AGE"] — user typed uppercase
        // CDC event carries "age" (exact schema case) — dispatcher finds it, we must
        // match it against "AGE" in sinkColumnNames via equalsIgnoreCase
        NoOpWriteStrategy strategy =
                buildStrategy(schemaEvolutionEnabledConfigWithUppercaseSinkColumns());

        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(TABLE_ID, "age");
        strategy.applySchemaChange(event);

        // "AGE" must be removed even though sinkColumnNames stored it as uppercase
        Assertions.assertFalse(
                strategy.getSinkColumnNames().stream().anyMatch(c -> c.equalsIgnoreCase("age")),
                "AGE should be removed via case-insensitive match");
        Assertions.assertEquals(2, strategy.getSinkColumnNames().size());
    }

    // ── RENAME with position change ───────────────────────────────────────────────

    @Test
    public void testRenameColumnWithPositionMoveToFirst() throws IOException {
        NoOpWriteStrategy strategy = buildStrategy(schemaEvolutionEnabledConfig());
        // Original: [id, name, age]. Move-rename "age" → "score" and put it first.
        PhysicalColumn renamedCol =
                PhysicalColumn.builder()
                        .name("score")
                        .dataType(BasicType.INT_TYPE)
                        .nullable(true)
                        .build();
        AlterTableChangeColumnEvent event =
                AlterTableChangeColumnEvent.changeFirst(TABLE_ID, "age", renamedCol);
        strategy.applySchemaChange(event);

        // "score" (was "age") should now be at index 0
        Assertions.assertEquals("score", strategy.getSinkColumnNames().get(0));
        Assertions.assertFalse(strategy.getSinkColumnNames().contains("age"));
    }

    @Test
    public void testRenameCaseInsensitiveSinkColumns() throws IOException {
        // sink_columns configured as ["id", "NAME", "AGE"] — user typed "NAME" uppercase.
        // CDC RENAME event sends oldColumn="name" (exact schema case), new name="full_name".
        // Our updateSinkColumnNames must find "NAME" in sinkColumnNames via equalsIgnoreCase.
        NoOpWriteStrategy strategy =
                buildStrategy(schemaEvolutionEnabledConfigWithUppercaseSinkColumns());

        PhysicalColumn renamedCol =
                PhysicalColumn.builder()
                        .name("full_name")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        // Event uses schema-exact lowercase "name" — dispatcher can find it
        AlterTableChangeColumnEvent event =
                AlterTableChangeColumnEvent.change(TABLE_ID, "name", renamedCol);
        strategy.applySchemaChange(event);

        // "NAME" (uppercase) should be replaced by "full_name"
        Assertions.assertFalse(
                strategy.getSinkColumnNames().stream().anyMatch(c -> c.equalsIgnoreCase("name")),
                "old name should be removed regardless of case in sinkColumnNames");
        Assertions.assertTrue(strategy.getSinkColumnNames().contains("full_name"));
    }

    // ── beingWrittenFile cleared even when finishAndCloseFile throws ──────────────

    @Test
    public void testBeingWrittenFileClearedEvenOnFinishAndCloseFileException() {
        // Strategy whose finishAndCloseFile throws to simulate a writer-close failure
        ThrowingWriteStrategy strategy = new ThrowingWriteStrategy(schemaEvolutionEnabledConfig());
        strategy.setSeaTunnelRowTypeForTest(BASE_ROW_TYPE);
        // Simulate a file already being tracked
        strategy.exposeBeingWrittenFile().put("NON_PARTITION", "/tmp/test/some-file.parquet");

        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(TABLE_ID, "age");
        try {
            strategy.applySchemaChange(event);
            Assertions.fail("Expected exception from finishAndCloseFile was not thrown");
        } catch (Exception e) {
            // Expected — finishAndCloseFile threw (FileConnectorException is a RuntimeException)
        }

        // Despite the exception, beingWrittenFile must be cleared (try-finally guarantee)
        Assertions.assertTrue(
                strategy.exposeBeingWrittenFile().isEmpty(),
                "beingWrittenFile must be cleared even when finishAndCloseFile throws");
        // Schema state must NOT be updated (schema change did not complete successfully)
        Assertions.assertEquals(
                3,
                strategy.getSeaTunnelRowType().getTotalFields(),
                "seaTunnelRowType must remain unchanged after failed schema change");
    }

    // ── Inner NoOp strategy ───────────────────────────────────────────────────────

    /**
     * Minimal concrete write strategy for unit-testing AbstractWriteStrategy logic. Tracks how many
     * times finishAndCloseFile() was called. No filesystem I/O is performed.
     *
     * <p>Accessor methods expose protected fields from the parent class, which is required because
     * the test class lives in a different package from AbstractWriteStrategy.
     */
    static class NoOpWriteStrategy extends AbstractWriteStrategy<Object> {
        int finishAndCloseFileCalls = 0;
        boolean onSchemaChangedCalled = false;

        NoOpWriteStrategy(FileSinkConfig config) {
            super(config);
            // Stub out needMoveFiles which is initialised in beginTransaction
            this.needMoveFiles = new LinkedHashMap<>();
        }

        // Accessors for protected fields (required when test is in a different package)
        List<String> getSinkColumnNames() {
            return sinkColumnNames;
        }

        List<Integer> getSinkColumnsIndexInRow() {
            return sinkColumnsIndexInRow;
        }

        SeaTunnelRowType getSeaTunnelRowType() {
            return seaTunnelRowType;
        }

        void setSeaTunnelRowTypeForTest(SeaTunnelRowType rowType) {
            this.seaTunnelRowType = rowType;
        }

        LinkedHashMap<String, String> exposeBeingWrittenFile() {
            return beingWrittenFile;
        }

        Object callGetFieldSafe(SeaTunnelRow row, int index) {
            return getFieldSafe(row, index);
        }

        @Override
        public void finishAndCloseFile() {
            finishAndCloseFileCalls++;
            // Don't touch needMoveFiles here — nothing to move in unit tests
        }

        @Override
        public Object getOrCreateOutputStream(String path) {
            return new Object();
        }

        @Override
        protected void onSchemaChanged() {
            onSchemaChangedCalled = true;
        }

        @Override
        public void init(
                org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf conf,
                String jobId,
                String uuidPrefix,
                int subTaskIndex) {
            // no-op — no filesystem in unit tests
        }

        @Override
        public Configuration getConfiguration(HadoopConf conf) {
            return new Configuration();
        }
    }

    /**
     * Variant that throws on finishAndCloseFile — used to verify the try-finally guarantee in
     * applySchemaChange (beingWrittenFile must be cleared even when the close fails).
     */
    static class ThrowingWriteStrategy extends NoOpWriteStrategy {
        ThrowingWriteStrategy(FileSinkConfig config) {
            super(config);
        }

        @Override
        public void finishAndCloseFile() {
            // Simulate a writer that fails to close (e.g., HDFS timeout)
            throw new org.apache.seatunnel.connectors.seatunnel.file.exception
                    .FileConnectorException(
                    org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated
                            .WRITER_OPERATION_FAILED,
                    "Simulated writer close failure for test");
        }
    }
}
