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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * End-to-end Parquet schema-evolution test through {@link ParquetWriteStrategy}'s real I/O path (no
 * mocks, no NoOp strategy). Covers the bugs that the {@code AlterTableSchemaEventHandler} swap
 * (commit 01cc356f2) was supposed to fix and that would regress if the file-rotation contract or
 * the cached Avro schema invalidation breaks.
 */
@Slf4j
public class ParquetWriteStrategyEvolutionTest {

    private static final String BASE_PATH = "file:///tmp/seatunnel/parquet/evolution";

    private static final TableIdentifier TABLE_ID =
            TableIdentifier.of("test", null, null, "evolution_table");

    /**
     * ADD_COLUMN flow: write 3 rows of [id, name], ALTER ADD email, write 3 rows of [id, name,
     * email], close. The strategy must rotate the file on schema change so that the old rows live
     * in a Parquet file with the pre-ALTER schema and the new rows live in a Parquet file with the
     * post-ALTER schema. Read everything back and verify both halves.
     */
    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testAddColumnRotatesFileAndPreservesAllRows() throws Exception {
        SeaTunnelRowType baseRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        Map<String, Object> writeConfig = new HashMap<>();
        String tmpPath = BASE_PATH + "/add-column/tmp";
        String finalPath = BASE_PATH + "/add-column/out";
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", finalPath);
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("schema_evolution_enabled", true);

        FileSinkConfig sinkConfig =
                new FileSinkConfig(
                        ReadonlyConfig.fromConfig(ConfigFactory.parseMap(writeConfig)),
                        baseRowType);
        ParquetWriteStrategy strategy = new ParquetWriteStrategy(sinkConfig);
        ParquetReadStrategyTest.LocalConf hadoopConf =
                new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test", null, null, "evolution_table", baseRowType));
        strategy.init(hadoopConf, "evolution-test", "evolution-test", 0);
        strategy.beginTransaction(1L);

        // Phase 1: write 3 rows with pre-ALTER schema [id, name]
        strategy.write(new SeaTunnelRow(new Object[] {1, "alice"}));
        strategy.write(new SeaTunnelRow(new Object[] {2, "bob"}));
        strategy.write(new SeaTunnelRow(new Object[] {3, "carol"}));

        // ALTER TABLE ADD COLUMN email STRING
        PhysicalColumn emailCol =
                PhysicalColumn.builder()
                        .name("email")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build();
        AlterTableAddColumnEvent addEvent = AlterTableAddColumnEvent.add(TABLE_ID, emailCol);
        strategy.applySchemaChange(addEvent);
        // Mirror what the SeaTunnel framework does after a schema change: snapshot the
        // current state and begin a new transaction so the next file gets a distinct path.
        // In production, this happens via snapshotState() between record batches.
        strategy.beginTransaction(2L);

        // Phase 2: write 3 rows with post-ALTER schema [id, name, email]
        strategy.write(new SeaTunnelRow(new Object[] {4, "dave", "dave@x.com"}));
        strategy.write(new SeaTunnelRow(new Object[] {5, "eve", "eve@x.com"}));
        strategy.write(new SeaTunnelRow(new Object[] {6, "frank", "frank@x.com"}));

        strategy.finishAndCloseFile();
        strategy.close();

        // Read every file under tmpPath. ParquetReadStrategy caches readColumns per
        // instance — use a fresh reader per file to surface each file's true schema.
        ParquetReadStrategy listStrategy = new ParquetReadStrategy();
        listStrategy.init(hadoopConf);
        List<String> files = listStrategy.getFileNamesByPath(tmpPath);
        listStrategy.close();
        Assertions.assertTrue(
                files.size() >= 2,
                "expected at least 2 Parquet files (file rotation on ALTER), got " + files.size());

        int totalRows = 0;
        boolean sawPreAlterFile = false;
        boolean sawPostAlterFile = false;
        for (String file : files) {
            ParquetReadStrategy perFile = new ParquetReadStrategy();
            perFile.init(hadoopConf);
            SeaTunnelRowType fileSchema = perFile.getSeaTunnelRowTypeInfo(file);
            List<String> fields = Arrays.asList(fileSchema.getFieldNames());
            List<SeaTunnelRow> rows = readRows(perFile, file);
            perFile.close();
            totalRows += rows.size();
            if (fields.contains("email")) {
                sawPostAlterFile = true;
                Assertions.assertEquals(3, rows.size(), "post-ALTER file should hold 3 rows");
                for (SeaTunnelRow row : rows) {
                    Assertions.assertNotNull(
                            row.getField(2), "email must be populated in post-ALTER file");
                }
            } else {
                sawPreAlterFile = true;
                Assertions.assertEquals(3, rows.size(), "pre-ALTER file should hold 3 rows");
                Assertions.assertEquals(2, fileSchema.getFieldNames().length);
            }
        }
        Assertions.assertTrue(sawPreAlterFile, "should find a Parquet file with pre-ALTER schema");
        Assertions.assertTrue(
                sawPostAlterFile, "should find a Parquet file with post-ALTER schema");
        Assertions.assertEquals(6, totalRows, "expected 6 rows total across all files");
    }

    /**
     * DROP_COLUMN flow: ensures rotation also triggers on column drops and that the writer doesn't
     * try to keep writing the old wider schema. Writes [id, name, age] then drops age then writes
     * [id, name] — must produce a pre-DROP file with 3 columns and a post-DROP file with 2.
     */
    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testDropColumnRotatesFile() throws Exception {
        SeaTunnelRowType baseRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        Map<String, Object> writeConfig = new HashMap<>();
        String tmpPath = BASE_PATH + "/drop-column/tmp";
        String finalPath = BASE_PATH + "/drop-column/out";
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", finalPath);
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("schema_evolution_enabled", true);

        FileSinkConfig sinkConfig =
                new FileSinkConfig(
                        ReadonlyConfig.fromConfig(ConfigFactory.parseMap(writeConfig)),
                        baseRowType);
        ParquetWriteStrategy strategy = new ParquetWriteStrategy(sinkConfig);
        ParquetReadStrategyTest.LocalConf hadoopConf =
                new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test", null, null, "evolution_table", baseRowType));
        strategy.init(hadoopConf, "drop-test", "drop-test", 0);
        strategy.beginTransaction(1L);

        strategy.write(new SeaTunnelRow(new Object[] {1, "alice", 30}));
        strategy.write(new SeaTunnelRow(new Object[] {2, "bob", 25}));

        AlterTableDropColumnEvent dropEvent = new AlterTableDropColumnEvent(TABLE_ID, "age");
        strategy.applySchemaChange(dropEvent);
        strategy.beginTransaction(2L);

        strategy.write(new SeaTunnelRow(new Object[] {3, "carol"}));
        strategy.write(new SeaTunnelRow(new Object[] {4, "dave"}));

        strategy.finishAndCloseFile();
        strategy.close();

        ParquetReadStrategy listStrategy = new ParquetReadStrategy();
        listStrategy.init(hadoopConf);
        List<String> files = listStrategy.getFileNamesByPath(tmpPath);
        listStrategy.close();
        Assertions.assertTrue(files.size() >= 2, "expected file rotation on DROP COLUMN");

        int rowsWith3Cols = 0;
        int rowsWith2Cols = 0;
        for (String file : files) {
            // Fresh reader per file because ParquetReadStrategy caches readColumns across calls.
            ParquetReadStrategy perFile = new ParquetReadStrategy();
            perFile.init(hadoopConf);
            SeaTunnelRowType fileSchema = perFile.getSeaTunnelRowTypeInfo(file);
            int cols = fileSchema.getFieldNames().length;
            int rowCount = readRows(perFile, file).size();
            perFile.close();
            if (cols == 3) {
                rowsWith3Cols += rowCount;
            } else if (cols == 2) {
                rowsWith2Cols += rowCount;
            }
        }
        Assertions.assertEquals(2, rowsWith3Cols, "pre-DROP file should hold 2 rows with 3 cols");
        Assertions.assertEquals(2, rowsWith2Cols, "post-DROP file should hold 2 rows with 2 cols");
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<SeaTunnelRow> readRows(ParquetReadStrategy readStrategy, String file)
            throws Exception {
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        rows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(file, "evolution_table", collector);
        return rows;
    }
}
