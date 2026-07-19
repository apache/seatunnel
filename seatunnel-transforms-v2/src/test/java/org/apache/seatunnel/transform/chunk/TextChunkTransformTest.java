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

package org.apache.seatunnel.transform.chunk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TextChunkTransformTest {

    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "docs"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0L, true, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "content",
                                                BasicType.STRING_TYPE,
                                                1000L,
                                                true,
                                                "",
                                                ""))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
    }

    private TextChunkTransform newTransform() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        configMap.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 4);
        configMap.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 0);
        // empty separators -> deterministic fixed-size chunking
        configMap.put(TextChunkTransformConfig.SEPARATORS.key(), Collections.emptyList());
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return new TextChunkTransform(TextChunkTransformConfig.of(config), catalogTable);
    }

    @Test
    void producedSchemaAppendsChunkColumns() {
        CatalogTable output = newTransform().getProducedCatalogTable();
        TableSchema schema = output.getTableSchema();

        // original two columns + chunk + chunk_index
        Assertions.assertEquals(4, schema.getColumns().size());
        Assertions.assertEquals(BasicType.STRING_TYPE, schema.getColumn("chunk").getDataType());
        Assertions.assertEquals(BasicType.INT_TYPE, schema.getColumn("chunk_index").getDataType());
        // original columns preserved
        Assertions.assertNotNull(schema.getColumn("id"));
        Assertions.assertNotNull(schema.getColumn("content"));
    }

    @Test
    void oneRowExpandsToNRowsPreservingSourceFields() {
        TextChunkTransform transform = newTransform();
        SeaTunnelRowType outputType =
                transform.getProducedCatalogTable().getTableSchema().toPhysicalRowDataType();
        int chunkIdx = outputType.indexOf("chunk");
        int chunkSeqIdx = outputType.indexOf("chunk_index");

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {7, "abcdefghij"});
        input.setTableId("docs");
        input.setRowKind(RowKind.UPDATE_AFTER);
        // per-row metadata (e.g. stain-trace payload) that must survive onto every chunk row
        input.getOptions().put("trace", "payload");

        List<SeaTunnelRow> out = transform.flatMap(input);

        // "abcdefghij" (10 chars) with chunk_size=4 -> ["abcd","efgh","ij"]
        Assertions.assertEquals(3, out.size());
        String[] expectedChunks = {"abcd", "efgh", "ij"};
        for (int i = 0; i < out.size(); i++) {
            SeaTunnelRow row = out.get(i);
            // source fields preserved on every chunk row
            Assertions.assertEquals(7, row.getField(0));
            Assertions.assertEquals("abcdefghij", row.getField(1));
            // appended chunk + 0-based index
            Assertions.assertEquals(expectedChunks[i], row.getField(chunkIdx));
            Assertions.assertEquals(i, row.getField(chunkSeqIdx));
            // tableId, rowKind and options (e.g. stain-trace payload) carried through
            Assertions.assertEquals("docs", row.getTableId());
            Assertions.assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
            Assertions.assertEquals("payload", row.getOptions().get("trace"));
        }
    }

    @Test
    void nullOrEmptyTextProducesNoRows() {
        TextChunkTransform transform = newTransform();

        SeaTunnelRow nullText = new SeaTunnelRow(new Object[] {1, null});
        SeaTunnelRow emptyText = new SeaTunnelRow(new Object[] {2, ""});

        Assertions.assertTrue(transform.flatMap(nullText).isEmpty());
        Assertions.assertTrue(transform.flatMap(emptyText).isEmpty());
    }

    @Test
    void skipEmptyTextFalsePassesRowThroughInsteadOfDropping() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        configMap.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 4);
        configMap.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 0);
        configMap.put(TextChunkTransformConfig.SEPARATORS.key(), Collections.emptyList());
        configMap.put(TextChunkTransformConfig.SKIP_EMPTY_TEXT.key(), false);
        TextChunkTransform transform =
                new TextChunkTransform(
                        TextChunkTransformConfig.of(ReadonlyConfig.fromMap(configMap)),
                        catalogTable);

        SeaTunnelRowType outputType =
                transform.getProducedCatalogTable().getTableSchema().toPhysicalRowDataType();
        int chunkIdx = outputType.indexOf("chunk");
        int chunkSeqIdx = outputType.indexOf("chunk_index");

        for (Object emptyValue : new Object[] {null, ""}) {
            SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, emptyValue});
            input.setTableId("docs");
            input.setRowKind(RowKind.INSERT);

            List<SeaTunnelRow> out = transform.flatMap(input);

            // one passthrough row: source fields kept, chunk = null, chunk_index = 0
            Assertions.assertEquals(1, out.size());
            SeaTunnelRow row = out.get(0);
            Assertions.assertEquals(1, row.getField(0));
            Assertions.assertNull(row.getField(chunkIdx));
            Assertions.assertEquals(0, row.getField(chunkSeqIdx));
            Assertions.assertEquals("docs", row.getTableId());
        }
    }

    @Test
    void producedSchemaExtendsPrimaryKeyAndUniqueKeyWithChunkIndex() {
        CatalogTable withKeys =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "docs"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0L, false, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "content",
                                                BasicType.STRING_TYPE,
                                                1000L,
                                                true,
                                                "",
                                                ""))
                                .primaryKey(PrimaryKey.of("pk_id", Collections.singletonList("id")))
                                .constraintKey(
                                        Arrays.asList(
                                                ConstraintKey.of(
                                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                        "uk_content",
                                                        Collections.singletonList(
                                                                ConstraintKey.ConstraintKeyColumn
                                                                        .of(
                                                                                "content",
                                                                                ConstraintKey
                                                                                        .ColumnSortType
                                                                                        .ASC))),
                                                ConstraintKey.of(
                                                        ConstraintKey.ConstraintType.INDEX_KEY,
                                                        "idx_content",
                                                        Collections.singletonList(
                                                                ConstraintKey.ConstraintKeyColumn
                                                                        .of(
                                                                                "content",
                                                                                ConstraintKey
                                                                                        .ColumnSortType
                                                                                        .ASC)))))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        TextChunkTransform transform =
                new TextChunkTransform(
                        TextChunkTransformConfig.of(ReadonlyConfig.fromMap(configMap)), withKeys);

        TableSchema schema = transform.getProducedCatalogTable().getTableSchema();

        // the chunk index column joins the keys below, so it must be non-nullable
        Assertions.assertFalse(schema.getColumn("chunk_index").isNullable());

        // the primary key is extended
        Assertions.assertNotNull(schema.getPrimaryKey());
        Assertions.assertEquals(
                Arrays.asList("id", "chunk_index"), schema.getPrimaryKey().getColumnNames());

        Map<String, ConstraintKey> keysByName =
                schema.getConstraintKeys().stream()
                        .collect(Collectors.toMap(ConstraintKey::getConstraintName, k -> k));
        Assertions.assertEquals(2, keysByName.size());

        // the unique key is extended
        List<String> ukColumns =
                keysByName.get("uk_content").getColumnNames().stream()
                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                        .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("content", "chunk_index"), ukColumns);

        // the non-unique index is copied unchanged
        List<String> idxColumns =
                keysByName.get("idx_content").getColumnNames().stream()
                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                        .collect(Collectors.toList());
        Assertions.assertEquals(Collections.singletonList("content"), idxColumns);
    }

    @Test
    void autoIdPrimaryKeyIsNotExtended() {
        // A sink-auto-generated primary key (enableAutoId) already yields a fresh unique value
        // per output row, so it must be kept as-is rather than extended with chunk_index.
        CatalogTable withAutoId =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "docs"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.LONG_TYPE, 0L, false, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "content",
                                                BasicType.STRING_TYPE,
                                                1000L,
                                                true,
                                                "",
                                                ""))
                                .primaryKey(
                                        PrimaryKey.of(
                                                "pk_id", Collections.singletonList("id"), true))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        TextChunkTransform transform =
                new TextChunkTransform(
                        TextChunkTransformConfig.of(ReadonlyConfig.fromMap(configMap)), withAutoId);

        TableSchema schema = transform.getProducedCatalogTable().getTableSchema();
        Assertions.assertNotNull(schema.getPrimaryKey());
        Assertions.assertEquals(
                Collections.singletonList("id"), schema.getPrimaryKey().getColumnNames());
        Assertions.assertEquals(Boolean.TRUE, schema.getPrimaryKey().getEnableAutoId());
    }

    @Test
    void chunkIndexFieldCollidingWithPrimaryKeyIsRejected() {
        CatalogTable withPk =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "docs"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0L, false, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "content",
                                                BasicType.STRING_TYPE,
                                                1000L,
                                                true,
                                                "",
                                                ""))
                                .primaryKey(PrimaryKey.of("pk_id", Collections.singletonList("id")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        configMap.put(TextChunkTransformConfig.CHUNK_INDEX_FIELD.key(), "id");

        SeaTunnelRuntimeException ex =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                new TextChunkTransform(
                                        TextChunkTransformConfig.of(
                                                ReadonlyConfig.fromMap(configMap)),
                                        withPk));
        Assertions.assertTrue(ex.getMessage().contains("id"));
    }

    @Test
    void outputFieldCollidingWithUniqueKeyIsRejected() {
        // output_field = "doc_hash" would overwrite a unique-key column with per-chunk text.
        CatalogTable withUniqueKey =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "docs"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0L, false, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "doc_hash",
                                                BasicType.STRING_TYPE,
                                                64L,
                                                true,
                                                "",
                                                ""))
                                .column(
                                        PhysicalColumn.of(
                                                "content",
                                                BasicType.STRING_TYPE,
                                                1000L,
                                                true,
                                                "",
                                                ""))
                                .constraintKey(
                                        Collections.singletonList(
                                                ConstraintKey.of(
                                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                        "uk_doc_hash",
                                                        Collections.singletonList(
                                                                ConstraintKey.ConstraintKeyColumn
                                                                        .of(
                                                                                "doc_hash",
                                                                                ConstraintKey
                                                                                        .ColumnSortType
                                                                                        .ASC)))))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        configMap.put(TextChunkTransformConfig.OUTPUT_FIELD.key(), "doc_hash");

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        new TextChunkTransform(
                                TextChunkTransformConfig.of(ReadonlyConfig.fromMap(configMap)),
                                withUniqueKey));
    }

    @Test
    void reusingNonKeyColumnAsOutputFieldIsAllowed() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        configMap.put(TextChunkTransformConfig.OUTPUT_FIELD.key(), "content");
        TextChunkTransform transform =
                new TextChunkTransform(
                        TextChunkTransformConfig.of(ReadonlyConfig.fromMap(configMap)),
                        catalogTable);

        TableSchema schema = transform.getProducedCatalogTable().getTableSchema();
        // "content" is reused (not duplicated): original 2 columns + chunk_index only
        Assertions.assertEquals(3, schema.getColumns().size());
        Assertions.assertNotNull(schema.getColumn("content"));
        Assertions.assertNotNull(schema.getColumn("chunk_index"));
    }

    @Test
    void multiCatalogPerTableDispatchChunksRows() {
        // exercises TextChunkMultiCatalogTransform.buildTransform -> per-table dispatch actually
        // chunking rows for a table matched by a table_transform entry
        String tablePath = catalogTable.getTableId().toTablePath().toString();

        Map<String, Object> tableTransform = new HashMap<>();
        tableTransform.put(TransformCommonOptions.TABLE_PATH.key(), tablePath);
        tableTransform.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        tableTransform.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 4);
        tableTransform.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 0);
        tableTransform.put(TextChunkTransformConfig.SEPARATORS.key(), Collections.emptyList());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                TransformCommonOptions.MULTI_TABLES.key(),
                Collections.singletonList(tableTransform));

        TextChunkMultiCatalogTransform multi =
                new TextChunkMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(configMap));

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {7, "abcdefghij"});
        input.setTableId(tablePath);

        List<SeaTunnelRow> out = multi.flatMap(input);

        // "abcdefghij" (10 chars) with chunk_size=4 -> ["abcd","efgh","ij"] = 3 rows
        Assertions.assertEquals(3, out.size());
    }
}
