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

package org.apache.seatunnel.transform.metadata;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.filter.FilterFieldMultiCatalogTransform;
import org.apache.seatunnel.transform.filterrowkind.FieldRowKindMultiCatalogTransform;
import org.apache.seatunnel.transform.rowkind.RowKindExtractorMultiCatalogTransform;
import org.apache.seatunnel.transform.sql.SQLMultiCatalogFlatMapTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reproduces the production CCE: after ALTER ADD COLUMN, the row at the sink boundary has String at
 * the position of a TIMESTAMP column instead of LocalDateTime. Verifies the chain preserves
 * LocalDateTime through ALL transforms and emits it at the FINAL output.
 *
 * <p>This is the missing test for the v24 stage failure. If the row at the end of the chain has
 * LocalDateTime, the bug is sink-side (order divergence already fixed). If it has String, the
 * conversion happens inside the chain and we have a different fix to find.
 */
public class ChainTimestampPreservationTest {

    private static final TablePath TBL = TablePath.of("ricky_test", "static_inventory");

    /** Builds a base table mirroring the production pipeline: includes a TIMESTAMP column. */
    private static CatalogTable buildBaseTable() {
        List<Column> metadataCols = new ArrayList<>();
        metadataCols.add(
                MetadataColumn.of(
                        CommonOptions.EVENT_TIME.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadataCols.add(
                MetadataColumn.of(
                        CommonOptions.DELAY.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        return CatalogTable.of(
                TableIdentifier.of("catalog", TBL),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "price",
                                        BasicType.DOUBLE_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "created_at",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "test",
                MetadataSchema.builder().columns(metadataCols).build());
    }

    @Test
    void chainPreservesLocalDateTimeAtTimestampColumn() {
        CatalogTable baseTable = buildBaseTable();

        // Build the production wrapper chain
        Map<String, Object> filterRowKindCfg = new HashMap<>();
        filterRowKindCfg.put("include_kinds", Arrays.asList("INSERT", "UPDATE_AFTER", "DELETE"));
        FieldRowKindMultiCatalogTransform filterRowKind =
                new FieldRowKindMultiCatalogTransform(
                        Collections.singletonList(baseTable),
                        ReadonlyConfig.fromMap(filterRowKindCfg));

        CatalogTable filterRowKindOut = filterRowKind.getProducedCatalogTables().get(0);
        Map<String, String> metaMapping = new LinkedHashMap<>();
        metaMapping.put("EventTime", "c_event_time");
        metaMapping.put("Delay", "c_delay");
        Map<String, Object> metaCfg = new HashMap<>();
        metaCfg.put("metadata_fields", metaMapping);
        MetadataMultiCatalogTransform metadata =
                new MetadataMultiCatalogTransform(
                        Collections.singletonList(filterRowKindOut),
                        ReadonlyConfig.fromMap(metaCfg));

        CatalogTable metadataOut = metadata.getProducedCatalogTables().get(0);
        Map<String, Object> rkeCfg = new HashMap<>();
        rkeCfg.put("custom_field_name", "c_operation_type");
        rkeCfg.put("transform_type", "FULL");
        RowKindExtractorMultiCatalogTransform rowKindExtractor =
                new RowKindExtractorMultiCatalogTransform(
                        Collections.singletonList(metadataOut), ReadonlyConfig.fromMap(rkeCfg));

        CatalogTable rkeOut = rowKindExtractor.getProducedCatalogTables().get(0);
        Map<String, Object> sqlCfg = new HashMap<>();
        sqlCfg.put("query", "select * from " + rkeOut.getTableId().getTableName());
        SQLMultiCatalogFlatMapTransform sql =
                new SQLMultiCatalogFlatMapTransform(
                        Collections.singletonList(rkeOut), ReadonlyConfig.fromMap(sqlCfg));

        CatalogTable sqlOut = sql.getProducedCatalogTables().get(0);
        Map<String, Object> filterFieldCfg = new HashMap<>();
        filterFieldCfg.put("exclude_fields", Arrays.asList("c_delay"));
        FilterFieldMultiCatalogTransform filterField =
                new FilterFieldMultiCatalogTransform(
                        Collections.singletonList(sqlOut), ReadonlyConfig.fromMap(filterFieldCfg));

        List<org.apache.seatunnel.api.transform.SeaTunnelTransform<SeaTunnelRow>> chain =
                Arrays.asList(filterRowKind, metadata, rowKindExtractor, sql, filterField);

        // Pre-ALTER: process a row with LocalDateTime at created_at, verify it survives
        LocalDateTime sampleDateTime = LocalDateTime.of(2026, 4, 27, 5, 24, 27);
        SeaTunnelRow preRow =
                new SeaTunnelRow(new Object[] {1L, "Widget A", 19.99d, sampleDateTime});
        preRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(preRow, 1700000000000L);
        MetadataUtil.setDelay(preRow, 50L);

        SeaTunnelRow afterFilterPre = filterRowKind.map(preRow);
        SeaTunnelRow afterMetaPre = metadata.map(afterFilterPre);
        SeaTunnelRow afterRkePre = rowKindExtractor.map(afterMetaPre);
        List<SeaTunnelRow> sqlOutPre = sql.flatMap(afterRkePre);
        SeaTunnelRow afterFilterFieldPre = filterField.map(sqlOutPre.get(0));

        // Find created_at in the post-Filter row by looking it up in the produced catalog.
        // (Using catalog's column name lookup avoids hardcoding positions.)
        CatalogTable filterFieldOut = filterField.getProducedCatalogTables().get(0);
        int createdAtIdxPre =
                filterFieldOut.getTableSchema().toPhysicalRowDataType().indexOf("created_at");
        Assertions.assertTrue(
                createdAtIdxPre >= 0, "Pre-ALTER: created_at must exist in final catalog");
        Object createdAtValPre = afterFilterFieldPre.getField(createdAtIdxPre);
        Assertions.assertTrue(
                createdAtValPre instanceof LocalDateTime,
                "Pre-ALTER: created_at value at position "
                        + createdAtIdxPre
                        + " must be LocalDateTime, was "
                        + (createdAtValPre == null ? "null" : createdAtValPre.getClass().getName())
                        + " = "
                        + createdAtValPre);

        // ============ Live ALTER ADD COLUMN supplier AFTER price ============
        TableIdentifier tid = baseTable.getTableId();
        SchemaChangeEvent alter =
                new AlterTableColumnsEvent(tid)
                        .addEvent(
                                AlterTableAddColumnEvent.addAfter(
                                        tid,
                                        PhysicalColumn.of(
                                                "supplier",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null),
                                        "price"));

        // Mimic engine: setInputCatalogTables on each downstream, then mapSchemaChangeEvent
        SchemaChangeEvent ev = alter;
        for (int i = 0; i < chain.size(); i++) {
            if (i > 0) {
                chain.get(i).setInputCatalogTables(chain.get(i - 1).getProducedCatalogTables());
            }
            ev = chain.get(i).mapSchemaChangeEvent(ev);
        }

        // Post-ALTER: row now has 5 base cols (id, name, price, supplier, created_at)
        SeaTunnelRow postRow =
                new SeaTunnelRow(
                        new Object[] {2L, "Premium A", 59.99d, "OfficeWorld", sampleDateTime});
        postRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(postRow, 1700000010000L);
        MetadataUtil.setDelay(postRow, 60L);

        SeaTunnelRow afterFilterPost = filterRowKind.map(postRow);
        SeaTunnelRow afterMetaPost = metadata.map(afterFilterPost);
        SeaTunnelRow afterRkePost = rowKindExtractor.map(afterMetaPost);
        List<SeaTunnelRow> sqlOutPost = sql.flatMap(afterRkePost);
        SeaTunnelRow afterFilterFieldPost = filterField.map(sqlOutPost.get(0));

        // Find created_at again in the new produced catalog
        CatalogTable filterFieldOutPost = filterField.getProducedCatalogTables().get(0);
        int createdAtIdxPost =
                filterFieldOutPost.getTableSchema().toPhysicalRowDataType().indexOf("created_at");
        Assertions.assertTrue(
                createdAtIdxPost >= 0, "Post-ALTER: created_at must exist in final catalog");

        Object createdAtValPost = afterFilterFieldPost.getField(createdAtIdxPost);
        Assertions.assertTrue(
                createdAtValPost instanceof LocalDateTime,
                "Post-ALTER: created_at value at position "
                        + createdAtIdxPost
                        + " must be LocalDateTime, was "
                        + (createdAtValPost == null
                                ? "null"
                                : createdAtValPost.getClass().getName())
                        + " = "
                        + createdAtValPost
                        + ". This reproduces the production CCE — chain converted "
                        + "LocalDateTime to String somewhere.");

        Assertions.assertEquals(
                sampleDateTime,
                createdAtValPost,
                "Post-ALTER: created_at value should be unchanged through chain");
    }
}
