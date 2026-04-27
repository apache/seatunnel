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
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.filter.FilterFieldMultiCatalogTransform;
import org.apache.seatunnel.transform.filterrowkind.FieldRowKindMultiCatalogTransform;
import org.apache.seatunnel.transform.rowkind.RowKindExtractorMultiCatalogTransform;
import org.apache.seatunnel.transform.sql.SQLMultiCatalogFlatMapTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Full-pipeline regression test that mirrors a production CDC + transforms + sink pipeline using
 * the actual MultiCatalog wrapper classes (the same instances the engine constructs via factories
 * and feeds via {@link org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle
 * TransformFlowLifeCycle}).
 *
 * <p>Pipeline: FieldRowKind → Metadata → RowKindExtractor → SQL → FieldField. This is the exact
 * shape of the user's production pipeline that exposed Bug 1.
 *
 * <p>Pre-fix: post-ALTER inserts had their new column values silently dropped. Post-fix (override
 * of {@code mapSchemaChangeEvent} in {@link
 * org.apache.seatunnel.transform.common.AbstractMultiCatalogTransform
 * AbstractMultiCatalogTransform}): post-ALTER rows preserve all values through the chain.
 */
public class ProductionPipelineSchemaChangeTest {

    private static final TablePath TBL = TablePath.of("ricky_test", "static_inventory");

    private static CatalogTable buildBaseTable() {
        List<Column> metadata = new ArrayList<>();
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.EVENT_TIME.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadata.add(
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
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "test",
                MetadataSchema.builder().columns(metadata).build());
    }

    @Test
    void productionWrapperPipelinePreservesPostAlterValues() {
        CatalogTable baseTable = buildBaseTable();

        // 1. FieldRowKindMultiCatalogTransform — filter by INSERT/UPDATE_AFTER/DELETE
        Map<String, Object> filterRowKindCfg = new HashMap<>();
        filterRowKindCfg.put("include_kinds", Arrays.asList("INSERT", "UPDATE_AFTER", "DELETE"));
        FieldRowKindMultiCatalogTransform filterRowKind =
                new FieldRowKindMultiCatalogTransform(
                        Collections.singletonList(baseTable),
                        ReadonlyConfig.fromMap(filterRowKindCfg));

        // 2. MetadataMultiCatalogTransform — append c_event_time, c_delay
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

        // 3. RowKindExtractorMultiCatalogTransform — append c_operation_type
        CatalogTable metadataOut = metadata.getProducedCatalogTables().get(0);
        Map<String, Object> rkeCfg = new HashMap<>();
        rkeCfg.put("custom_field_name", "c_operation_type");
        rkeCfg.put("transform_type", "FULL");
        RowKindExtractorMultiCatalogTransform rowKindExtractor =
                new RowKindExtractorMultiCatalogTransform(
                        Collections.singletonList(metadataOut), ReadonlyConfig.fromMap(rkeCfg));

        // 4. SQLMultiCatalogFlatMapTransform — select *
        CatalogTable rkeOut = rowKindExtractor.getProducedCatalogTables().get(0);
        Map<String, Object> sqlCfg = new HashMap<>();
        sqlCfg.put("query", "select * from " + rkeOut.getTableId().getTableName());
        SQLMultiCatalogFlatMapTransform sql =
                new SQLMultiCatalogFlatMapTransform(
                        Collections.singletonList(rkeOut), ReadonlyConfig.fromMap(sqlCfg));

        // 5. FilterFieldMultiCatalogTransform — exclude c_delay
        CatalogTable sqlOut = sql.getProducedCatalogTables().get(0);
        Map<String, Object> filterFieldCfg = new HashMap<>();
        filterFieldCfg.put("exclude_fields", Arrays.asList("c_delay"));
        FilterFieldMultiCatalogTransform filterField =
                new FilterFieldMultiCatalogTransform(
                        Collections.singletonList(sqlOut), ReadonlyConfig.fromMap(filterFieldCfg));

        // List of all wrappers in chain order — engine iterates in this order during ALTER.
        List<org.apache.seatunnel.api.transform.SeaTunnelTransform<SeaTunnelRow>> chain =
                Arrays.asList(filterRowKind, metadata, rowKindExtractor, sql, filterField);

        // ============ Pre-ALTER ============
        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(preRow, 1700000000000L);
        MetadataUtil.setDelay(preRow, 50L);

        SeaTunnelRow afterFilter = filterRowKind.map(preRow);
        Assertions.assertNotNull(afterFilter);
        SeaTunnelRow afterMeta = metadata.map(afterFilter);
        Assertions.assertEquals(4, afterMeta.getArity(), "pre-ALTER Metadata: 2+2=4");
        SeaTunnelRow afterRke = rowKindExtractor.map(afterMeta);
        Assertions.assertEquals(5, afterRke.getArity(), "pre-ALTER RowKind: 4+1=5");
        List<SeaTunnelRow> sqlOutPre = sql.flatMap(afterRke);
        Assertions.assertEquals(1, sqlOutPre.size());
        Assertions.assertEquals(5, sqlOutPre.get(0).getArity(), "pre-ALTER SQL select *: 5");
        SeaTunnelRow afterFilterField = filterField.map(sqlOutPre.get(0));
        Assertions.assertEquals(
                4,
                afterFilterField.getArity(),
                "pre-ALTER FilterField: 5 - 1 (excluded c_delay) = 4");

        // ============ Live ALTER ADD COLUMN discount_pct, is_featured ============
        TableIdentifier tid = baseTable.getTableId();
        SchemaChangeEvent alter =
                new AlterTableColumnsEvent(tid)
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        tid,
                                        PhysicalColumn.of(
                                                "discount_pct",
                                                BasicType.DOUBLE_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null)))
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        tid,
                                        PhysicalColumn.of(
                                                "is_featured",
                                                BasicType.BOOLEAN_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null)));

        // Mimic TransformFlowLifeCycle.received's schema-change branch: each downstream wrapper
        // is refreshed from the previous wrapper's post-event produced schema before its own
        // mapSchemaChangeEvent runs.
        SchemaChangeEvent ev = alter;
        for (int i = 0; i < chain.size(); i++) {
            if (i > 0) {
                chain.get(i).setInputCatalogTables(chain.get(i - 1).getProducedCatalogTables());
            }
            ev = chain.get(i).mapSchemaChangeEvent(ev);
        }
        Assertions.assertNotNull(ev, "schema-change event should propagate downstream");

        // ============ Post-ALTER ============
        SeaTunnelRow postRow =
                new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d, Boolean.TRUE});
        postRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(postRow, 1700000010000L);
        MetadataUtil.setDelay(postRow, 60L);

        SeaTunnelRow postFilter = filterRowKind.map(postRow);
        Assertions.assertEquals(
                4,
                postFilter.getArity(),
                "FilterRowKind passes 4 base cols through unchanged post-ALTER");
        Assertions.assertEquals(10.00d, postFilter.getField(2));
        Assertions.assertEquals(Boolean.TRUE, postFilter.getField(3));

        SeaTunnelRow postMeta = metadata.map(postFilter);
        Assertions.assertEquals(
                6, postMeta.getArity(), "Metadata post-ALTER: 4 base + 2 metadata = 6");
        Assertions.assertEquals(
                10.00d, postMeta.getField(2), "discount_pct survives Metadata wrapper");
        Assertions.assertEquals(
                Boolean.TRUE, postMeta.getField(3), "is_featured survives Metadata wrapper");

        SeaTunnelRow postRke = rowKindExtractor.map(postMeta);
        Assertions.assertEquals(7, postRke.getArity(), "RowKindExtractor post-ALTER: 6 + 1 = 7");
        Assertions.assertEquals(
                10.00d, postRke.getField(2), "discount_pct survives RowKindExtractor wrapper");
        Assertions.assertEquals(
                Boolean.TRUE, postRke.getField(3), "is_featured survives RowKindExtractor wrapper");

        List<SeaTunnelRow> sqlOutPost = sql.flatMap(postRke);
        Assertions.assertEquals(1, sqlOutPost.size());
        SeaTunnelRow postSql = sqlOutPost.get(0);
        Assertions.assertEquals(
                7,
                postSql.getArity(),
                "SQL select * post-ALTER MUST be 7 cols (4 base + 2 metadata + 1 rowkind)");
        Assertions.assertEquals(10.00d, postSql.getField(2), "discount_pct survives SQL wrapper");
        Assertions.assertEquals(
                Boolean.TRUE, postSql.getField(3), "is_featured survives SQL wrapper");

        SeaTunnelRow postFinal = filterField.map(postSql);
        Assertions.assertEquals(
                6,
                postFinal.getArity(),
                "FilterField post-ALTER: 7 - 1 (c_delay excluded) = 6 — values preserved");
        Assertions.assertEquals(
                10.00d,
                postFinal.getField(2),
                "discount_pct preserved at sink boundary — proves order-divergence fix works");
        Assertions.assertEquals(
                Boolean.TRUE,
                postFinal.getField(3),
                "is_featured preserved at sink boundary — proves order-divergence fix works");
    }
}
