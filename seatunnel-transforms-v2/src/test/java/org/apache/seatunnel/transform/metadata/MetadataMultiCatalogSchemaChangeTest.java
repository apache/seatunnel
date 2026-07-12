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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reproduces upstream Bug 1: schema-change events are silently dropped by {@code
 * AbstractMultiCatalogTransform} subclasses, leaving the inner per-table transforms with stale
 * {@code inputCatalogTable} after ALTER. Result: post-ALTER rows lose new column values.
 *
 * <p>This test calls the OUTER wrapper ({@code MetadataMultiCatalogTransform}) — the same instance
 * the SeaTunnel engine constructs via the factory and feeds via {@code TransformFlowLifeCycle}.
 * Earlier {@code TransformChainLiveAlterTest} called the inner transform directly, missing the bug.
 *
 * <p>Without the fix in {@code AbstractMultiCatalogTransform.mapSchemaChangeEvent}, the wrapper's
 * default no-op returns the event without dispatching to inner transforms, so this test FAILS at
 * the post-ALTER arity assertion.
 */
public class MetadataMultiCatalogSchemaChangeTest {

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
    void multiCatalogWrapperPropagatesSchemaChangeToInnerTransforms() {
        CatalogTable baseTable = buildBaseTable();

        Map<String, String> metaMapping = new LinkedHashMap<>();
        metaMapping.put("EventTime", "c_event_time");
        metaMapping.put("Delay", "c_delay");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("metadata_fields", metaMapping);
        // table_match_regex defaults to ".*" which matches all tables; this lets the wrapper apply
        // its config to the only inner table without per-table overrides.
        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);

        MetadataMultiCatalogTransform wrapper =
                new MetadataMultiCatalogTransform(Collections.singletonList(baseTable), config);

        // Pre-ALTER row: 2 base cols
        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(preRow, 1700000000000L);
        MetadataUtil.setDelay(preRow, 50L);

        SeaTunnelRow preOut = wrapper.map(preRow);
        Assertions.assertEquals(4, preOut.getArity(), "pre-ALTER: 2 base + 2 metadata = 4");

        // Live ALTER ADD COLUMN discount_pct, is_featured
        TableIdentifier tid = baseTable.getTableId();
        AlterTableColumnsEvent alter =
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

        // This is the exact call TransformFlowLifeCycle.received makes on the outer wrapper.
        // Without the fix: wrapper's default no-op returns event unchanged; inner transformMap
        // entries never see ALTER; their inputCatalogTable stays at 2 cols.
        wrapper.mapSchemaChangeEvent(alter);

        // Post-ALTER row: 4 base cols (id, name, discount_pct, is_featured)
        SeaTunnelRow postRow =
                new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d, Boolean.TRUE});
        postRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(postRow, 1700000010000L);
        MetadataUtil.setDelay(postRow, 60L);

        SeaTunnelRow postOut = wrapper.map(postRow);

        Assertions.assertEquals(
                6,
                postOut.getArity(),
                "post-ALTER MUST be arity 6 (4 base + 2 metadata). If 4, the wrapper "
                        + "swallowed the schema change without notifying inner MetadataTransform — "
                        + "exactly Bug 1.");
        Assertions.assertEquals(2L, postOut.getField(0));
        Assertions.assertEquals("Premium A", postOut.getField(1));
        Assertions.assertEquals(
                10.00d,
                postOut.getField(2),
                "discount_pct must survive the wrapper after live ALTER");
        Assertions.assertEquals(
                Boolean.TRUE,
                postOut.getField(3),
                "is_featured must survive the wrapper after live ALTER");
    }
}
