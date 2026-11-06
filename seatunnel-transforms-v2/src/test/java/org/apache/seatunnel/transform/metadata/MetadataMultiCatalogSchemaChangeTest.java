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
 * Regression coverage for {@link MetadataMultiCatalogTransform} under live schema-change events.
 *
 * <p>Exercises the outer multi-catalog wrapper (the instance the engine builds via the factory and
 * feeds through {@code TransformFlowLifeCycle}), verifying that {@code mapSchemaChangeEvent}
 * dispatches ALTER events to inner per-table transforms so post-ALTER rows keep the expected
 * arity/shape. Also covers connector-declared metadata fields surviving ALTER through the wrapper.
 *
 * <p>Note: dispatch of schema-change events through {@code AbstractMultiCatalogTransform} is
 * pre-existing behavior (not introduced by the connector-declared metadata change); the first case
 * below is a general regression guard, while the second case exercises the new Metadata feature.
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

        // Same call path as TransformFlowLifeCycle.received on the outer wrapper: the event must
        // be dispatched to the inner MetadataTransform so its catalog/schema stay in sync.
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
                "post-ALTER MUST be arity 6 (4 base + 2 metadata). If 4, the wrapper did not"
                        + " propagate the schema change to the inner MetadataTransform.");
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

    @Test
    void multiCatalogWrapperProjectsConnectorDeclaredMetadataAfterSchemaChange() {
        List<Column> metadata = new ArrayList<>();
        metadata.add(
                MetadataColumn.of(
                        "KafkaOffset",
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        "Kafka record offset"));
        CatalogTable baseTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TBL),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
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

        Map<String, String> metaMapping = new LinkedHashMap<>();
        metaMapping.put("KafkaOffset", "kafka_offset");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("metadata_fields", metaMapping);
        MetadataMultiCatalogTransform wrapper =
                new MetadataMultiCatalogTransform(
                        Collections.singletonList(baseTable), ReadonlyConfig.fromMap(cfg));

        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        preRow.getOptions().put("KafkaOffset", 42L);
        SeaTunnelRow preOut = wrapper.map(preRow);
        Assertions.assertEquals(3, preOut.getArity(), "pre-ALTER: 2 base + 1 custom metadata");
        Assertions.assertEquals(42L, preOut.getField(2));

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
                                                null)));
        wrapper.mapSchemaChangeEvent(alter);

        SeaTunnelRow postRow = new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d});
        postRow.setTableId(TBL.getFullName());
        postRow.getOptions().put("KafkaOffset", 99L);
        SeaTunnelRow postOut = wrapper.map(postRow);

        Assertions.assertEquals(4, postOut.getArity(), "post-ALTER: 3 base + 1 custom metadata");
        Assertions.assertEquals(2L, postOut.getField(0));
        Assertions.assertEquals("Premium A", postOut.getField(1));
        Assertions.assertEquals(10.00d, postOut.getField(2));
        Assertions.assertEquals(99L, postOut.getField(3));
    }
}
