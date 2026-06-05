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
import org.apache.seatunnel.transform.filter.FilterFieldTransform;
import org.apache.seatunnel.transform.filterrowkind.FilterRowKindTransform;
import org.apache.seatunnel.transform.rowkind.RowKindExtractorTransform;
import org.apache.seatunnel.transform.sql.SQLTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reproduces Bug 1 (live-ALTER column values dropped) by replaying the production transform chain
 * in unit form: FilterRowKind → Metadata → RowKindExtractor.
 *
 * <p>The empirical stage A/B (no-transforms config = correct values; with-transforms config = null
 * values) proves the drop happens inside the transform chain. Single-transform tests pass. This
 * test exercises the chain to show whether the chain itself reproduces the bug, and serves as the
 * green-light indicator that any fix actually closes Bug 1.
 */
public class TransformChainLiveAlterTest {

    private static final TablePath TBL = TablePath.of("ricky_test", "static_inventory");

    /** Builds a 2-col base table: id BIGINT, name STRING. */
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

    private static AlterTableColumnsEvent buildAddTwoColsEvent(CatalogTable table) {
        TableIdentifier id = table.getTableId();
        return new AlterTableColumnsEvent(id)
                .addEvent(
                        AlterTableAddColumnEvent.add(
                                id,
                                PhysicalColumn.of(
                                        "discount_pct",
                                        BasicType.DOUBLE_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null)))
                .addEvent(
                        AlterTableAddColumnEvent.add(
                                id,
                                PhysicalColumn.of(
                                        "is_featured",
                                        BasicType.BOOLEAN_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null)));
    }

    /**
     * Full production chain: FilterRowKind → Metadata → RowKindExtractor.
     *
     * <p>Pre-ALTER: 2 base cols → FilterRowKind passes through (arity 2) → Metadata adds 2 metadata
     * cols (arity 4) → RowKindExtractor adds 1 (arity 5).
     *
     * <p>Post-ALTER: 4 base cols → FilterRowKind passes through (arity 4) → Metadata adds 2 (arity
     * 6) → RowKindExtractor adds 1 (arity 7).
     *
     * <p>Bug 1: production shows post-ALTER input arity to SQL = pre-ALTER value, meaning Metadata
     * silently truncated. Reproducing here proves the chain has the bug; assertions failing means
     * we've nailed the layer.
     */
    @Test
    void chainPreservesPostAlterColumnValues() {
        CatalogTable baseTable = buildBaseTable();

        // Build chain mirroring user's prod pipeline order.
        Map<String, Object> filterCfg = new HashMap<>();
        filterCfg.put("include_kinds", Arrays.asList("INSERT", "UPDATE_AFTER", "DELETE"));
        FilterRowKindTransform filterRowKind =
                new FilterRowKindTransform(ReadonlyConfig.fromMap(filterCfg), baseTable);

        // FilterRowKind doesn't change schema; Metadata's input is FilterRowKind's output schema =
        // baseTable.
        Map<String, String> metaMapping = new LinkedHashMap<>();
        metaMapping.put("EventTime", "c_event_time");
        metaMapping.put("Delay", "c_delay");
        Map<String, Object> metaCfg = new HashMap<>();
        metaCfg.put("metadata_fields", metaMapping);
        MetadataTransform metadata =
                new MetadataTransform(ReadonlyConfig.fromMap(metaCfg), baseTable);
        metadata.initRowContainerGenerator();

        // RowKindExtractor's input is Metadata's output schema. Construct from Metadata's produced
        // table so column-name conflict checks see the metadata cols already present.
        CatalogTable metadataOutput = metadata.getProducedCatalogTable();
        Map<String, Object> rkeCfg = new HashMap<>();
        rkeCfg.put("custom_field_name", "c_operation_type");
        rkeCfg.put("transform_type", "FULL");
        RowKindExtractorTransform rowKindExtractor =
                new RowKindExtractorTransform(ReadonlyConfig.fromMap(rkeCfg), metadataOutput);
        rowKindExtractor.initRowContainerGenerator();

        // ============ Pre-ALTER ============
        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(preRow, 1700000000000L);
        MetadataUtil.setDelay(preRow, 50L);

        SeaTunnelRow afterFilter = filterRowKind.map(preRow);
        Assertions.assertNotNull(afterFilter, "FilterRowKind should pass INSERT through");
        Assertions.assertEquals(2, afterFilter.getArity(), "FilterRowKind doesn't change arity");

        SeaTunnelRow afterMeta = metadata.map(afterFilter);
        Assertions.assertEquals(
                4, afterMeta.getArity(), "Metadata: 2 base + 2 metadata cols pre-ALTER");
        Assertions.assertEquals(1L, afterMeta.getField(0));
        Assertions.assertEquals("Widget A", afterMeta.getField(1));

        SeaTunnelRow afterRke = rowKindExtractor.map(afterMeta);
        Assertions.assertEquals(
                5, afterRke.getArity(), "RowKindExtractor: 4 + 1 rowkind col pre-ALTER");

        // ============ Live ALTER ADD COLUMN discount_pct, is_featured ============
        SchemaChangeEvent alter = buildAddTwoColsEvent(baseTable);
        // TransformFlowLifeCycle iterates in chain order, calling each transform's
        // mapSchemaChangeEvent with the previous return value. Pass the same event through.
        SchemaChangeEvent eventThroughChain = filterRowKind.mapSchemaChangeEvent(alter);
        eventThroughChain = metadata.mapSchemaChangeEvent(eventThroughChain);
        eventThroughChain = rowKindExtractor.mapSchemaChangeEvent(eventThroughChain);
        Assertions.assertNotNull(eventThroughChain, "schema-change event should propagate");

        // ============ Post-ALTER row (4 base cols: id, name, discount_pct, is_featured)
        // ============
        SeaTunnelRow postRow =
                new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d, Boolean.TRUE});
        postRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(postRow, 1700000010000L);
        MetadataUtil.setDelay(postRow, 60L);

        SeaTunnelRow postFilter = filterRowKind.map(postRow);
        Assertions.assertEquals(
                4,
                postFilter.getArity(),
                "FilterRowKind must pass through full post-ALTER arity (4 base cols)");
        Assertions.assertEquals(10.00d, postFilter.getField(2));
        Assertions.assertEquals(Boolean.TRUE, postFilter.getField(3));

        SeaTunnelRow postMeta = metadata.map(postFilter);
        // The smoking-gun assertion: must be 4 base + 2 metadata = 6, NOT 2 base + 2 metadata = 4.
        Assertions.assertEquals(
                6,
                postMeta.getArity(),
                "Metadata MUST produce arity 6 post-ALTER (4 base + 2 metadata). "
                        + "If this fails with arity 4, that's Bug 1 — silent column drop in "
                        + "MultipleFieldOutputTransform's cached System.arraycopy.");
        Assertions.assertEquals(2L, postMeta.getField(0));
        Assertions.assertEquals("Premium A", postMeta.getField(1));
        Assertions.assertEquals(
                10.00d,
                postMeta.getField(2),
                "discount_pct must survive Metadata transform after live ALTER");
        Assertions.assertEquals(
                Boolean.TRUE,
                postMeta.getField(3),
                "is_featured must survive Metadata transform after live ALTER");

        SeaTunnelRow postRke = rowKindExtractor.map(postMeta);
        Assertions.assertEquals(
                7,
                postRke.getArity(),
                "RowKindExtractor MUST produce arity 7 post-ALTER (4 base + 2 metadata + 1 "
                        + "rowkind). If arity is less, RowKindExtractor's cache is also stale.");
        Assertions.assertEquals(10.00d, postRke.getField(2));
        Assertions.assertEquals(Boolean.TRUE, postRke.getField(3));
    }

    /**
     * Full production chain INCLUDING SQL transform: FilterRowKind → Metadata → RowKindExtractor →
     * SQL → FilterFieldTransform.
     *
     * <p>SQLTransform.mapSchemaChangeEvent does NOT call transformTableSchema() — it only nullifies
     * sqlEngine and outputCatalogTable. So outRowType (set inside transformTableSchema) stays at
     * the pre-ALTER value forever post-ALTER. transformBySQL(inputRow, outRowType) constructs
     * output rows shaped to outRowType. Stale outRowType ⇒ stale output shape ⇒ Bug 1.
     */
    @Test
    void chainWithSqlAndFilterPreservesPostAlterColumnValues() {
        CatalogTable baseTable = buildBaseTable();

        Map<String, Object> filterCfg = new HashMap<>();
        filterCfg.put("include_kinds", Arrays.asList("INSERT", "UPDATE_AFTER", "DELETE"));
        FilterRowKindTransform filterRowKind =
                new FilterRowKindTransform(ReadonlyConfig.fromMap(filterCfg), baseTable);

        Map<String, String> metaMapping = new LinkedHashMap<>();
        metaMapping.put("EventTime", "c_event_time");
        metaMapping.put("Delay", "c_delay");
        Map<String, Object> metaCfg = new HashMap<>();
        metaCfg.put("metadata_fields", metaMapping);
        MetadataTransform metadata =
                new MetadataTransform(ReadonlyConfig.fromMap(metaCfg), baseTable);
        metadata.initRowContainerGenerator();

        CatalogTable metadataOutput = metadata.getProducedCatalogTable();
        Map<String, Object> rkeCfg = new HashMap<>();
        rkeCfg.put("custom_field_name", "c_operation_type");
        rkeCfg.put("transform_type", "FULL");
        RowKindExtractorTransform rowKindExtractor =
                new RowKindExtractorTransform(ReadonlyConfig.fromMap(rkeCfg), metadataOutput);
        rowKindExtractor.initRowContainerGenerator();

        // SQL transform with user's actual production query shape: `select *, computed_a,
        // computed_b from rkeOutput`. This adds projected columns whose positions are CACHED in
        // SQLTransform.outRowType. SQLTransform.mapSchemaChangeEvent does NOT call
        // transformTableSchema, so outRowType stays at pre-ALTER positions ⇒ Bug 1.
        CatalogTable rkeOutput = rowKindExtractor.getProducedCatalogTable();
        Map<String, Object> sqlCfg = new HashMap<>();
        sqlCfg.put(
                "query",
                "select *, c_event_time AS c_event_time_copy, c_operation_type AS c_op_copy from "
                        + rkeOutput.getTableId().getTableName());
        SQLTransform sqlTransform = new SQLTransform(ReadonlyConfig.fromMap(sqlCfg), rkeOutput);

        // FilterFieldTransform: exclude one of the metadata cols (mimic user's exclude_fields)
        CatalogTable sqlOutput = sqlTransform.getProducedCatalogTable();
        Map<String, Object> filterFieldCfg = new HashMap<>();
        filterFieldCfg.put("exclude_fields", Arrays.asList("c_delay"));
        FilterFieldTransform filterField =
                new FilterFieldTransform(ReadonlyConfig.fromMap(filterFieldCfg), sqlOutput);
        // Trigger lazy init of inputValueIndexList by computing the produced catalog table.
        filterField.getProducedCatalogTable();

        // ============ Pre-ALTER ============
        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(preRow, 1700000000000L);
        MetadataUtil.setDelay(preRow, 50L);

        SeaTunnelRow afterFilter = filterRowKind.map(preRow);
        SeaTunnelRow afterMeta = metadata.map(afterFilter);
        SeaTunnelRow afterRke = rowKindExtractor.map(afterMeta);
        Assertions.assertEquals(5, afterRke.getArity(), "pre-ALTER through RowKindExtractor: 5");

        List<SeaTunnelRow> sqlOut = sqlTransform.flatMap(afterRke);
        Assertions.assertEquals(1, sqlOut.size());
        SeaTunnelRow afterSql = sqlOut.get(0);
        // 5 input + 2 computed = 7
        Assertions.assertEquals(
                7,
                afterSql.getArity(),
                "pre-ALTER SQL select *, computed_a, computed_b: 5 + 2 = 7");

        SeaTunnelRow afterFilterField = filterField.map(afterSql);
        Assertions.assertEquals(
                6,
                afterFilterField.getArity(),
                "pre-ALTER FilterField excludes c_delay: 7 - 1 = 6");

        // ============ Live ALTER ADD COLUMN discount_pct, is_featured ============
        SchemaChangeEvent alter = buildAddTwoColsEvent(baseTable);
        SchemaChangeEvent ev = filterRowKind.mapSchemaChangeEvent(alter);
        ev = metadata.mapSchemaChangeEvent(ev);
        ev = rowKindExtractor.mapSchemaChangeEvent(ev);
        ev = sqlTransform.mapSchemaChangeEvent(ev);
        ev = filterField.mapSchemaChangeEvent(ev);
        Assertions.assertNotNull(ev);

        // ============ Post-ALTER ============
        SeaTunnelRow postRow =
                new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d, Boolean.TRUE});
        postRow.setTableId(TBL.getFullName());
        MetadataUtil.setEventTime(postRow, 1700000010000L);
        MetadataUtil.setDelay(postRow, 60L);

        SeaTunnelRow postFilter = filterRowKind.map(postRow);
        Assertions.assertEquals(4, postFilter.getArity());

        SeaTunnelRow postMeta = metadata.map(postFilter);
        Assertions.assertEquals(
                6, postMeta.getArity(), "Metadata post-ALTER: 4 base + 2 metadata = 6");
        Assertions.assertEquals(10.00d, postMeta.getField(2));
        Assertions.assertEquals(Boolean.TRUE, postMeta.getField(3));

        SeaTunnelRow postRke = rowKindExtractor.map(postMeta);
        Assertions.assertEquals(7, postRke.getArity(), "RowKindExtractor post-ALTER: 6 + 1 = 7");
        Assertions.assertEquals(10.00d, postRke.getField(2));
        Assertions.assertEquals(Boolean.TRUE, postRke.getField(3));

        // ============ THE SMOKING GUN: SQL transform's transformBySQL ============
        // Pre-ALTER: 5 input + 2 computed = 7. Post-ALTER expected: 7 input + 2 computed = 9.
        List<SeaTunnelRow> postSqlOut = sqlTransform.flatMap(postRke);
        Assertions.assertEquals(1, postSqlOut.size());
        SeaTunnelRow postSql = postSqlOut.get(0);
        Assertions.assertEquals(
                9,
                postSql.getArity(),
                "SQL post-ALTER MUST produce arity 9 (7 input + 2 computed). If 7, "
                        + "SQLTransform.outRowType is stale at pre-ALTER size and the engine "
                        + "projects with old positions ⇒ Bug 1 reproduced.");
        Assertions.assertEquals(
                10.00d, postSql.getField(2), "discount_pct must survive SQL after live ALTER");
        Assertions.assertEquals(
                Boolean.TRUE, postSql.getField(3), "is_featured must survive SQL after live ALTER");
    }
}
