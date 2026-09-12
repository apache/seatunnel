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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformCommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.rename.ConvertCase;
import org.apache.seatunnel.transform.rename.FieldRenameConfig;
import org.apache.seatunnel.transform.rename.FieldRenameTransform;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLEngine;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers the SQL transform's schema change translation end to end: output-relative events for star
 * projections, projections, aliases and derived columns, absorbed changes, fail-fast cases, lineage
 * through composites, staged engine hand-offs at chain positions greater than zero, the multi-table
 * wrapper with several rules per table, resynchronisation events and the engine and UDF lifecycle
 * across changes.
 */
public class SQLTransformSchemaChangeTest {

    private static final TablePath TBL = TablePath.of("db", "products");
    private static final TableIdentifier TID = TableIdentifier.of("catalog", TBL);

    @Test
    public void testStarAddColumnKeepsUpstreamShape() {
        SQLTransform transform = transform("select * from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();
        AlterTableColumnsEvent event = composite(addAge());

        SchemaChangeEvent out = transform.mapSchemaChangeEvent(event);

        Assertions.assertTrue(out instanceof AlterTableColumnsEvent);
        List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) out).getEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableAddColumnEvent add = (AlterTableAddColumnEvent) events.get(0);
        Assertions.assertEquals("age", add.getColumn().getName());
        Assertions.assertFalse(add.isFirst());
        Assertions.assertNull(add.getAfterColumn());
        Assertions.assertEquals("MySQL", add.getSourceDialectName());
        Assertions.assertEquals("job-1", add.getJobId());
        Assertions.assertEquals(event.getStatement(), add.getStatement());
        Assertions.assertSame(transform.getProducedCatalogTable(), add.getChangeAfter());
        Assertions.assertSame(transform.getProducedCatalogTable(), out.getChangeAfter());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());

        List<SeaTunnelRow> rows =
                transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}));
        Assertions.assertEquals(4, rows.get(0).getArity());
        Assertions.assertEquals(20, rows.get(0).getField(3));
    }

    @Test
    public void testStarAddAfterAndFirstKeepPositions() {
        SQLTransform after = transform("select * from products", baseTable());
        AlterTableAddColumnEvent addAfter =
                (AlterTableAddColumnEvent)
                        after.mapSchemaChangeEvent(
                                AlterTableAddColumnEvent.addAfter(
                                        TID, column("age", BasicType.INT_TYPE, "int"), "id"));
        Assertions.assertEquals("id", addAfter.getAfterColumn());
        Assertions.assertArrayEquals(
                new String[] {"id", "age", "name", "weight"},
                after.getProducedCatalogTable().getTableSchema().getFieldNames());

        SQLTransform first = transform("select * from products", baseTable());
        AlterTableAddColumnEvent addFirst =
                (AlterTableAddColumnEvent)
                        first.mapSchemaChangeEvent(
                                AlterTableAddColumnEvent.addFirst(
                                        TID, column("age", BasicType.INT_TYPE, "int")));
        Assertions.assertTrue(addFirst.isFirst());
    }

    @Test
    public void testStarWithTrailingExpressionAddsAfterLastStarColumn() {
        SQLTransform transform =
                transform("select *, weight * 2 as double_weight from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        SchemaChangeEvent out = transform.mapSchemaChangeEvent(composite(addAge()));

        AlterTableAddColumnEvent add =
                (AlterTableAddColumnEvent) ((AlterTableColumnsEvent) out).getEvents().get(0);
        Assertions.assertEquals("weight", add.getAfterColumn());
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "weight", "age", "double_weight"},
                transform.getProducedCatalogTable().getTableSchema().getFieldNames());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());

        List<SeaTunnelRow> rows =
                transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.5d, 20}));
        Assertions.assertEquals(5, rows.get(0).getArity());
        Assertions.assertEquals(20, rows.get(0).getField(3));
        Assertions.assertEquals(3.0d, ((Number) rows.get(0).getField(4)).doubleValue(), 0.0001d);
    }

    @Test
    public void testProjectionAbsorbsUnrelatedAddAndDrop() {
        SQLTransform transform = transform("select id, name from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        Assertions.assertNull(transform.mapSchemaChangeEvent(composite(addAge())));
        Assertions.assertEquals(before, transform.getProducedCatalogTable().getTableSchema());
        List<SeaTunnelRow> rows =
                transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}));
        Assertions.assertEquals(2, rows.get(0).getArity());
        Assertions.assertEquals("a", rows.get(0).getField(1));

        Assertions.assertNull(
                transform.mapSchemaChangeEvent(
                        composite(new AlterTableDropColumnEvent(TID, "age"))));
        rows = transform.flatMap(new SeaTunnelRow(new Object[] {2L, "b", 1.0d}));
        Assertions.assertEquals(2, rows.get(0).getArity());
    }

    @Test
    public void testDropOrRenameOfReferencedColumnFailsFastAndLeavesStateUntouched() {
        SQLTransform transform = transform("select id, name from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        TransformException drop =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                transform.mapSchemaChangeEvent(
                                        composite(new AlterTableDropColumnEvent(TID, "name"))));
        Assertions.assertEquals(
                TransformCommonErrorCode.SQL_SCHEMA_CHANGE_INCOMPATIBLE,
                drop.getSeaTunnelErrorCode());
        Assertions.assertTrue(drop.getMessage().contains("[name]"), drop.getMessage());

        TransformException rename =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                transform.mapSchemaChangeEvent(
                                        composite(
                                                AlterTableChangeColumnEvent.change(
                                                        TID,
                                                        "name",
                                                        column(
                                                                "full_name",
                                                                BasicType.STRING_TYPE,
                                                                "varchar(255)")))));
        Assertions.assertEquals(
                TransformCommonErrorCode.SQL_SCHEMA_CHANGE_INCOMPATIBLE,
                rename.getSeaTunnelErrorCode());

        Assertions.assertEquals(before, transform.getProducedCatalogTable().getTableSchema());
        List<SeaTunnelRow> rows = transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d}));
        Assertions.assertEquals(2, rows.get(0).getArity());
    }

    @Test
    public void testStarRenameEmitsChangeWithFinalColumn() {
        SQLTransform transform = transform("select * from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();
        PhysicalColumn fullName = column("full_name", BasicType.STRING_TYPE, "varchar(255)");

        SchemaChangeEvent out =
                transform.mapSchemaChangeEvent(
                        composite(AlterTableChangeColumnEvent.change(TID, "name", fullName)));

        AlterTableChangeColumnEvent change =
                (AlterTableChangeColumnEvent) ((AlterTableColumnsEvent) out).getEvents().get(0);
        Assertions.assertEquals("name", change.getOldColumn());
        Assertions.assertEquals("full_name", change.getColumn().getName());
        Assertions.assertEquals("varchar(255)", change.getColumn().getSourceType());
        Assertions.assertEquals("MySQL", change.getSourceDialectName());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testCompositeRenameReuseAndDropReaddKeepLineage() {
        SQLTransform star = transform("select * from products", baseTable());
        TableSchema starBefore = star.getProducedCatalogTable().getTableSchema();
        SchemaChangeEvent starOut =
                star.mapSchemaChangeEvent(
                        composite(
                                AlterTableChangeColumnEvent.change(
                                        TID,
                                        "name",
                                        column("name_old", BasicType.STRING_TYPE, "varchar(255)")),
                                AlterTableAddColumnEvent.add(
                                        TID,
                                        column("name", BasicType.STRING_TYPE, "varchar(64)"))));
        List<AlterTableColumnEvent> starEvents = ((AlterTableColumnsEvent) starOut).getEvents();
        Assertions.assertEquals(2, starEvents.size());
        Assertions.assertTrue(starEvents.get(0) instanceof AlterTableChangeColumnEvent);
        Assertions.assertTrue(starEvents.get(1) instanceof AlterTableAddColumnEvent);
        assertReplays(starBefore, starOut, star.getProducedCatalogTable().getTableSchema());

        SQLTransform reference = transform("select id, name from products", baseTable());
        TableSchema referenceBefore = reference.getProducedCatalogTable().getTableSchema();
        SchemaChangeEvent referenceOut =
                reference.mapSchemaChangeEvent(
                        composite(
                                AlterTableChangeColumnEvent.change(
                                        TID,
                                        "name",
                                        column("name_old", BasicType.STRING_TYPE, "varchar(255)")),
                                AlterTableAddColumnEvent.add(
                                        TID,
                                        column("name", BasicType.STRING_TYPE, "varchar(64)"))));
        List<AlterTableColumnEvent> referenceEvents =
                ((AlterTableColumnsEvent) referenceOut).getEvents();
        Assertions.assertEquals(2, referenceEvents.size());
        Assertions.assertTrue(referenceEvents.get(0) instanceof AlterTableDropColumnEvent);
        Assertions.assertEquals(
                "name", ((AlterTableDropColumnEvent) referenceEvents.get(0)).getColumn());
        AlterTableAddColumnEvent readd = (AlterTableAddColumnEvent) referenceEvents.get(1);
        Assertions.assertEquals("name", readd.getColumn().getName());
        Assertions.assertEquals("varchar(64)", readd.getColumn().getSourceType());
        assertReplays(
                referenceBefore,
                referenceOut,
                reference.getProducedCatalogTable().getTableSchema());

        SQLTransform dropReadd = transform("select id, name from products", baseTable());
        TableSchema dropReaddBefore = dropReadd.getProducedCatalogTable().getTableSchema();
        SchemaChangeEvent dropReaddOut =
                dropReadd.mapSchemaChangeEvent(
                        composite(
                                new AlterTableDropColumnEvent(TID, "name"),
                                AlterTableAddColumnEvent.add(
                                        TID,
                                        column("name", BasicType.STRING_TYPE, "varchar(64)"))));
        List<AlterTableColumnEvent> dropReaddEvents =
                ((AlterTableColumnsEvent) dropReaddOut).getEvents();
        Assertions.assertTrue(dropReaddEvents.get(0) instanceof AlterTableDropColumnEvent);
        Assertions.assertTrue(dropReaddEvents.get(1) instanceof AlterTableAddColumnEvent);
        assertReplays(
                dropReaddBefore,
                dropReaddOut,
                dropReadd.getProducedCatalogTable().getTableSchema());

        SQLTransform roundTrip = transform("select id, name from products", baseTable());
        Assertions.assertNull(
                roundTrip.mapSchemaChangeEvent(
                        composite(
                                AlterTableChangeColumnEvent.change(
                                        TID,
                                        "name",
                                        column("tmp", BasicType.STRING_TYPE, "varchar(255)")),
                                AlterTableChangeColumnEvent.change(
                                        TID,
                                        "tmp",
                                        column("name", BasicType.STRING_TYPE, "varchar(255)")))));
    }

    @Test
    public void testModifyDirectReferenceCarriesDialectAndSourceType() {
        SQLTransform transform = transform("select id, name as n from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        SchemaChangeEvent out =
                transform.mapSchemaChangeEvent(
                        AlterTableModifyColumnEvent.modify(
                                TID, column("name", BasicType.STRING_TYPE, "longtext")));

        Assertions.assertTrue(out instanceof AlterTableModifyColumnEvent);
        AlterTableModifyColumnEvent modify = (AlterTableModifyColumnEvent) out;
        Assertions.assertEquals("n", modify.getColumn().getName());
        Assertions.assertEquals("longtext", modify.getColumn().getSourceType());
        Assertions.assertEquals("MySQL", modify.getSourceDialectName());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testModifyDerivedColumnHasNoDialectAndNoSourceType() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("id", BasicType.LONG_TYPE, "bigint"))
                        .column(column("weight", BasicType.FLOAT_TYPE, "float"))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();
        SQLTransform transform =
                transform(
                        "select id, weight, weight * 2 as double_weight from products",
                        table(schema));
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();
        Assertions.assertEquals(
                BasicType.FLOAT_TYPE, before.getColumn("double_weight").getDataType());

        SchemaChangeEvent out =
                transform.mapSchemaChangeEvent(
                        AlterTableModifyColumnEvent.modify(
                                TID, column("weight", BasicType.DOUBLE_TYPE, "double")));

        Assertions.assertTrue(out instanceof AlterTableColumnsEvent);
        List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) out).getEvents();
        Assertions.assertEquals(2, events.size());
        AlterTableModifyColumnEvent weight = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("weight", weight.getColumn().getName());
        Assertions.assertEquals("double", weight.getColumn().getSourceType());
        Assertions.assertEquals("MySQL", weight.getSourceDialectName());
        AlterTableModifyColumnEvent derived = (AlterTableModifyColumnEvent) events.get(1);
        Assertions.assertEquals("double_weight", derived.getColumn().getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, derived.getColumn().getDataType());
        Assertions.assertNull(derived.getColumn().getSourceType());
        Assertions.assertNull(derived.getSourceDialectName());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testCastFixesTheOutputTypeSoModifyIsAbsorbed() {
        SQLTransform transform =
                transform("select id, cast(weight as double) as w from products", baseTable());
        Assertions.assertNull(
                transform.mapSchemaChangeEvent(
                        AlterTableModifyColumnEvent.modify(
                                TID, column("weight", BasicType.FLOAT_TYPE, "float"))));
        Assertions.assertEquals(
                BasicType.DOUBLE_TYPE,
                transform.getProducedCatalogTable().getTableSchema().getColumn("w").getDataType());
    }

    @Test
    public void testSingleModifyAffectingTwoOutputColumnsYieldsComposite() {
        SQLTransform transform =
                transform("select name as x, name as y from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        SchemaChangeEvent out =
                transform.mapSchemaChangeEvent(
                        AlterTableModifyColumnEvent.modify(
                                TID, column("name", BasicType.STRING_TYPE, "longtext")));

        Assertions.assertTrue(out instanceof AlterTableColumnsEvent);
        List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) out).getEvents();
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals(
                "x", ((AlterTableModifyColumnEvent) events.get(0)).getColumn().getName());
        Assertions.assertEquals(
                "y", ((AlterTableModifyColumnEvent) events.get(1)).getColumn().getName());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testWhereComparisonIncompatibleAfterModifyFailsFast() {
        SQLTransform ordering = transform("select id from products where weight > 0", baseTable());
        TransformException error =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                ordering.mapSchemaChangeEvent(
                                        composite(
                                                AlterTableModifyColumnEvent.modify(
                                                        TID,
                                                        column(
                                                                "weight",
                                                                BasicType.STRING_TYPE,
                                                                "varchar(32)")))));
        Assertions.assertTrue(error.getMessage().contains("type compatible"), error.getMessage());

        SQLTransform wrapped =
                transform("select id from products where length(name) > 0", baseTable());
        Assertions.assertNull(
                wrapped.mapSchemaChangeEvent(
                        composite(
                                AlterTableModifyColumnEvent.modify(
                                        TID, column("name", BasicType.STRING_TYPE, "longtext")))));
    }

    @Test
    public void testKeyColumnsCannotBeDroppedOrRenamedButCanBeModified() {
        SQLTransform transform = transform("select * from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();

        TransformException drop =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                transform.mapSchemaChangeEvent(
                                        composite(new AlterTableDropColumnEvent(TID, "id"))));
        Assertions.assertTrue(drop.getMessage().contains("primary key"), drop.getMessage());
        Assertions.assertThrows(
                TransformException.class,
                () ->
                        transform.mapSchemaChangeEvent(
                                composite(
                                        AlterTableChangeColumnEvent.change(
                                                TID,
                                                "id",
                                                column("pid", BasicType.LONG_TYPE, "bigint")))));
        Assertions.assertEquals(before, transform.getProducedCatalogTable().getTableSchema());

        SchemaChangeEvent out =
                transform.mapSchemaChangeEvent(
                        composite(
                                AlterTableModifyColumnEvent.modify(
                                        TID,
                                        column("id", BasicType.LONG_TYPE, "bigint unsigned"))));
        AlterTableModifyColumnEvent modify =
                (AlterTableModifyColumnEvent) ((AlterTableColumnsEvent) out).getEvents().get(0);
        Assertions.assertEquals("bigint unsigned", modify.getColumn().getSourceType());
        assertReplays(before, out, transform.getProducedCatalogTable().getTableSchema());
        Assertions.assertEquals(
                Collections.singletonList("id"),
                transform
                        .getProducedCatalogTable()
                        .getTableSchema()
                        .getPrimaryKey()
                        .getColumnNames());
    }

    @Test
    public void testDuplicateOutputNameFailsFast() {
        SQLTransform transform = transform("select *, name as age from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();
        TransformException error =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> transform.mapSchemaChangeEvent(composite(addAge())));
        Assertions.assertTrue(error.getMessage().contains("not unique"), error.getMessage());
        Assertions.assertEquals(before, transform.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testTableLevelEventsAreForwardedAndCommentApplied() {
        SQLTransform transform = transform("select * from products", baseTable());
        AlterTableCommentEvent comment = AlterTableCommentEvent.of(TID, "products", "catalog");
        Assertions.assertSame(comment, transform.mapSchemaChangeEvent(comment));
        Assertions.assertEquals("catalog", transform.getProducedCatalogTable().getComment());

        AlterTableNameEvent rename =
                new AlterTableNameEvent(TID, TableIdentifier.of("catalog", "db", "items"));
        Assertions.assertSame(rename, transform.mapSchemaChangeEvent(rename));
        Assertions.assertEquals(TBL, transform.getProducedCatalogTable().getTablePath());
    }

    @Test
    public void testResyncFromEventCarryingTheWholeTable() {
        SQLTransform transform = transform("select * from products", baseTable());
        AlterTableEvent resync =
                new AlterTableEvent(TID) {
                    @Override
                    public EventType getEventType() {
                        return EventType.SCHEMA_CHANGE_UPDATE_COLUMNS;
                    }
                };
        resync.setChangeAfter(apply(baseTable(), composite(addAge())));

        SchemaChangeEvent out = transform.mapSchemaChangeEvent(resync);

        Assertions.assertSame(resync, out);
        Assertions.assertSame(transform.getProducedCatalogTable(), out.getChangeAfter());
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "weight", "age"},
                transform.getProducedCatalogTable().getTableSchema().getFieldNames());
        List<SeaTunnelRow> rows =
                transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}));
        Assertions.assertEquals(4, rows.get(0).getArity());
    }

    @Test
    public void testStagedHandOffTranslatesLikeChainPositionZero() {
        SQLTransform positionZero = transform("select * from products", baseTable());
        SchemaChangeEvent expected = positionZero.mapSchemaChangeEvent(composite(addAge()));

        SQLTransform staged = transform("select * from products", baseTable());
        TableSchema before = staged.getProducedCatalogTable().getTableSchema();
        CatalogTable handed = apply(baseTable(), composite(addAge()));
        staged.setInputCatalogTable(handed);
        staged.setInputCatalogTable(handed);
        SchemaChangeEvent out = staged.mapSchemaChangeEvent(composite(addAge()));

        Assertions.assertEquals(
                describe(expected),
                describe(out),
                "staged hand-off must translate like position zero");
        assertReplays(before, out, staged.getProducedCatalogTable().getTableSchema());
        Assertions.assertEquals(
                positionZero.getProducedCatalogTable().getTableSchema(),
                staged.getProducedCatalogTable().getTableSchema());
        List<SeaTunnelRow> rows =
                staged.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}));
        Assertions.assertEquals(4, rows.get(0).getArity());
    }

    @Test
    public void testStagedHandOffRejectedEventLeavesStateUntouched() {
        SQLTransform transform = transform("select id, name from products", baseTable());
        TableSchema before = transform.getProducedCatalogTable().getTableSchema();
        AlterTableColumnsEvent dropName = composite(new AlterTableDropColumnEvent(TID, "name"));
        transform.setInputCatalogTable(apply(baseTable(), dropName));

        Assertions.assertThrows(
                TransformException.class, () -> transform.mapSchemaChangeEvent(dropName));

        Assertions.assertEquals(before, transform.getProducedCatalogTable().getTableSchema());
        List<SeaTunnelRow> rows = transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d}));
        Assertions.assertEquals(2, rows.get(0).getArity());
    }

    @Test
    public void testStagedHandOffThatDisagreesWithTheEventFailsFast() {
        SQLTransform transform = transform("select * from products", baseTable());
        transform.setInputCatalogTable(
                apply(
                        baseTable(),
                        composite(
                                AlterTableAddColumnEvent.add(
                                        TID, column("other", BasicType.INT_TYPE, "int")))));
        TransformException error =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> transform.mapSchemaChangeEvent(composite(addAge())));
        Assertions.assertTrue(error.getMessage().contains("upstream"), error.getMessage());
    }

    @Test
    public void testRowDuringPendingHandOffFailsAndEqualHandOffIsNoOp() {
        SQLTransform transform = transform("select id, name from products", baseTable());
        transform.setInputCatalogTable(baseTable());
        Assertions.assertEquals(
                2,
                transform
                        .flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d}))
                        .get(0)
                        .getArity());

        transform.setInputCatalogTable(apply(baseTable(), composite(addAge())));
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20})));
    }

    @Test
    public void testAllMatchRulesAtEnginePositionGreaterThanZero() {
        Map<String, Object> firstRule = new HashMap<>();
        firstRule.put("table_path", TBL.getFullName());
        firstRule.put("query", "select *, weight * 2 as double_weight from products");
        Map<String, Object> secondRule = new HashMap<>();
        secondRule.put("table_path", TBL.getFullName());
        secondRule.put("query", "select *, double_weight as dw2 from products");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("query", "select * from products");
        cfg.put("table_transform", Arrays.asList(firstRule, secondRule));
        cfg.put("rule_match_mode", "ALL_MATCH");
        SQLMultiCatalogFlatMapTransform wrapper =
                new SQLMultiCatalogFlatMapTransform(
                        Collections.singletonList(baseTable()), ReadonlyConfig.fromMap(cfg));
        TableSchema before = wrapper.getProducedCatalogTable().getTableSchema();
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "weight", "double_weight", "dw2"},
                before.getFieldNames());

        // The engine hands the upstream produced table to every transform after the first one
        // before it dispatches the event.
        wrapper.setInputCatalogTables(
                Collections.singletonList(apply(baseTable(), composite(addAge()))));
        SchemaChangeEvent out = wrapper.mapSchemaChangeEvent(composite(addAge()));

        Assertions.assertNotNull(out);
        List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) out).getEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableAddColumnEvent add = (AlterTableAddColumnEvent) events.get(0);
        Assertions.assertEquals("age", add.getColumn().getName());
        Assertions.assertEquals("weight", add.getAfterColumn());
        TableSchema after = wrapper.getProducedCatalogTable().getTableSchema();
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "weight", "age", "double_weight", "dw2"},
                after.getFieldNames());
        assertReplays(before, out, after);
        Assertions.assertSame(wrapper.getProducedCatalogTable(), out.getChangeAfter());

        SeaTunnelRow post = new SeaTunnelRow(new Object[] {1L, "a", 1.5d, 20});
        post.setTableId(TBL.getFullName());
        List<SeaTunnelRow> rows = wrapper.flatMap(post);
        Assertions.assertEquals(6, rows.get(0).getArity());
        Assertions.assertEquals(20, rows.get(0).getField(3));
        Assertions.assertEquals(3.0d, ((Number) rows.get(0).getField(5)).doubleValue(), 0.0001d);
    }

    @Test
    public void testFieldRenameThenSqlChainTranslatesTheRenamedEvent() {
        CatalogTable base = baseTable();
        FieldRenameTransform rename =
                new FieldRenameTransform(
                        new FieldRenameConfig().setConvertCase(ConvertCase.UPPER), base);
        CatalogTable renamed = rename.getProducedCatalogTable();
        SQLTransform sql = transform("select * from products", renamed);
        TableSchema before = sql.getProducedCatalogTable().getTableSchema();

        SchemaChangeEvent renamedEvent =
                rename.mapSchemaChangeEvent(
                        composite(
                                AlterTableModifyColumnEvent.modify(
                                        TID, column("name", BasicType.STRING_TYPE, "longtext"))));
        sql.setInputCatalogTable(apply(renamed, renamedEvent));
        SchemaChangeEvent out = sql.mapSchemaChangeEvent(renamedEvent);

        AlterTableModifyColumnEvent modify =
                (AlterTableModifyColumnEvent) ((AlterTableColumnsEvent) out).getEvents().get(0);
        Assertions.assertEquals("NAME", modify.getColumn().getName());
        Assertions.assertEquals("longtext", modify.getColumn().getSourceType());
        assertReplays(before, out, sql.getProducedCatalogTable().getTableSchema());
    }

    @Test
    public void testEngineAndUdfLifecycleBalancesAcrossChanges() {
        AtomicInteger inits = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        CountingUdf udf = new CountingUdf();
        CountingSqlTransform transform =
                new CountingSqlTransform(
                        "select * from products where weight > 0", baseTable(), inits, closes, udf);
        transform.getProducedCatalogTable();
        Assertions.assertEquals(1, inits.get());
        Assertions.assertEquals(0, closes.get());

        transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d}));
        Assertions.assertEquals(1, udf.opens.get());

        Assertions.assertNotNull(transform.mapSchemaChangeEvent(composite(addAge())));
        Assertions.assertEquals(2, inits.get(), "one candidate engine per change");
        Assertions.assertEquals(1, closes.get(), "the retired live engine is closed");
        Assertions.assertEquals(1, udf.closes.get(), "UDFs of the retired engine are closed");
        transform.flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}));
        Assertions.assertEquals(2, udf.opens.get());

        // Rejected before evaluation: the primary key column cannot be dropped.
        Assertions.assertThrows(
                TransformException.class,
                () ->
                        transform.mapSchemaChangeEvent(
                                composite(new AlterTableDropColumnEvent(TID, "id"))));
        Assertions.assertEquals(2, inits.get());
        Assertions.assertEquals(1, closes.get());

        // Rejected after evaluation: the WHERE comparison becomes type incompatible.
        AlterTableColumnsEvent weightToString =
                composite(
                        AlterTableModifyColumnEvent.modify(
                                TID, column("weight", BasicType.STRING_TYPE, "varchar(32)")));
        Assertions.assertThrows(
                TransformException.class, () -> transform.mapSchemaChangeEvent(weightToString));
        Assertions.assertEquals(3, inits.get(), "the candidate engine was created");
        Assertions.assertEquals(2, closes.get(), "the candidate engine was closed");
        Assertions.assertEquals(1, udf.closes.get(), "the live engine keeps its UDFs");

        // Rejected on the staged path as well.
        CatalogTable current = transform.getProducedCatalogTable();
        transform.setInputCatalogTable(apply(current, weightToString));
        Assertions.assertThrows(
                TransformException.class, () -> transform.mapSchemaChangeEvent(weightToString));
        Assertions.assertEquals(4, inits.get());
        Assertions.assertEquals(3, closes.get());
        Assertions.assertEquals(
                4,
                transform
                        .flatMap(new SeaTunnelRow(new Object[] {1L, "a", 1.0d, 20}))
                        .get(0)
                        .getArity());

        transform.close();
        Assertions.assertEquals(inits.get(), closes.get());
        Assertions.assertEquals(udf.opens.get(), udf.closes.get());
    }

    private static PhysicalColumn column(
            String name, SeaTunnelDataType<?> type, String sourceType) {
        return PhysicalColumn.of(
                name, type, (Long) null, true, null, null, sourceType, new HashMap<>());
    }

    private static CatalogTable table(TableSchema schema) {
        return CatalogTable.of(TID, schema, new HashMap<>(), new ArrayList<>(), "products");
    }

    private static CatalogTable baseTable() {
        return table(
                TableSchema.builder()
                        .column(column("id", BasicType.LONG_TYPE, "bigint"))
                        .column(column("name", BasicType.STRING_TYPE, "varchar(255)"))
                        .column(column("weight", BasicType.DOUBLE_TYPE, "double"))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build());
    }

    private static SQLTransform transform(String query, CatalogTable input) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("query", query);
        SQLTransform transform = new SQLTransform(ReadonlyConfig.fromMap(cfg), input);
        transform.getProducedCatalogTable();
        return transform;
    }

    private static AlterTableAddColumnEvent addAge() {
        return AlterTableAddColumnEvent.add(TID, column("age", BasicType.INT_TYPE, "int"));
    }

    private static AlterTableColumnsEvent composite(AlterTableColumnEvent... events) {
        AlterTableColumnsEvent composite =
                new AlterTableColumnsEvent(TID, new ArrayList<>(Arrays.asList(events)));
        composite.setJobId("job-1");
        composite.setStatement("alter table products change");
        composite.setSourceDialectName("MySQL");
        for (AlterTableColumnEvent event : events) {
            event.setJobId("job-1");
            event.setStatement("alter table products change");
            event.setSourceDialectName("MySQL");
        }
        return composite;
    }

    private static CatalogTable apply(CatalogTable input, SchemaChangeEvent event) {
        TableSchema schema =
                new AlterTableSchemaEventHandler().reset(input.getTableSchema()).apply(event);
        return CatalogTable.of(
                input.getTableId(),
                schema,
                input.getOptions(),
                input.getPartitionKeys(),
                input.getComment(),
                input.getTableId().getCatalogName(),
                input.getMetadataSchema());
    }

    private static void assertReplays(
            TableSchema before, SchemaChangeEvent out, TableSchema produced) {
        TableSchema replayed = new AlterTableSchemaEventHandler().reset(before).apply(out);
        Assertions.assertEquals(
                produced,
                replayed,
                "emitted events must replay onto the pre-event produced schema");
    }

    private static String describe(SchemaChangeEvent event) {
        StringBuilder text = new StringBuilder(event.getClass().getSimpleName());
        for (AlterTableColumnEvent sub : SQLSchemaChangeTranslator.flatten(event)) {
            text.append('|').append(sub.getEventType());
            if (sub instanceof AlterTableAddColumnEvent) {
                AlterTableAddColumnEvent add = (AlterTableAddColumnEvent) sub;
                text.append(':')
                        .append(add.getColumn().getName())
                        .append(':')
                        .append(add.isFirst())
                        .append(':')
                        .append(add.getAfterColumn());
            }
        }
        return text.toString();
    }

    /** Test seam that counts engine and UDF lifecycles. */
    private static final class CountingSqlTransform extends SQLTransform {
        private final AtomicInteger inits;
        private final AtomicInteger closes;
        private final CountingUdf udf;

        private CountingSqlTransform(
                String query,
                CatalogTable input,
                AtomicInteger inits,
                AtomicInteger closes,
                CountingUdf udf) {
            super(config(query), input);
            this.inits = inits;
            this.closes = closes;
            this.udf = udf;
        }

        private static ReadonlyConfig config(String query) {
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("query", query);
            return ReadonlyConfig.fromMap(cfg);
        }

        @Override
        protected SQLEngine createSqlEngine() {
            return new CountingEngine(inits, closes, udf);
        }
    }

    /** Zeta engine that counts init and close calls and exposes one counting UDF. */
    private static final class CountingEngine extends ZetaSQLEngine {
        private final AtomicInteger inits;
        private final AtomicInteger closes;
        private final CountingUdf udf;

        private CountingEngine(AtomicInteger inits, AtomicInteger closes, CountingUdf udf) {
            this.inits = inits;
            this.closes = closes;
            this.udf = udf;
        }

        @Override
        public void init(
                String inputTableName,
                String catalogTableName,
                SeaTunnelRowType inputRowType,
                String sql) {
            inits.incrementAndGet();
            super.init(inputTableName, catalogTableName, inputRowType, sql);
        }

        @Override
        protected List<ZetaUDF> loadUDFs() {
            return Collections.singletonList(udf);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            super.close();
        }
    }

    /** UDF that counts how often it is opened and closed. */
    private static final class CountingUdf implements ZetaUDF {
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public String functionName() {
            return "COUNTING_UDF";
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            return "counted";
        }

        @Override
        public void open() {
            opens.incrementAndGet();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
