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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Covers the identity-tracked translation of column DDL into output-relative events: lineage
 * through renames and re-creations, ordering of the emitted events, position derivation, key
 * protection, the replay invariant and the shape and metadata rules of the rebuilt event.
 */
public class SQLSchemaChangeTranslatorTest {

    private static final TableIdentifier TID = TableIdentifier.of("catalog", "db", "t");

    @Test
    public void testLineageTracksRenameAndReuse() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        SQLLineageSchema lineage = SQLLineageSchema.initial(pre);
        lineage =
                lineage.apply(
                        AlterTableChangeColumnEvent.change(TID, "a", col("c", BasicType.INT_TYPE)));
        lineage = lineage.apply(AlterTableAddColumnEvent.add(TID, col("a", BasicType.LONG_TYPE)));

        Assertions.assertArrayEquals(
                new String[] {"c", "b", "a"}, lineage.getSchema().getFieldNames());
        Assertions.assertEquals(0, lineage.identityOf("c"));
        Assertions.assertEquals(1, lineage.identityOf("b"));
        Assertions.assertEquals(2, lineage.identityOf("a"));
        Assertions.assertEquals(-1, lineage.identityOf("missing"));
        Assertions.assertNotNull(lineage.creatingHint(2));
        Assertions.assertNull(lineage.creatingHint(0));
    }

    @Test
    public void testLineageRejectsEventsThatDoNotApply() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        SQLLineageSchema lineage = SQLLineageSchema.initial(pre);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> lineage.apply(new AlterTableDropColumnEvent(TID, "x")));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        lineage.apply(
                                AlterTableChangeColumnEvent.change(
                                        TID, "a", col("b", BasicType.INT_TYPE))));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        lineage.apply(
                                AlterTableAddColumnEvent.addAfter(
                                        TID, col("c", BasicType.INT_TYPE), "missing")));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        lineage.apply(
                                AlterTableModifyColumnEvent.modifyAfter(
                                        TID, col("a", BasicType.INT_TYPE), "a")));
    }

    @Test
    public void testRenameAndReuseWithStarKeepsLineage() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        List<AlterTableColumnEvent> hints =
                Arrays.asList(
                        AlterTableChangeColumnEvent.change(TID, "a", col("c", BasicType.INT_TYPE)),
                        AlterTableAddColumnEvent.add(TID, col("a", BasicType.LONG_TYPE)));

        List<AlterTableColumnEvent> out = translateStar(pre, hints);

        Assertions.assertEquals(2, out.size());
        AlterTableChangeColumnEvent change = (AlterTableChangeColumnEvent) out.get(0);
        Assertions.assertEquals("a", change.getOldColumn());
        Assertions.assertEquals("c", change.getColumn().getName());
        Assertions.assertFalse(change.isFirst());
        Assertions.assertNull(change.getAfterColumn());
        AlterTableAddColumnEvent add = (AlterTableAddColumnEvent) out.get(1);
        Assertions.assertEquals("a", add.getColumn().getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, add.getColumn().getDataType());
        Assertions.assertFalse(add.isFirst());
        Assertions.assertNull(add.getAfterColumn());
    }

    @Test
    public void testDropAndReaddNeverCollapsesToModify() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        List<AlterTableColumnEvent> hints =
                Arrays.asList(
                        new AlterTableDropColumnEvent(TID, "a"),
                        AlterTableAddColumnEvent.add(TID, col("a", BasicType.LONG_TYPE)));

        List<AlterTableColumnEvent> out = translateStar(pre, hints);

        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof AlterTableDropColumnEvent);
        Assertions.assertEquals("a", ((AlterTableDropColumnEvent) out.get(0)).getColumn());
        Assertions.assertTrue(out.get(1) instanceof AlterTableAddColumnEvent);
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                ((AlterTableAddColumnEvent) out.get(1)).getColumn().getDataType());
    }

    @Test
    public void testRenameAwayAndBackIsAbsorbed() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        List<AlterTableColumnEvent> hints =
                Arrays.asList(
                        AlterTableChangeColumnEvent.change(TID, "a", col("x", BasicType.INT_TYPE)),
                        AlterTableChangeColumnEvent.change(TID, "x", col("a", BasicType.INT_TYPE)));

        Assertions.assertTrue(translateStar(pre, hints).isEmpty());
    }

    @Test
    public void testDropThenRenameOntoFreedNameIsOrderedSafely() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        List<AlterTableColumnEvent> hints =
                Arrays.asList(
                        new AlterTableDropColumnEvent(TID, "a"),
                        AlterTableChangeColumnEvent.change(
                                TID, "b", col("a", BasicType.STRING_TYPE)));

        List<AlterTableColumnEvent> out = translateStar(pre, hints);

        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof AlterTableDropColumnEvent);
        AlterTableChangeColumnEvent change = (AlterTableChangeColumnEvent) out.get(1);
        Assertions.assertEquals("b", change.getOldColumn());
        Assertions.assertEquals("a", change.getColumn().getName());
    }

    @Test
    public void testRenameCycleFailsFast() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.INT_TYPE));
        List<AlterTableColumnEvent> hints =
                Arrays.asList(
                        AlterTableChangeColumnEvent.change(
                                TID, "a", col("tmp", BasicType.INT_TYPE)),
                        AlterTableChangeColumnEvent.change(TID, "b", col("a", BasicType.INT_TYPE)),
                        AlterTableChangeColumnEvent.change(
                                TID, "tmp", col("b", BasicType.INT_TYPE)));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> translateStar(pre, hints));
        Assertions.assertTrue(error.getMessage().contains("cycle"), error.getMessage());
    }

    @Test
    public void testReferenceSlotWithReplacedIdentityIsDroppedAndReadded() {
        TableSchema preInput =
                schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        SQLLineageSchema initial = SQLLineageSchema.initial(preInput);
        SQLLineageSchema lineage = initial;
        lineage = lineage.apply(new AlterTableDropColumnEvent(TID, "a"));
        lineage = lineage.apply(AlterTableAddColumnEvent.add(TID, col("a", BasicType.LONG_TYPE)));

        List<SQLOutputSlot> slots = Collections.singletonList(SQLOutputSlot.reference(0, "a", "a"));
        TableSchema preOutput = schema(col("a", BasicType.INT_TYPE));
        TableSchema finalOutput = schema(col("a", BasicType.LONG_TYPE));

        List<AlterTableColumnEvent> out =
                SQLSchemaChangeTranslator.translate(
                        TID, slots, preOutput, initial, slots, finalOutput, lineage);

        Assertions.assertEquals(2, out.size());
        Assertions.assertTrue(out.get(0) instanceof AlterTableDropColumnEvent);
        Assertions.assertTrue(out.get(1) instanceof AlterTableAddColumnEvent);
        SQLSchemaChangeTranslator.verifyReplay(preOutput, finalOutput, out);
    }

    @Test
    public void testExpressionSlotIsModifiedOnlyWhenItsDefinitionChanges() {
        TableSchema preInput = schema(col("a", BasicType.INT_TYPE));
        SQLLineageSchema initial = SQLLineageSchema.initial(preInput);
        SQLLineageSchema lineage =
                initial.apply(
                        AlterTableModifyColumnEvent.modify(TID, col("a", BasicType.LONG_TYPE)));
        List<SQLOutputSlot> slots =
                Collections.singletonList(
                        SQLOutputSlot.expression(0, "d", Collections.singletonList("a")));

        TableSchema unchangedOutput = schema(derived("d", BasicType.INT_TYPE));
        Assertions.assertTrue(
                SQLSchemaChangeTranslator.translate(
                                TID,
                                slots,
                                unchangedOutput,
                                initial,
                                slots,
                                unchangedOutput,
                                lineage)
                        .isEmpty());

        TableSchema changedOutput = schema(derived("d", BasicType.LONG_TYPE));
        List<AlterTableColumnEvent> out =
                SQLSchemaChangeTranslator.translate(
                        TID, slots, unchangedOutput, initial, slots, changedOutput, lineage);
        Assertions.assertEquals(1, out.size());
        AlterTableModifyColumnEvent modify = (AlterTableModifyColumnEvent) out.get(0);
        Assertions.assertEquals("d", modify.getColumn().getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, modify.getColumn().getDataType());
        Assertions.assertFalse(modify.isFirst());
        Assertions.assertNull(modify.getAfterColumn());
        SQLSchemaChangeTranslator.verifyReplay(unchangedOutput, changedOutput, out);
    }

    @Test
    public void testAddPositionsFollowTheOutputLayout() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));

        AlterTableAddColumnEvent appended =
                (AlterTableAddColumnEvent)
                        translateStar(
                                        pre,
                                        Collections.singletonList(
                                                AlterTableAddColumnEvent.add(
                                                        TID, col("c", BasicType.INT_TYPE))))
                                .get(0);
        Assertions.assertFalse(appended.isFirst());
        Assertions.assertNull(appended.getAfterColumn());

        AlterTableAddColumnEvent afterA =
                (AlterTableAddColumnEvent)
                        translateStar(
                                        pre,
                                        Collections.singletonList(
                                                AlterTableAddColumnEvent.addAfter(
                                                        TID, col("c", BasicType.INT_TYPE), "a")))
                                .get(0);
        Assertions.assertEquals("a", afterA.getAfterColumn());

        AlterTableAddColumnEvent first =
                (AlterTableAddColumnEvent)
                        translateStar(
                                        pre,
                                        Collections.singletonList(
                                                AlterTableAddColumnEvent.addFirst(
                                                        TID, col("c", BasicType.INT_TYPE))))
                                .get(0);
        Assertions.assertTrue(first.isFirst());

        // An appended source column lands before a trailing expression column in the output.
        SQLLineageSchema initial = SQLLineageSchema.initial(pre);
        SQLLineageSchema lineage =
                initial.apply(AlterTableAddColumnEvent.add(TID, col("c", BasicType.INT_TYPE)));
        List<SQLOutputSlot> preSlots =
                Arrays.asList(
                        SQLOutputSlot.star(0, "a", "a"),
                        SQLOutputSlot.star(0, "b", "b"),
                        SQLOutputSlot.expression(1, "x", Collections.singletonList("a")));
        List<SQLOutputSlot> finalSlots =
                Arrays.asList(
                        SQLOutputSlot.star(0, "a", "a"),
                        SQLOutputSlot.star(0, "b", "b"),
                        SQLOutputSlot.star(0, "c", "c"),
                        SQLOutputSlot.expression(1, "x", Collections.singletonList("a")));
        TableSchema preOutput =
                schema(
                        col("a", BasicType.INT_TYPE),
                        col("b", BasicType.STRING_TYPE),
                        derived("x", BasicType.INT_TYPE));
        TableSchema finalOutput =
                schema(
                        col("a", BasicType.INT_TYPE),
                        col("b", BasicType.STRING_TYPE),
                        lineage.getSchema().getColumn("c"),
                        derived("x", BasicType.INT_TYPE));
        List<AlterTableColumnEvent> out =
                SQLSchemaChangeTranslator.translate(
                        TID, preSlots, preOutput, initial, finalSlots, finalOutput, lineage);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("b", ((AlterTableAddColumnEvent) out.get(0)).getAfterColumn());
        SQLSchemaChangeTranslator.verifyReplay(preOutput, finalOutput, out);
    }

    @Test
    public void testModifyCarriesPositionOnlyWhenTheColumnMoved() {
        TableSchema pre =
                schema(
                        col("a", BasicType.INT_TYPE),
                        col("b", BasicType.STRING_TYPE),
                        col("c", BasicType.INT_TYPE));

        List<AlterTableColumnEvent> moved =
                translateStar(
                        pre,
                        Collections.singletonList(
                                AlterTableModifyColumnEvent.modifyAfter(
                                        TID, col("c", BasicType.INT_TYPE), "a")));
        Assertions.assertEquals(1, moved.size());
        AlterTableModifyColumnEvent movedModify = (AlterTableModifyColumnEvent) moved.get(0);
        Assertions.assertEquals("c", movedModify.getColumn().getName());
        Assertions.assertEquals("a", movedModify.getAfterColumn());

        List<AlterTableColumnEvent> retyped =
                translateStar(
                        pre,
                        Collections.singletonList(
                                AlterTableModifyColumnEvent.modify(
                                        TID, col("b", BasicType.LONG_TYPE))));
        Assertions.assertEquals(1, retyped.size());
        AlterTableModifyColumnEvent retypedModify = (AlterTableModifyColumnEvent) retyped.get(0);
        Assertions.assertEquals(BasicType.LONG_TYPE, retypedModify.getColumn().getDataType());
        Assertions.assertFalse(retypedModify.isFirst());
        Assertions.assertNull(retypedModify.getAfterColumn());
    }

    @Test
    public void testCommentOnlyChangeEmitsCommentEvent() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE));
        List<AlterTableColumnEvent> out =
                translateStar(
                        pre,
                        Collections.singletonList(
                                AlterColumnCommentEvent.of(TID, "a", null, "first column")));
        Assertions.assertEquals(1, out.size());
        AlterColumnCommentEvent comment = (AlterColumnCommentEvent) out.get(0);
        Assertions.assertEquals("a", comment.getColumn());
        Assertions.assertEquals("first column", comment.getNewComment());
    }

    @Test
    public void testProtectedColumnsCannotBeDroppedOrRenamed() {
        TableSchema preInput =
                schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        TableSchema preOutput =
                TableSchema.builder()
                        .columns(preInput.getColumns())
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("a")))
                        .build();
        SQLLineageSchema initial = SQLLineageSchema.initial(preInput);
        Set<Integer> protectedIdentities =
                SQLSchemaChangeTranslator.protectedIdentities(
                        starSlots(preInput), preOutput, Collections.singletonList("b"), initial);
        Assertions.assertEquals(2, protectedIdentities.size());

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SQLSchemaChangeTranslator.rejectProtectedColumnChange(
                                initial,
                                new AlterTableDropColumnEvent(TID, "a"),
                                protectedIdentities));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SQLSchemaChangeTranslator.rejectProtectedColumnChange(
                                initial,
                                AlterTableChangeColumnEvent.change(
                                        TID, "b", col("z", BasicType.STRING_TYPE)),
                                protectedIdentities));
        SQLSchemaChangeTranslator.rejectProtectedColumnChange(
                initial,
                AlterTableModifyColumnEvent.modify(TID, col("a", BasicType.LONG_TYPE)),
                protectedIdentities);
    }

    @Test
    public void testVerifyReplayDetectsNameCollisionsAndDivergence() {
        TableSchema pre = schema(col("a", BasicType.INT_TYPE), col("b", BasicType.STRING_TYPE));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        SQLSchemaChangeTranslator.verifyReplay(
                                pre,
                                pre,
                                Collections.singletonList(
                                        AlterTableAddColumnEvent.add(
                                                TID, col("a", BasicType.INT_TYPE)))));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        SQLSchemaChangeTranslator.verifyReplay(
                                pre,
                                schema(
                                        col("a", BasicType.LONG_TYPE),
                                        col("b", BasicType.STRING_TYPE)),
                                Collections.emptyList()));
    }

    @Test
    public void testRebuildShapeAndMetadata() {
        CatalogTable produced =
                CatalogTable.of(
                        TID,
                        schema(col("a", BasicType.INT_TYPE)),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        Column sourced =
                PhysicalColumn.of(
                        "a",
                        BasicType.INT_TYPE,
                        (Long) null,
                        true,
                        null,
                        null,
                        "int",
                        new HashMap<>());
        Column derived = derived("d", BasicType.INT_TYPE);

        AlterTableAddColumnEvent incoming = AlterTableAddColumnEvent.add(TID, sourced);
        incoming.setJobId("job-1");
        incoming.setStatement("alter table t add column a int");
        incoming.setSourceDialectName("MySQL");

        SchemaChangeEvent single =
                SQLSchemaChangeTranslator.rebuild(
                        incoming,
                        TID,
                        Collections.singletonList(AlterTableAddColumnEvent.add(TID, sourced)),
                        produced);
        Assertions.assertTrue(single instanceof AlterTableAddColumnEvent);
        Assertions.assertEquals("job-1", single.getJobId());
        Assertions.assertEquals(
                "MySQL", ((AlterTableAddColumnEvent) single).getSourceDialectName());
        Assertions.assertSame(produced, single.getChangeAfter());

        SchemaChangeEvent composite =
                SQLSchemaChangeTranslator.rebuild(
                        incoming,
                        TID,
                        Arrays.asList(
                                AlterTableModifyColumnEvent.modify(TID, derived),
                                AlterTableModifyColumnEvent.modify(TID, sourced)),
                        produced);
        Assertions.assertTrue(composite instanceof AlterTableColumnsEvent);
        List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) composite).getEvents();
        Assertions.assertEquals(2, events.size());
        Assertions.assertNull(events.get(0).getSourceDialectName());
        Assertions.assertEquals("MySQL", events.get(1).getSourceDialectName());
        Assertions.assertEquals("alter table t add column a int", events.get(0).getStatement());
        Assertions.assertSame(produced, events.get(0).getChangeAfter());
        Assertions.assertEquals(
                "MySQL", ((AlterTableColumnsEvent) composite).getSourceDialectName());

        AlterTableColumnsEvent incomingComposite =
                new AlterTableColumnsEvent(TID, Collections.singletonList(incoming));
        SchemaChangeEvent compositeOut =
                SQLSchemaChangeTranslator.rebuild(
                        incomingComposite,
                        TID,
                        Collections.singletonList(AlterTableAddColumnEvent.add(TID, sourced)),
                        produced);
        Assertions.assertTrue(compositeOut instanceof AlterTableColumnsEvent);
    }

    @Test
    public void testFlattenHandlesEveryShape() {
        AlterTableAddColumnEvent add =
                AlterTableAddColumnEvent.add(TID, col("a", BasicType.INT_TYPE));
        Assertions.assertEquals(1, SQLSchemaChangeTranslator.flatten(add).size());
        Assertions.assertEquals(
                1,
                SQLSchemaChangeTranslator.flatten(
                                new AlterTableColumnsEvent(TID, Collections.singletonList(add)))
                        .size());
        Assertions.assertTrue(
                SQLSchemaChangeTranslator.flatten(
                                org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent
                                        .of(TID, null, "c"))
                        .isEmpty());
    }

    private static List<AlterTableColumnEvent> translateStar(
            TableSchema pre, List<AlterTableColumnEvent> hints) {
        SQLLineageSchema initial = SQLLineageSchema.initial(pre);
        SQLLineageSchema lineage = initial;
        for (AlterTableColumnEvent hint : hints) {
            lineage = lineage.apply(hint);
        }
        TableSchema finalOutput = lineage.getSchema();
        List<AlterTableColumnEvent> out =
                SQLSchemaChangeTranslator.translate(
                        TID,
                        starSlots(pre),
                        pre,
                        initial,
                        starSlots(finalOutput),
                        finalOutput,
                        lineage);
        SQLSchemaChangeTranslator.verifyReplay(pre, finalOutput, out);
        return out;
    }

    private static List<SQLOutputSlot> starSlots(TableSchema schema) {
        List<SQLOutputSlot> slots = new ArrayList<>();
        for (String name : schema.getFieldNames()) {
            slots.add(SQLOutputSlot.star(0, name, name));
        }
        return slots;
    }

    private static TableSchema schema(Column... columns) {
        return TableSchema.builder().columns(Arrays.asList(columns)).build();
    }

    private static PhysicalColumn col(String name, SeaTunnelDataType<?> type) {
        return PhysicalColumn.of(name, type, (Long) null, true, null, null);
    }

    private static PhysicalColumn derived(String name, SeaTunnelDataType<?> type) {
        return PhysicalColumn.of(name, type, 0, true, null, null);
    }
}
