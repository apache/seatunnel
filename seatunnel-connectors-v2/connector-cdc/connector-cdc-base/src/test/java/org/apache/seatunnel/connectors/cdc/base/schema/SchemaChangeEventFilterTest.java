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

package org.apache.seatunnel.connectors.cdc.base.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class SchemaChangeEventFilterTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("", "db", "tbl");

    private static Column column(String name) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 10L, true, null, "");
    }

    private static SchemaChangeEventFilter filter(List<String> include, List<String> exclude) {
        Map<String, Object> map = new HashMap<>();
        map.put("schema-changes.include", include);
        map.put("schema-changes.exclude", exclude);
        return SchemaChangeEventFilter.fromConfig(ReadonlyConfig.fromMap(map));
    }

    private static AlterTableColumnsEvent composite(AlterTableColumnEvent... events) {
        return new AlterTableColumnsEvent(TABLE, new ArrayList<>(Arrays.asList(events)));
    }

    private static List<EventType> subTypes(SchemaChangeEvent event) {
        return ((AlterTableColumnsEvent) event)
                .getEvents().stream()
                        .map(AlterTableColumnEvent::getEventType)
                        .collect(Collectors.toList());
    }

    @Test
    void noConfigIsAllowAll() {
        SchemaChangeEventFilter f = filter(Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(f.isNoOp());
        AlterTableColumnsEvent event =
                composite(
                        AlterTableAddColumnEvent.add(TABLE, column("a")),
                        new AlterTableDropColumnEvent(TABLE, "b"));
        Assertions.assertSame(event, f.filter(event));
    }

    @Test
    void includeOnlyKeepsOnlyListedLeafTypes() {
        SchemaChangeEventFilter f = filter(Arrays.asList("add.column"), Collections.emptyList());
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b"),
                                AlterTableModifyColumnEvent.modify(TABLE, column("c"))));
        Assertions.assertEquals(
                Collections.singletonList(EventType.SCHEMA_CHANGE_ADD_COLUMN), subTypes(result));
    }

    @Test
    void excludeOnlyDropsListedLeafTypes() {
        SchemaChangeEventFilter f = filter(Collections.emptyList(), Arrays.asList("drop.column"));
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b")));
        Assertions.assertEquals(
                Collections.singletonList(EventType.SCHEMA_CHANGE_ADD_COLUMN), subTypes(result));
    }

    @Test
    void excludeWinsOverIncludeForSameType() {
        SchemaChangeEventFilter f =
                filter(Arrays.asList("add.column", "drop.column"), Arrays.asList("add.column"));
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b")));
        Assertions.assertEquals(
                Collections.singletonList(EventType.SCHEMA_CHANGE_DROP_COLUMN), subTypes(result));
    }

    @Test
    void wholeEventDroppedWhenNoSubEventSurvives() {
        SchemaChangeEventFilter f =
                filter(Collections.emptyList(), Arrays.asList("add.column", "drop.column"));
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b")));
        Assertions.assertNull(result);
    }

    @Test
    void changeColumnIsFilteredIndependentlyOfModifyColumn() {
        // exclude modify.column must NOT drop a change.column (rename) sub-event
        SchemaChangeEventFilter f = filter(Collections.emptyList(), Arrays.asList("modify.column"));
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableModifyColumnEvent.modify(TABLE, column("c")),
                                AlterTableChangeColumnEvent.change(TABLE, "old", column("new"))));
        Assertions.assertEquals(
                Collections.singletonList(EventType.SCHEMA_CHANGE_CHANGE_COLUMN), subTypes(result));
    }

    @Test
    void updateColumnsIncludeIsGroupAliasForAllColumnChanges() {
        SchemaChangeEventFilter f =
                filter(Arrays.asList("update.columns"), Collections.emptyList());
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b"),
                                AlterTableModifyColumnEvent.modify(TABLE, column("c"))));
        Assertions.assertEquals(3, ((AlterTableColumnsEvent) result).getEvents().size());
    }

    @Test
    void updateColumnsExcludeSuppressesAllColumnChanges() {
        SchemaChangeEventFilter f =
                filter(Collections.emptyList(), Arrays.asList("update.columns"));
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                AlterTableModifyColumnEvent.modify(TABLE, column("c"))));
        Assertions.assertNull(result);
    }

    @Test
    void renameTableIsFilteredAsTableLevelEvent() {
        TableIdentifier renamed = TableIdentifier.of("", "db", "tbl2");
        AlterTableNameEvent rename = new AlterTableNameEvent(TABLE, renamed);

        Assertions.assertNull(
                filter(Arrays.asList("add.column"), Collections.emptyList()).filter(rename));
        Assertions.assertSame(
                rename,
                filter(Arrays.asList("rename.table"), Collections.emptyList()).filter(rename));
        Assertions.assertNull(
                filter(Collections.emptyList(), Arrays.asList("rename.table")).filter(rename));
    }

    @Test
    void unknownNameFailsFast() {
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> filter(Arrays.asList("create.table"), Collections.emptyList()));
        Assertions.assertTrue(ex.getMessage().contains("create.table"));
        Assertions.assertTrue(ex.getMessage().contains("add.column"));
    }

    @Test
    void namesAreNormalizedAndDeduplicated() {
        SchemaChangeEventFilter f =
                filter(Arrays.asList("  ADD.COLUMN  ", "add.column"), Collections.emptyList());
        SchemaChangeEvent result =
                f.filter(
                        composite(
                                AlterTableAddColumnEvent.add(TABLE, column("a")),
                                new AlterTableDropColumnEvent(TABLE, "b")));
        Assertions.assertEquals(
                Collections.singletonList(EventType.SCHEMA_CHANGE_ADD_COLUMN), subTypes(result));
    }

    @Test
    void canonicalNameMappingIsExhaustiveAndStable() {
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_ADD_COLUMN,
                SchemaChangeEventType.fromCanonicalName("add.column"));
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_DROP_COLUMN,
                SchemaChangeEventType.fromCanonicalName("drop.column"));
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_MODIFY_COLUMN,
                SchemaChangeEventType.fromCanonicalName("modify.column"));
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_CHANGE_COLUMN,
                SchemaChangeEventType.fromCanonicalName("change.column"));
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_UPDATE_COLUMNS,
                SchemaChangeEventType.fromCanonicalName("update.columns"));
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_RENAME_TABLE,
                SchemaChangeEventType.fromCanonicalName("rename.table"));
        Assertions.assertEquals(6, SchemaChangeEventType.canonicalNames().size());
    }

    /**
     * Per-type coverage: for each column-level canonical name, {@code include=[name]} must keep
     * exactly that sub-event out of a composite carrying all four column-level changes.
     */
    @Test
    void includeEachColumnLevelTypeKeepsOnlyThatType() {
        Map<String, EventType> cases = new HashMap<>();
        cases.put("add.column", EventType.SCHEMA_CHANGE_ADD_COLUMN);
        cases.put("drop.column", EventType.SCHEMA_CHANGE_DROP_COLUMN);
        cases.put("modify.column", EventType.SCHEMA_CHANGE_MODIFY_COLUMN);
        cases.put("change.column", EventType.SCHEMA_CHANGE_CHANGE_COLUMN);

        for (Map.Entry<String, EventType> c : cases.entrySet()) {
            SchemaChangeEventFilter f =
                    filter(Collections.singletonList(c.getKey()), Collections.emptyList());
            SchemaChangeEvent result =
                    f.filter(
                            composite(
                                    AlterTableAddColumnEvent.add(TABLE, column("a")),
                                    new AlterTableDropColumnEvent(TABLE, "b"),
                                    AlterTableModifyColumnEvent.modify(TABLE, column("c")),
                                    AlterTableChangeColumnEvent.change(TABLE, "old", column("d"))));
            Assertions.assertEquals(
                    Collections.singletonList(c.getValue()),
                    subTypes(result),
                    "include=[" + c.getKey() + "] should keep only " + c.getValue());
        }
    }
}
