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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.ddl.DdlParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AbstractSchemaChangeResolverTest {

    @Test
    void testCompletionEvent() {
        AbstractSchemaChangeResolver resolver = createResolver();

        AlterTableChangeColumnEvent changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        "old_column",
                        PhysicalColumn.builder().name("new_column").build());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("old_column")
                                                .dataType(BasicType.STRING_TYPE)
                                                .columnLength(1L)
                                                .comment("column comment")
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);

        List<AlterTableColumnEvent> events =
                resolver.completionEvent(
                        Arrays.asList(changeColumnEvent), Arrays.asList(catalogTable));
        changeColumnEvent = (AlterTableChangeColumnEvent) events.get(0);
        Assertions.assertEquals("mysql", changeColumnEvent.getSourceDialectName());
        Assertions.assertEquals(BasicType.STRING_TYPE, changeColumnEvent.getColumn().getDataType());
        Assertions.assertEquals(1L, changeColumnEvent.getColumn().getColumnLength());
        Assertions.assertEquals("column comment", changeColumnEvent.getColumn().getComment());
    }

    @Test
    void testCompletionEventFillsTableComment() {
        AbstractSchemaChangeResolver resolver = createResolver();
        TableIdentifier resolvedTableIdentifier =
                TableIdentifier.of("mysql", "test_db", "test_table");
        AlterTableCommentEvent tableCommentEvent =
                AlterTableCommentEvent.of(
                        TableIdentifier.of(null, null, "test_table"), null, "new table comment");
        CatalogTable catalogTable =
                CatalogTable.of(
                        resolvedTableIdentifier,
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "old table comment",
                        null);

        List<AlterTableEvent> events =
                resolver.completeSchemaChangeEvents(
                        Collections.singletonList(tableCommentEvent),
                        Collections.singletonList(catalogTable),
                        TablePath.of("test_db", "test_table"));

        AlterTableCommentEvent completedEvent = (AlterTableCommentEvent) events.get(0);
        Assertions.assertEquals("mysql", completedEvent.getSourceDialectName());
        Assertions.assertEquals(resolvedTableIdentifier, completedEvent.getTableIdentifier());
        Assertions.assertEquals("old table comment", completedEvent.getOldComment());
        Assertions.assertEquals("new table comment", completedEvent.getNewComment());
    }

    @Test
    void testNormalizeTableCommentIdentifierFromSourceRecordPath() {
        AbstractSchemaChangeResolver resolver = createResolver();
        AlterTableCommentEvent parsedEvent =
                AlterTableCommentEvent.of(
                        TableIdentifier.of(null, null, "products"), null, "new comment");

        AlterTableCommentEvent normalizedEvent =
                (AlterTableCommentEvent)
                        resolver.normalizeTableIdentifiers(
                                        Collections.singletonList(parsedEvent),
                                        TablePath.of("shop", "products"))
                                .get(0);

        Assertions.assertEquals(TablePath.of("shop", "products"), normalizedEvent.tablePath());
        Assertions.assertEquals("new comment", normalizedEvent.getNewComment());
    }

    @Test
    void testCompletionEventConvertsModifyColumnToColumnCommentEvent() {
        AbstractSchemaChangeResolver resolver = createResolver();
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        PhysicalColumn.builder()
                                .name("description")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(512L)
                                .nullable(true)
                                .comment("new column comment")
                                .build());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("description")
                                                .dataType(BasicType.STRING_TYPE)
                                                .columnLength(512L)
                                                .nullable(true)
                                                .comment("old column comment")
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);

        List<AlterTableEvent> events =
                resolver.completeSchemaChangeEvents(
                        Collections.singletonList(modifyColumnEvent),
                        Collections.singletonList(catalogTable),
                        TablePath.of("test_db", "test_table"));

        AlterColumnCommentEvent completedEvent = (AlterColumnCommentEvent) events.get(0);
        Assertions.assertEquals("mysql", completedEvent.getSourceDialectName());
        Assertions.assertEquals("description", completedEvent.getColumn());
        Assertions.assertEquals("old column comment", completedEvent.getOldComment());
        Assertions.assertEquals("new column comment", completedEvent.getNewComment());
    }

    @Test
    void testCompletionEventKeepsStructuralModifyColumnEvent() {
        AbstractSchemaChangeResolver resolver = createResolver();
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        PhysicalColumn.builder()
                                .name("description")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(1024L)
                                .nullable(true)
                                .comment("new column comment")
                                .build());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("description")
                                                .dataType(BasicType.STRING_TYPE)
                                                .columnLength(512L)
                                                .nullable(true)
                                                .comment("old column comment")
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);

        List<AlterTableEvent> events =
                resolver.completeSchemaChangeEvents(
                        Collections.singletonList(modifyColumnEvent),
                        Collections.singletonList(catalogTable),
                        TablePath.of("test_db", "test_table"));

        Assertions.assertSame(modifyColumnEvent, events.get(0));
    }

    @Test
    void testCompletionEventUsesCanonicalSourceTypeForColumnComment() {
        AbstractSchemaChangeResolver resolver = createResolver();
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        PhysicalColumn.builder()
                                .name("description")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(512L)
                                .sourceType("VARCHAR(512)")
                                .nullable(true)
                                .comment("new column comment")
                                .build());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("description")
                                                .dataType(BasicType.STRING_TYPE)
                                                .columnLength(2048L)
                                                .sourceType("VARCHAR(512)")
                                                .nullable(true)
                                                .comment("old column comment")
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);

        List<AlterTableEvent> events =
                resolver.completeSchemaChangeEvents(
                        Collections.singletonList(modifyColumnEvent),
                        Collections.singletonList(catalogTable),
                        TablePath.of("test_db", "test_table"));

        Assertions.assertInstanceOf(AlterColumnCommentEvent.class, events.get(0));
    }

    @Test
    void testCompletionEventKeepsSourceTypeLengthChangeStructural() {
        AbstractSchemaChangeResolver resolver = createResolver();
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        PhysicalColumn.builder()
                                .name("description")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(1024L)
                                .sourceType("VARCHAR(1024)")
                                .nullable(true)
                                .comment("new column comment")
                                .build());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(null, "test_db", "test_table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("description")
                                                .dataType(BasicType.STRING_TYPE)
                                                .columnLength(2048L)
                                                .sourceType("VARCHAR(512)")
                                                .nullable(true)
                                                .comment("old column comment")
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);

        List<AlterTableEvent> events =
                resolver.completeSchemaChangeEvents(
                        Collections.singletonList(modifyColumnEvent),
                        Collections.singletonList(catalogTable),
                        TablePath.of("test_db", "test_table"));

        Assertions.assertSame(modifyColumnEvent, events.get(0));
    }

    private AbstractSchemaChangeResolver createResolver() {
        return new AbstractSchemaChangeResolver(null) {
            @Override
            protected DdlParser createDdlParser(TablePath tablePath) {
                return null;
            }

            @Override
            protected List<AlterTableColumnEvent> getAndClearParsedEvents() {
                return Collections.emptyList();
            }

            @Override
            protected String getSourceDialectName() {
                return "mysql";
            }
        };
    }
}
