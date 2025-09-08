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
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.ddl.DdlParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;

public class AbstractSchemaChangeResolverTest {

    @Test
    void testCompletionEvent() {
        JdbcSourceConfig config = mock(JdbcSourceConfig.class);
        AbstractSchemaChangeResolver resolver =
                new AbstractSchemaChangeResolver(config) {
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
}
