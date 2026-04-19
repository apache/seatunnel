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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.parser;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PostgresAlterTableParserTest {

    private PostgresAlterTableParser parser;

    @BeforeEach
    public void setUp() {
        parser = new PostgresAlterTableParser(TablePath.of("postgres_cdc", "inventory", "t1"));
        parser.setCurrentDatabase("postgres_cdc");
        parser.setCurrentSchema("inventory");
    }

    @Test
    public void testParseAddColumn() {
        parser.parse(
                "ALTER TABLE \"inventory\".\"t1\" ADD COLUMN add_column1 bigint, ADD COLUMN add_column2 varchar(20) NOT NULL",
                null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, events.size());
        AlterTableAddColumnEvent first = (AlterTableAddColumnEvent) events.get(0);
        Assertions.assertEquals("add_column1", first.getColumn().getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, first.getColumn().getDataType());
        AlterTableAddColumnEvent second = (AlterTableAddColumnEvent) events.get(1);
        Assertions.assertEquals("add_column2", second.getColumn().getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, second.getColumn().getDataType());
        Assertions.assertFalse(second.getColumn().isNullable());
    }

    @Test
    public void testParseAddColumnWithArrayType() {
        parser.parse(
                "ALTER TABLE inventory.t1 ADD COLUMN tags text[], ADD COLUMN flags boolean[]",
                null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, events.size());
        AlterTableAddColumnEvent first = (AlterTableAddColumnEvent) events.get(0);
        Assertions.assertEquals("tags", first.getColumn().getName());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, first.getColumn().getDataType());
        AlterTableAddColumnEvent second = (AlterTableAddColumnEvent) events.get(1);
        Assertions.assertEquals("flags", second.getColumn().getName());
        Assertions.assertEquals(ArrayType.BOOLEAN_ARRAY_TYPE, second.getColumn().getDataType());
    }

    @Test
    public void testParseDropColumn() {
        parser.parse("ALTER TABLE t1 DROP COLUMN f_small, DROP COLUMN f_bytea", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals("f_small", ((AlterTableDropColumnEvent) events.get(0)).getColumn());
        Assertions.assertEquals("f_bytea", ((AlterTableDropColumnEvent) events.get(1)).getColumn());
    }

    @Test
    public void testParseDropColumnWithCascade() {
        parser.parse("ALTER TABLE t1 DROP COLUMN f_small CASCADE", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals("f_small", ((AlterTableDropColumnEvent) events.get(0)).getColumn());
    }

    @Test
    public void testParseRenameColumn() {
        parser.parse("ALTER TABLE inventory.t1 RENAME COLUMN f_int TO f_integer", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableChangeColumnEvent event = (AlterTableChangeColumnEvent) events.get(0);
        Assertions.assertEquals("f_int", event.getOldColumn());
        Assertions.assertEquals("f_integer", event.getColumn().getName());
    }

    @Test
    public void testParseModifyColumn() {
        parser.parse("ALTER TABLE inventory.t1 ALTER COLUMN f_added TYPE bigint", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableModifyColumnEvent event = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("f_added", event.getColumn().getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, event.getColumn().getDataType());
    }

    @Test
    public void testParseModifyColumnWithArrayType() {
        parser.parse("ALTER TABLE inventory.t1 ALTER COLUMN f_added TYPE integer[]", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableModifyColumnEvent event = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("f_added", event.getColumn().getName());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, event.getColumn().getDataType());
    }

    @Test
    public void testParseModifyColumnWithCharacterVaryingArrayType() {
        parser.parse(
                "ALTER TABLE inventory.t1 ALTER COLUMN f_added TYPE character varying[]", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableModifyColumnEvent event = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("f_added", event.getColumn().getName());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, event.getColumn().getDataType());
    }

    @Test
    public void testParseModifyColumnWithTimeZoneType() {
        parser.parse(
                "ALTER TABLE inventory.t1 ALTER COLUMN f_added TYPE timestamp with time zone",
                null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableModifyColumnEvent event = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("f_added", event.getColumn().getName());
        Assertions.assertEquals(BasicType.OFFSET_DATE_TIME_TYPE, event.getColumn().getDataType());
    }

    @Test
    public void testParseModifyColumnWithTimeZoneTypeAndPrecision() {
        parser.parse(
                "ALTER TABLE inventory.t1 ALTER COLUMN f_added TYPE timestamp(6) with time zone",
                null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        AlterTableModifyColumnEvent event = (AlterTableModifyColumnEvent) events.get(0);
        Assertions.assertEquals("f_added", event.getColumn().getName());
        Assertions.assertEquals(BasicType.OFFSET_DATE_TIME_TYPE, event.getColumn().getDataType());
    }

    @Test
    public void testParseAddColumnWithQuotedTableNameContainingKeyword() {
        parser.parse(
                "ALTER TABLE \"inventory\".\"audit_drop_log\" ADD COLUMN f_added bigint", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        SchemaChangeEvent event = events.get(0);
        Assertions.assertEquals("audit_drop_log", event.tablePath().getTableName());
        Assertions.assertEquals(
                "f_added", ((AlterTableAddColumnEvent) event).getColumn().getName());
    }

    @Test
    public void testParseAddColumnWithUnquotedTableNameContainingKeyword() {
        parser.parse("ALTER TABLE inventory.audit_drop_log ADD COLUMN f_added bigint", null);

        List<AlterTableColumnEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        SchemaChangeEvent event = events.get(0);
        Assertions.assertEquals("audit_drop_log", event.tablePath().getTableName());
        Assertions.assertEquals(
                "f_added", ((AlterTableAddColumnEvent) event).getColumn().getName());
    }
}
