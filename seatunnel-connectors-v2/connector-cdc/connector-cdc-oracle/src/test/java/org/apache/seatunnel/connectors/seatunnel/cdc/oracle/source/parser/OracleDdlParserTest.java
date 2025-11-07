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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.parser;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Tables;

import java.util.List;

public class OracleDdlParserTest {
    private static final String PDB_NAME = "qyws_empi";
    private static final String SCHEMA_NAME = "QYWS_EMPI";
    private static final String TABLE_NAME = "STUDENTS";
    private static CustomOracleAntlrDdlParser parser;

    @BeforeAll
    public static void setUp() {
        parser = new CustomOracleAntlrDdlParser(TablePath.of(PDB_NAME, SCHEMA_NAME, TABLE_NAME));
        parser.setCurrentDatabase(PDB_NAME);
        parser.setCurrentSchema(SCHEMA_NAME);
    }

    @Test
    public void testParseDDLForAddColumn() {
        String ddl =
                "alter table \""
                        + SCHEMA_NAME
                        + "\".\""
                        + TABLE_NAME
                        + "\" add ("
                        + "\"col21\" varchar2(20), col22 number(19));";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> addEvent1 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, addEvent1.size());
        testColumn(addEvent1.get(0), "col21", "varchar2(20)", "STRING", 20 * 4L, null, true, null);
        testColumn(
                addEvent1.get(1),
                "col22".toUpperCase(),
                "number(19, 0)",
                "Decimal(19, 0)",
                19L,
                null,
                true,
                null);

        ddl = "alter table " + TABLE_NAME + " add (col23 varchar2(20) not null);";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> addEvent2 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, addEvent2.size());
        testColumn(
                addEvent2.get(0),
                "col23".toUpperCase(),
                "varchar2(20)",
                "STRING",
                20 * 4L,
                null,
                false,
                null);

        ddl =
                "alter table "
                        + TABLE_NAME
                        + " add ("
                        + "col1 numeric(4,2),\n"
                        + "col2 varchar2(255) default 'debezium' not null ,\n"
                        + "col3 varchar2(255) default sys_context('userenv','host') not null ,\n"
                        + "col4 nvarchar2(255) not null,\n"
                        + "col5 char(4),\n"
                        + "col6 nchar(4),\n"
                        + "col7 float default '3.0' not null,\n"
                        + "col8 date,\n"
                        + "col9 timestamp(6) default sysdate,\n"
                        + "col10 blob,\n"
                        + "col11 clob,\n"
                        + "col12 number(1,0),\n"
                        + "col13 timestamp with time zone not null,\n"
                        + "col14 number default (sysdate-to_date('1970-01-01 08:00:00', 'yyyy-mm-dd hh24:mi:ss'))*86400000,\n"
                        + "col15 timestamp(9) default to_timestamp('20190101 00:00:00.000000','yyyymmdd hh24:mi:ss.ff6') not null,\n"
                        + "col16 date default sysdate not null);";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> addEvent3 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(16, addEvent3.size());
        // Special default values are handled for reference:
        // io.debezium.connector.oracle.OracleDefaultValueConverter.castTemporalFunctionCall
        testColumn(
                addEvent3.get(0),
                "col1".toUpperCase(),
                "number(4, 2)",
                "Decimal(4, 2)",
                4L,
                2,
                true,
                null);
        testColumn(
                addEvent3.get(1),
                "col2".toUpperCase(),
                "varchar2(255)",
                "STRING",
                255 * 4L,
                null,
                false,
                "'debezium'");
        testColumn(
                addEvent3.get(2),
                "col3".toUpperCase(),
                "varchar2(255)",
                "STRING",
                255 * 4L,
                null,
                false,
                "sys_context('userenv','host')");
        testColumn(
                addEvent3.get(3),
                "col4".toUpperCase(),
                "nvarchar2(255)",
                "STRING",
                255 * 2L,
                null,
                false,
                null);
        testColumn(
                addEvent3.get(4),
                "col5".toUpperCase(),
                "char(4)",
                "STRING",
                4 * 4L,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(5),
                "col6".toUpperCase(),
                "nchar(4)",
                "STRING",
                4 * 2L,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(6),
                "col7".toUpperCase(),
                "float",
                "Decimal(38, 18)",
                38L,
                18,
                false,
                "'3.0'");
        testColumn(
                addEvent3.get(7),
                "col8".toUpperCase(),
                "date",
                "TIMESTAMP",
                null,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(8),
                "col9".toUpperCase(),
                "timestamp(6)",
                "TIMESTAMP",
                null,
                6,
                true,
                "sysdate");
        testColumn(
                addEvent3.get(9),
                "col10".toUpperCase(),
                "blob",
                "BYTES",
                OracleTypeConverter.BYTES_4GB - 1,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(10),
                "col11".toUpperCase(),
                "clob",
                "STRING",
                OracleTypeConverter.BYTES_4GB - 1,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(11),
                "col12".toUpperCase(),
                "number(1, 0)",
                "Decimal(1, 0)",
                1L,
                null,
                true,
                null);
        testColumn(
                addEvent3.get(12),
                "col13".toUpperCase(),
                "timestamp with time zone(6)",
                "TIMESTAMP",
                null,
                6,
                false,
                null);
        testColumn(
                addEvent3.get(13),
                "col14".toUpperCase(),
                "number",
                "Decimal(38, 0)",
                38L,
                null,
                true,
                "(sysdate-to_date('1970-01-01 08:00:00','yyyy-mm-dd hh24:mi:ss'))*86400000");
        testColumn(
                addEvent3.get(14),
                "col15".toUpperCase(),
                "timestamp(9)",
                "TIMESTAMP",
                null,
                9,
                false,
                "to_timestamp('20190101 00:00:00.000000','yyyymmdd hh24:mi:ss.ff6')");
        testColumn(
                addEvent3.get(15),
                "col16".toUpperCase(),
                "date",
                "TIMESTAMP",
                null,
                null,
                false,
                "sysdate");

        ddl =
                "ALTER TABLE \""
                        + SCHEMA_NAME
                        + "\".\""
                        + TABLE_NAME
                        + "\" ADD \"ADD_COL2\" TIMESTAMP(6) DEFAULT current_timestamp(6) NOT NULL ";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> addEvent4 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, addEvent4.size());
        testColumn(
                addEvent4.get(0),
                "ADD_COL2",
                "timestamp(6)",
                "TIMESTAMP",
                null,
                6,
                false,
                "current_timestamp(6)");
    }

    @Test
    public void testParseDDLForDropColumn() {
        String ddl = "ALTER TABLE \"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" DROP (T_VARCHAR2)";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> dropEvent1 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, dropEvent1.size());
        Assertions.assertEquals(
                "T_VARCHAR2", ((AlterTableDropColumnEvent) dropEvent1.get(0)).getColumn());

        ddl = "alter table " + TABLE_NAME + " drop (col22, col23);";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> dropEvent2 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, dropEvent2.size());
        Assertions.assertEquals(
                "col22".toUpperCase(), ((AlterTableDropColumnEvent) dropEvent2.get(0)).getColumn());
        Assertions.assertEquals(
                "col23".toUpperCase(), ((AlterTableDropColumnEvent) dropEvent2.get(1)).getColumn());

        ddl = "alter table " + TABLE_NAME + " drop (\"col22\");";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> dropEvent3 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, dropEvent3.size());
        Assertions.assertEquals(
                "col22", ((AlterTableDropColumnEvent) dropEvent3.get(0)).getColumn());
    }

    @Test
    public void testParseDDLForRenameColumn() {
        String ddl = "alter table " + TABLE_NAME + " rename column STUDENT_NAME to STUDENT_NAME1";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> renameEvent1 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, renameEvent1.size());
        Assertions.assertEquals(
                "STUDENT_NAME", ((AlterTableChangeColumnEvent) renameEvent1.get(0)).getOldColumn());
        Assertions.assertEquals(
                "STUDENT_NAME1",
                ((AlterTableChangeColumnEvent) renameEvent1.get(0)).getColumn().getName());

        ddl =
                "alter table \""
                        + TABLE_NAME
                        + "\" rename column STUDENT_ID to STUDENT_ID1;\n"
                        + "alter table \""
                        + TABLE_NAME
                        + "\" rename column CLASS_ID to CLASS_ID1\n";

        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> renameEvent2 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(2, renameEvent2.size());
        Assertions.assertEquals(
                "STUDENT_ID", ((AlterTableChangeColumnEvent) renameEvent2.get(0)).getOldColumn());
        Assertions.assertEquals(
                "STUDENT_ID1",
                ((AlterTableChangeColumnEvent) renameEvent2.get(0)).getColumn().getName());
        Assertions.assertEquals(
                "CLASS_ID", ((AlterTableChangeColumnEvent) renameEvent2.get(1)).getOldColumn());
        Assertions.assertEquals(
                "CLASS_ID1",
                ((AlterTableChangeColumnEvent) renameEvent2.get(1)).getColumn().getName());
    }

    @Test
    public void testParseDDLForModifyColumn() {
        String ddl = "ALTER TABLE " + TABLE_NAME + " MODIFY COL1 varchar2(50) not null;";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> modifyEvent1 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, modifyEvent1.size());
        testColumn(
                modifyEvent1.get(0), "COL1", "varchar2(50)", "STRING", 50 * 4L, null, false, null);

        ddl = "alter table " + TABLE_NAME + " modify sex char(2) default 'M' not null ;";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> modifyEvent2 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, modifyEvent2.size());
        testColumn(
                modifyEvent2.get(0),
                "sex".toUpperCase(),
                "char(2)",
                "STRING",
                2 * 4L,
                null,
                false,
                "'M'");
        ddl =
                "ALTER TABLE \""
                        + SCHEMA_NAME
                        + "\".\""
                        + TABLE_NAME
                        + "\" MODIFY (ID NUMBER(*,0) NULL);";
        parser.parse(ddl, new Tables());
        List<AlterTableColumnEvent> modifyEvent3 = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, modifyEvent3.size());
        testColumn(
                modifyEvent3.get(0),
                "ID",
                "number(38, 0)",
                "Decimal(38, 0)",
                38L,
                null,
                true,
                null);
    }

    private void testColumn(
            AlterTableColumnEvent alterTableColumnEvent,
            String columnName,
            String sourceType,
            String dataType,
            Long columnLength,
            Integer scale,
            boolean isNullable,
            Object defaultValue) {
        Column column;
        switch (alterTableColumnEvent.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                column = ((AlterTableAddColumnEvent) alterTableColumnEvent).getColumn();
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                column = ((AlterTableModifyColumnEvent) alterTableColumnEvent).getColumn();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported method named getColumn() for the AlterTableColumnEvent: "
                                + alterTableColumnEvent.getEventType().name());
        }
        Assertions.assertEquals(columnName, column.getName());
        Assertions.assertEquals(sourceType.toUpperCase(), column.getSourceType());
        Assertions.assertEquals(dataType, column.getDataType().toString());
        Assertions.assertEquals(columnLength, column.getColumnLength());
        Assertions.assertEquals(scale, column.getScale());
        Assertions.assertEquals(isNullable, column.isNullable());
        Assertions.assertEquals(defaultValue, column.getDefaultValue());
    }
}
