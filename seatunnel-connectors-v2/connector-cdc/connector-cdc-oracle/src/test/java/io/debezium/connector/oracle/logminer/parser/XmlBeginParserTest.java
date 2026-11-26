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

package io.debezium.connector.oracle.logminer.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import oracle.jdbc.OracleTypes;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XmlBeginParserTest {

    private final XmlBeginParser parser = new XmlBeginParser();

    @Test
    public void parsesSimpleXmlBeginRedoAndMarksTheXmlColumnUnavailable() {
        Table table =
                Table.editor()
                        .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                        .addColumn(Column.editor().name("ID").create())
                        .addColumn(
                                Column.editor()
                                        .name("DATA")
                                        .jdbcType(OracleTypes.SQLXML)
                                        .type("XMLTYPE")
                                        .create())
                        .create();

        String redoSql =
                "XML DOC BEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'";
        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        Assertions.assertEquals("DATA", parser.getColumnName());
        Assertions.assertEquals("DEBEZIUM", entry.getObjectOwner());
        Assertions.assertEquals("XML_TEST", entry.getObjectName());
        Assertions.assertEquals(EventType.XML_BEGIN, entry.getEventType());
        Assertions.assertEquals("1", entry.getNewValues()[0]);
        Assertions.assertEquals(OracleValueConverters.UNAVAILABLE_VALUE, entry.getNewValues()[1]);
        Assertions.assertTrue(OracleDatabaseSchema.isXmlColumn(table.columnWithName("DATA")));
    }

    @Test
    public void parsesXmlBeginWhenAPredicateIsNull() {
        Table table =
                Table.editor()
                        .tableId(TableId.parse("SCHEMA.TABLE"))
                        .addColumn(Column.editor().name("PROPERTIES").create())
                        .addColumn(Column.editor().name("COLUMN_A").create())
                        .addColumn(Column.editor().name("COLUMN_B").create())
                        .addColumn(Column.editor().name("COLUMN_D").create())
                        .addColumn(Column.editor().name("TIME_A").create())
                        .addColumn(Column.editor().name("TIME_B").create())
                        .addColumn(Column.editor().name("MODIFICATIONTIME").create())
                        .create();

        String redoSql =
                "XML DOC BEGIN:  select \"PROPERTIES\" from \"SCHEMA\".\"TABLE\" where \"COLUMN_A\" = '314107'"
                        + " and \"COLUMN_B\" = '69265' and \"COLUMN_D\" = '74'"
                        + " and \"TIME_A\" = TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')"
                        + " and \"TIME_B\" = TO_TIMESTAMP_TZ('3000-01-01 00:00:00.000000 +00:00')"
                        + " and \"MODIFICATIONTIME\" IS NULL";
        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        Assertions.assertEquals("PROPERTIES", parser.getColumnName());
        Assertions.assertEquals("SCHEMA", entry.getObjectOwner());
        Assertions.assertEquals("TABLE", entry.getObjectName());
        Assertions.assertEquals("314107", entry.getNewValues()[1]);
        Assertions.assertNull(entry.getNewValues()[6]);
    }

    @Test
    public void parsesOfficialTextXmlBeginUpdateRedo() {
        Table table =
                Table.editor()
                        .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                        .addColumn(Column.editor().name("ID").create())
                        .addColumn(
                                Column.editor()
                                        .name("DATA")
                                        .jdbcType(OracleTypes.SQLXML)
                                        .type("XMLTYPE")
                                        .create())
                        .create();

        // Debezium 3.7.1.Final XmlBeginTextParser: INFO is not "not re-executable".
        String redoSql =
                "update \"DEBEZIUM\".\"XML_TEST\" a set a.\"DATA\" = XMLTYPE('<a/>') where \"ID\" = '7'";
        LogMinerDmlEntry entry = parser.parse(redoSql, table);

        Assertions.assertEquals("DATA", parser.getColumnName());
        Assertions.assertEquals("DEBEZIUM", entry.getObjectOwner());
        Assertions.assertEquals("XML_TEST", entry.getObjectName());
        Assertions.assertEquals(EventType.XML_BEGIN, entry.getEventType());
        Assertions.assertEquals("7", entry.getNewValues()[0]);
        Assertions.assertEquals(OracleValueConverters.UNAVAILABLE_VALUE, entry.getNewValues()[1]);
    }

    @Test
    public void rejectsRedoWithoutTheXmlBeginPreamble() {
        Table table =
                Table.editor()
                        .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                        .addColumn(Column.editor().name("ID").create())
                        .addColumn(Column.editor().name("DATA").create())
                        .create();

        Assertions.assertThrows(
                ParsingException.class,
                () ->
                        parser.parse(
                                "XMLDOCBEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '1'",
                                table));
    }

    @Test
    public void mapsXmlLogMinerOperationCodes() {
        Assertions.assertEquals(EventType.XML_BEGIN, EventType.from(68));
        Assertions.assertEquals(EventType.XML_WRITE, EventType.from(70));
        Assertions.assertEquals(EventType.XML_END, EventType.from(71));
        Assertions.assertEquals(68, EventType.XML_BEGIN.getValue());
        Assertions.assertEquals(EventType.LOB_WRITE, EventType.from(10));
        Assertions.assertEquals(EventType.UNSUPPORTED, EventType.from(255));
    }

    @Test
    public void elevenGMissingInfoKeepsTheSelectXmlBeginParser() {
        // 11g/21c often leave INFO null. Official 3.7.1.Final NPEs then uses the text parser,
        // which cannot read "XML DOC BEGIN:" SELECT redo.
        XmlBeginParser.XmlBegin begin =
                parser.parse(event(null, selectXmlBeginRedo("1")), xmlTable());

        Assertions.assertEquals("DATA", begin.getColumnName());
        Assertions.assertEquals("1", begin.getParsedEvent().getNewValues()[0]);
        Assertions.assertEquals(
                OracleValueConverters.UNAVAILABLE_VALUE, begin.getParsedEvent().getNewValues()[1]);
    }

    @Test
    public void twentyOneCBinaryInfoUsesTheSelectXmlBeginParser() {
        XmlBeginParser.XmlBegin begin =
                parser.parse(
                        event("XML DOC BEGIN not re-executable", selectXmlBeginRedo("2")),
                        xmlTable());

        Assertions.assertEquals("DATA", begin.getColumnName());
        Assertions.assertEquals("2", begin.getParsedEvent().getNewValues()[0]);
        Assertions.assertEquals(
                OracleValueConverters.UNAVAILABLE_VALUE, begin.getParsedEvent().getNewValues()[1]);
    }

    @Test
    public void twentyOneCTextInfoUsesTheUpdateXmlBeginParser() {
        XmlBeginParser.XmlBegin begin =
                parser.parse(
                        event(
                                "XML DOC BEGIN",
                                "update \"DEBEZIUM\".\"XML_TEST\" a set a.\"DATA\" = XMLTYPE('<a/>')"
                                        + " where \"ID\" = '7'"),
                        xmlTable());

        Assertions.assertEquals("DATA", begin.getColumnName());
        Assertions.assertEquals("7", begin.getParsedEvent().getNewValues()[0]);
        Assertions.assertEquals(
                OracleValueConverters.UNAVAILABLE_VALUE, begin.getParsedEvent().getNewValues()[1]);
    }

    @Test
    public void nonBinaryInfoStillUsesTheSelectParserWhenRedoIsXmlDocBegin() {
        XmlBeginParser.XmlBegin begin =
                parser.parse(event("XML DOC BEGIN", selectXmlBeginRedo("3")), xmlTable());

        Assertions.assertEquals("DATA", begin.getColumnName());
        Assertions.assertEquals("3", begin.getParsedEvent().getNewValues()[0]);
    }

    private static Table xmlTable() {
        return Table.editor()
                .tableId(TableId.parse("DEBEZIUM.XML_TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(
                        Column.editor()
                                .name("DATA")
                                .jdbcType(OracleTypes.SQLXML)
                                .type("XMLTYPE")
                                .create())
                .create();
    }

    private static String selectXmlBeginRedo(String id) {
        return "XML DOC BEGIN:  select \"DATA\" from \"DEBEZIUM\".\"XML_TEST\" where \"ID\" = '"
                + id
                + "'";
    }

    private static LogMinerEventRow event(String info, String redoSql) {
        LogMinerEventRow event = mock(LogMinerEventRow.class);
        when(event.getInfo()).thenReturn(info);
        when(event.getRedoSql()).thenReturn(redoSql);
        return event;
    }
}
