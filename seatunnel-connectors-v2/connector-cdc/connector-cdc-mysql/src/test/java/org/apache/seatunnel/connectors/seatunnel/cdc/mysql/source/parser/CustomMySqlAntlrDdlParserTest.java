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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CustomMySqlAntlrDdlParserTest {

    /**
     * Verifies that one parser instance can follow the current database across multiple same-name
     * tables. Multi-database CDC jobs reuse the resolver, so the parser must not pin later DDL
     * events to the first table it saw.
     */
    @Test
    public void testParseQualifiedTableIdUsesCurrentDatabaseForSameNameTables() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        MySqlSourceConfig sourceConfig = factory.create(0);

        CustomMySqlAntlrDdlParser parser =
                new CustomMySqlAntlrDdlParser(sourceConfig.getDbzConnectorConfig());

        parser.setCurrentDatabase("multi_schema_shop_a");
        parser.parse(
                "ALTER TABLE products ADD COLUMN add_column1 VARCHAR(64) NOT NULL DEFAULT 'db-a'",
                new Tables());
        List<AlterTableColumnEvent> firstDatabaseEvents = parser.getAndClearParsedColumnEvents();
        Assertions.assertEquals(1, firstDatabaseEvents.size());
        Assertions.assertEquals(
                "multi_schema_shop_a",
                firstDatabaseEvents.get(0).getTableIdentifier().getDatabaseName());
        Assertions.assertEquals(
                "products", firstDatabaseEvents.get(0).getTablePath().getTableName());

        parser.setCurrentDatabase("multi_schema_shop_b");
        parser.parse(
                "ALTER TABLE products ADD COLUMN add_column2 INT NOT NULL DEFAULT 1", new Tables());
        List<AlterTableColumnEvent> secondDatabaseEvents = parser.getAndClearParsedColumnEvents();
        Assertions.assertEquals(1, secondDatabaseEvents.size());
        Assertions.assertEquals(
                "multi_schema_shop_b",
                secondDatabaseEvents.get(0).getTableIdentifier().getDatabaseName());
        Assertions.assertEquals(
                "products", secondDatabaseEvents.get(0).getTablePath().getTableName());
    }

    @Test
    void testParseAlterTableCommentEvent() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser(null);

        parser.setCurrentDatabase("test_db");
        parser.parse("ALTER TABLE products COMMENT = 'Product catalog table'", new Tables());

        List<AlterTableEvent> events = parser.getAndClearParsedEvents();
        Assertions.assertEquals(1, events.size());
        Assertions.assertTrue(events.get(0) instanceof AlterTableCommentEvent);
        AlterTableCommentEvent event = (AlterTableCommentEvent) events.get(0);
        Assertions.assertEquals("Product catalog table", event.getNewComment());
    }

    /**
     * Regression test for <a href="https://github.com/apache/seatunnel/issues/12354">#12354</a>.
     *
     * <p>Debezium reports the bare type name for {@code SET} / {@code ENUM} and keeps the option
     * list on the column, but the base {@code getSourceColumnTypeWithLengthScale} renders the
     * bookkeeping length as {@code SET(5)} / {@code ENUM(1)}. That string is what {@code
     * org.apache.seatunnel.api.table.catalog.Column#getSourceType()} exposes, and {@code
     * MysqlCreateTableSqlBuilder} emits it verbatim into generated auto-create DDL, which MySQL
     * rejects. The option list therefore has to survive as part of the rebuilt type expression, and
     * the column length must not stay on the bookkeeping value either, since a sink that rebuilds
     * the type from the length would create an undersized column.
     */
    @Test
    void testParseAlterTableAddSetAndEnumColumnKeepsOptionList() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        CustomMySqlAntlrDdlParser parser =
                new CustomMySqlAntlrDdlParser(factory.create(0).getDbzConnectorConfig());
        parser.setCurrentDatabase("test_db");
        parser.parse(
                "ALTER TABLE products ADD COLUMN c_set SET('a','b','c') NULL, "
                        + "ADD COLUMN c_enum ENUM('x','y') NULL, "
                        + "ADD COLUMN c_enum_multi ENUM('active','inactive') NULL, "
                        + "ADD COLUMN c_set_single SET('only') NULL, "
                        + "ADD COLUMN c_set_escaped SET('a,b','it''s') NULL, "
                        + "ADD COLUMN c_enum_charset ENUM('x','y') CHARACTER SET utf8mb4 NULL",
                new Tables());

        List<AlterTableColumnEvent> events = parser.getAndClearParsedColumnEvents();
        Assertions.assertEquals(6, events.size());

        Column setColumn = ((AlterTableAddColumnEvent) events.get(0)).getColumn();
        Assertions.assertEquals("c_set", setColumn.getName());
        Assertions.assertEquals("SET('a','b','c')", setColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, setColumn.getDataType());
        // The longest value of this SET is "a,b,c": three members plus two separators.
        Assertions.assertEquals(5L, setColumn.getColumnLength());

        Column enumColumn = ((AlterTableAddColumnEvent) events.get(1)).getColumn();
        Assertions.assertEquals("c_enum", enumColumn.getName());
        Assertions.assertEquals("ENUM('x','y')", enumColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, enumColumn.getDataType());
        Assertions.assertEquals(1L, enumColumn.getColumnLength());

        // Multi-character members are the case where the real length and Debezium's bookkeeping
        // count (options * 2 - 1 for SET, 1 for ENUM) differ, so the length assertion below only
        // passes for the right reason with members like these.
        Column multiEnumColumn = ((AlterTableAddColumnEvent) events.get(2)).getColumn();
        Assertions.assertEquals("c_enum_multi", multiEnumColumn.getName());
        Assertions.assertEquals("ENUM('active','inactive')", multiEnumColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, multiEnumColumn.getDataType());
        Assertions.assertEquals(8L, multiEnumColumn.getColumnLength());

        // A single-option SET would also look plausible with a wrong fallback: its bookkeeping
        // length is 1, not the four characters of its only member.
        Column singleSetColumn = ((AlterTableAddColumnEvent) events.get(3)).getColumn();
        Assertions.assertEquals("c_set_single", singleSetColumn.getName());
        Assertions.assertEquals("SET('only')", singleSetColumn.getSourceType());
        Assertions.assertEquals(4L, singleSetColumn.getColumnLength());

        // The option list is re-emitted verbatim, so an embedded comma and an escaped quote have to
        // round-trip exactly; the stored length counts the unescaped members plus the separator.
        Column escapedSetColumn = ((AlterTableAddColumnEvent) events.get(4)).getColumn();
        Assertions.assertEquals("c_set_escaped", escapedSetColumn.getName());
        Assertions.assertEquals("SET('a,b','it''s')", escapedSetColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, escapedSetColumn.getDataType());
        Assertions.assertEquals(8L, escapedSetColumn.getColumnLength());

        // The rebuilt expression carries the option list only: a CHARACTER SET clause on the
        // source column is not part of it. Pinned here so the behaviour is explicit rather than
        // accidental; the sink then applies its own default charset for the added column.
        Column charsetEnumColumn = ((AlterTableAddColumnEvent) events.get(5)).getColumn();
        Assertions.assertEquals("c_enum_charset", charsetEnumColumn.getName());
        Assertions.assertEquals("ENUM('x','y')", charsetEnumColumn.getSourceType());
        Assertions.assertEquals(1L, charsetEnumColumn.getColumnLength());
    }

    /**
     * Same regression as {@link #testParseAlterTableAddSetAndEnumColumnKeepsOptionList()}, reached
     * through the other two DDL forms that share {@code toSeatunnelColumnWithFullTypeInfo}: {@code
     * MODIFY COLUMN} and {@code CHANGE COLUMN}. {@code CHANGE} additionally renames the column, so
     * the rebuilt type has to survive that too. A length-bearing type is kept in the same statement
     * to pin the fallback to the base implementation.
     */
    @Test
    void testParseAlterTableModifyAndChangeSetAndEnumColumnKeepOptionList() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        CustomMySqlAntlrDdlParser parser =
                new CustomMySqlAntlrDdlParser(factory.create(0).getDbzConnectorConfig());
        parser.setCurrentDatabase("test_db");
        parser.parse(
                "ALTER TABLE products MODIFY COLUMN c_set SET('a','b','c') NULL, "
                        + "CHANGE COLUMN c_enum_src c_enum ENUM('x','y') NULL, "
                        + "MODIFY COLUMN c_varchar VARCHAR(64) NULL",
                new Tables());

        List<AlterTableColumnEvent> events = parser.getAndClearParsedColumnEvents();
        Assertions.assertEquals(3, events.size());

        Column modifiedColumn = ((AlterTableModifyColumnEvent) events.get(0)).getColumn();
        Assertions.assertEquals("c_set", modifiedColumn.getName());
        Assertions.assertEquals("SET('a','b','c')", modifiedColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, modifiedColumn.getDataType());

        AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) events.get(1);
        Assertions.assertEquals("c_enum_src", changeColumnEvent.getOldColumn());
        Column renamedColumn = changeColumnEvent.getColumn();
        Assertions.assertEquals("c_enum", renamedColumn.getName());
        Assertions.assertEquals("ENUM('x','y')", renamedColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, renamedColumn.getDataType());

        Column varcharColumn = ((AlterTableModifyColumnEvent) events.get(2)).getColumn();
        Assertions.assertEquals("c_varchar", varcharColumn.getName());
        Assertions.assertEquals("VARCHAR(64)", varcharColumn.getSourceType());
    }

    @Test
    void testParsePropagatesDdlErrors() {
        CustomMySqlAntlrDdlParser parser = new ParserWithTreeWalkError();

        Assertions.assertThrows(
                ParsingException.class,
                () ->
                        parser.parse(
                                "ALTER TABLE products COMMENT = 'Product catalog table'",
                                new Tables()));
    }

    private static class ParserWithTreeWalkError extends CustomMySqlAntlrDdlParser {
        private ParserWithTreeWalkError() {
            super(null);
        }

        @Override
        protected AntlrDdlParserListener createParseTreeWalkerListener() {
            return new ListenerWithError();
        }
    }

    private static class ListenerWithError extends MySqlParserBaseListener
            implements AntlrDdlParserListener {
        private final Collection<ParsingException> errors =
                Collections.singletonList(new ParsingException(null, "listener failure"));

        @Override
        public Collection<ParsingException> getErrors() {
            return errors;
        }
    }
}
