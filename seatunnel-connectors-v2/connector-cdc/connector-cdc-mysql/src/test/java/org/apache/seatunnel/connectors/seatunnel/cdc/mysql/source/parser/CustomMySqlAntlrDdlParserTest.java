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
     * list on the column, but {@code ColumnType} is emitted verbatim into generated auto-create
     * DDL, where a bare {@code SET} is a syntax error. The option list has to survive as part of
     * the rebuilt type expression.
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
                        + "ADD COLUMN c_enum ENUM('x','y') NULL",
                new Tables());

        List<AlterTableColumnEvent> events = parser.getAndClearParsedColumnEvents();
        Assertions.assertEquals(2, events.size());

        Column setColumn = ((AlterTableAddColumnEvent) events.get(0)).getColumn();
        Assertions.assertEquals("c_set", setColumn.getName());
        Assertions.assertEquals("SET('a','b','c')", setColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, setColumn.getDataType());

        Column enumColumn = ((AlterTableAddColumnEvent) events.get(1)).getColumn();
        Assertions.assertEquals("c_enum", enumColumn.getName());
        Assertions.assertEquals("ENUM('x','y')", enumColumn.getSourceType());
        Assertions.assertEquals(BasicType.STRING_TYPE, enumColumn.getDataType());
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
