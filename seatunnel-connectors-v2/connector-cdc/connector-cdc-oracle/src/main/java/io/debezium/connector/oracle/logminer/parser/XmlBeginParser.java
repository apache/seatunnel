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

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.relational.Table;

/**
 * A parser that interprets an Oracle LogMiner {@code XML BEGIN DOC} event type.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/XmlBeginParser.java}. Routes binary vs text redo using
 * {@link XmlParserUtils#isXmlSerializedAsBinary(String)}.
 *
 * <p>SeaTunnel changes:
 *
 * <ul>
 *   <li>Java 8: {@code record XmlBegin} is a static nested class
 *   <li>{@code [SeaTunnel]} when {@code INFO} is missing, keep the SELECT ({@code XML DOC BEGIN:})
 *       parser instead of the text {@code update} parser so 11g/21c still parse
 * </ul>
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class XmlBeginParser {

    private final XmlBeginBinaryParser binaryParser = new XmlBeginBinaryParser();
    private final XmlBeginTextParser textParser = new XmlBeginTextParser();
    private AbstractSingleColumnSqlRedoPreambleParser lastParser = binaryParser;

    /**
     * Parsed data of a {@code XML DOC BEGIN} operation.
     *
     * <p>Official 3.7.1.Final uses a {@code record}; SeaTunnel keeps a Java 8 nested class.
     */
    public static final class XmlBegin {
        private final String columnName;
        private final LogMinerDmlEntry parsedEvent;

        public XmlBegin(String columnName, LogMinerDmlEntry parsedEvent) {
            this.columnName = columnName;
            this.parsedEvent = parsedEvent;
        }

        public String getColumnName() {
            return columnName;
        }

        public LogMinerDmlEntry getParsedEvent() {
            return parsedEvent;
        }
    }

    /**
     * Parses a LogMiner {@code XML DOC BEGIN} event.
     *
     * @param event the event, should not be {@code null}
     * @param table the relational table, should not be {@code null}
     * @return the parsed begin event data, never {@code null}
     */
    public XmlBegin parse(LogMinerEventRow event, Table table) {
        lastParser = getParserForEvent(event);
        final LogMinerDmlEntry result = lastParser.parse(event.getRedoSql(), table);
        return new XmlBegin(lastParser.getColumnName(), result);
    }

    /**
     * Parses redo SQL when no {@link LogMinerEventRow} is available (unit tests).
     *
     * <p>{@code [SeaTunnel]} Official 3.7.1.Final only exposes {@link #parse(LogMinerEventRow,
     * Table)}.
     */
    public LogMinerDmlEntry parse(String sql, Table table) {
        lastParser = parserForSql(sql);
        return lastParser.parse(sql, table);
    }

    public String getColumnName() {
        return lastParser.getColumnName();
    }

    private AbstractSingleColumnSqlRedoPreambleParser getParserForEvent(LogMinerEventRow event) {
        if (XmlParserUtils.isXmlSerializedAsBinary(event.getInfo())) {
            return binaryParser;
        }
        // [SeaTunnel] INFO is often null on 11g/21c. Official 3.7.1.Final would NPE and then use
        // the text parser; the SELECT "XML DOC BEGIN:" form must still use the binary parser.
        return parserForSql(event.getRedoSql());
    }

    private AbstractSingleColumnSqlRedoPreambleParser parserForSql(String sql) {
        if (sql != null && sql.contains("XML DOC BEGIN:")) {
            return binaryParser;
        }
        return textParser;
    }

    /** A parser for {@code XML DOC BEGIN} operations that use binary-based storage. */
    private static class XmlBeginBinaryParser
            extends AbstractSelectSingleColumnSqlRedoPreambleParser {
        private static final String PREAMBLE = "XML DOC BEGIN:";

        XmlBeginBinaryParser() {
            super(PREAMBLE);
        }

        @Override
        protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
            return LogMinerDmlEntryImpl.forXml(columnValues);
        }
    }

    /** A parser for {@code XML DOC BEGIN} operations that use text-based storage. */
    private static class XmlBeginTextParser extends AbstractSingleColumnSqlRedoPreambleParser {
        private static final String UPDATE = "update ";
        private static final String ALIAS_CLAUSE = " a set a.";
        private static final String WHERE_CLAUSE = " where ";

        @Override
        protected void parseInternal(String sql, Table table) {
            int index = sql.indexOf(UPDATE);
            if (index == -1) {
                throw new IllegalStateException("Failed to locate preamble: " + UPDATE);
            }

            index = parseQuotedValue(sql, index, value -> schemaName = value);
            index += 1; // skip dot
            index = parseQuotedValue(sql, index, value -> tableName = value);

            index = indexOfThrow(ALIAS_CLAUSE, sql, index) + ALIAS_CLAUSE.length();
            index = parseQuotedValue(sql, index, value -> columnName = value);

            index = indexOfThrow(WHERE_CLAUSE, sql, index) + WHERE_CLAUSE.length();
            parseWhereClause(sql, index, table);

            ParserUtils.setColumnUnavailableValues(columnValues, table);
        }

        @Override
        protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
            return LogMinerDmlEntryImpl.forXml(columnValues);
        }
    }
}
