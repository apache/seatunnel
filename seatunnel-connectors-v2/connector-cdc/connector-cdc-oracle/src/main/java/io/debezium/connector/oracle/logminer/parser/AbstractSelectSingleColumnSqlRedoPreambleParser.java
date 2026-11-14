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

import io.debezium.relational.Table;

/**
 * An abstract implementation that knows how to parse a basic Oracle SELECT statement that contains
 * a single column with one or more where conditions.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/AbstractSelectSingleColumnSqlRedoPreambleParser.java}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractSelectSingleColumnSqlRedoPreambleParser
        extends AbstractSingleColumnSqlRedoPreambleParser {

    private static final String SELECT = "select";
    private static final String INTO = "into";
    private static final String FROM = "from";
    private static final String WHERE = "where";

    private final String preamble;

    public AbstractSelectSingleColumnSqlRedoPreambleParser(String preamble) {
        this.preamble = preamble;
    }

    @Override
    protected void parseInternal(String sql, Table table) {
        int startIndex = sql.indexOf(preamble);
        if (startIndex == -1) {
            throw new IllegalStateException("Failed to locate preamble: " + preamble);
        }
        startIndex = sql.indexOf(" ", startIndex + preamble.length() + 1);
        startIndex = sql.indexOf(SELECT, startIndex);
        if (startIndex == -1) {
            throw new IllegalStateException("Failed to locate SELECT keyword");
        }
        startIndex = sql.indexOf(" ", startIndex) + 1;
        startIndex = parseSelectable(sql, startIndex, table);
        startIndex = sql.indexOf(" ", startIndex) + 1; // skip leading space
        if (startsWithAtIndex(INTO, startIndex, sql)) {
            startIndex += INTO.length() + 1;
            startIndex = parseIntoClause(sql, startIndex);
        }
        if (startsWithAtIndex(FROM, startIndex, sql)) {
            startIndex += FROM.length() + 1;
            startIndex = parseFromClause(sql, startIndex);
        }
        if (startsWithAtIndex(WHERE, startIndex, sql)) {
            startIndex += WHERE.length() + 1;
            startIndex = parseWhereClause(sql, startIndex, table);
        }

        ParserUtils.setColumnUnavailableValues(columnValues, table);
    }

    protected int parseSelectable(String sql, int index, Table table) {
        return parseQuotedValue(
                sql,
                index,
                value -> {
                    if (table.columnWithName(value) == null) {
                        throw new IllegalStateException(
                                "No column named " + value + " found in table " + table.id());
                    }
                    columnName = value;
                });
    }

    protected int parseIntoClause(String sql, int index) {
        return index;
    }

    protected int parseFromClause(String sql, int index) {
        index = parseQuotedValue(sql, index, value -> schemaName = value);
        if (sql.charAt(index) != '.') {
            throw new IllegalStateException("Expected object with format \"<schema>\".\"<table>\"");
        }

        index += 1; // skip dot
        index = parseQuotedValue(sql, index, value -> tableName = value);
        index += 1; // space

        return index;
    }
}
