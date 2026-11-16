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

import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.relational.Table;
import io.debezium.text.ParsingException;
import io.debezium.util.Strings;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A common abstract base class for {@link SingleColumnSqlRedoPreambleParser} implementations.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/AbstractSingleColumnSqlRedoPreambleParser.java}.
 *
 * <p>SeaTunnel changes: Java 8 only (no language-level differences in this type).
 *
 * @author Chris Cranford
 */
public abstract class AbstractSingleColumnSqlRedoPreambleParser
        implements SingleColumnSqlRedoPreambleParser {

    protected static final String DECLARE = "DECLARE";
    protected static final String BEGIN = "BEGIN";
    protected static final String WHERE = "where ";
    protected static final String AND = "and";
    protected static final String OR = "or";
    protected static final String FOR_UPDATE = "for update";

    protected String schemaName;
    protected String tableName;
    protected String columnName;
    protected Object[] columnValues;

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    protected void reset() {
        schemaName = null;
        tableName = null;
        columnName = null;
        columnValues = null;
    }

    @Override
    public LogMinerDmlEntry parse(String sql, Table table) throws ParsingException {
        Objects.requireNonNull(table, "The relational table model should not be null");

        reset();

        this.columnValues = new Object[table.columns().size()];

        if (!Strings.isNullOrBlank(sql)) {
            try {
                parseInternal(sql, table);
            } catch (Throwable t) {
                throw new ParsingException(null, "Parsing failed for SQL: '" + sql + "'", t);
            }
        }

        final LogMinerDmlEntry entry = createDmlEntryForColumnValues(columnValues);
        entry.setObjectOwner(schemaName);
        entry.setObjectName(tableName);
        return entry;
    }

    /**
     * An internal parse handler that handles the unique logic per parser implementation.
     *
     * @param sql the SQL to be parsed, should not be {@code null}
     * @param table the relational table model, should not be {@code null}
     */
    protected abstract void parseInternal(String sql, Table table);

    /**
     * Parses a quoted SQL value.
     *
     * @param sql the SQL to be parsed
     * @param index the position in the SQL where the parse starts
     * @param collector the consumer functor to receive the parsed value
     * @return the index where the quoted value token ends
     */
    protected static int parseQuotedValue(String sql, int index, Consumer<String> collector) {
        boolean inDoubleQuotes = false;
        int startIndex = -1;
        int lastIndex = -1;

        for (int i = index; i < sql.length(); i++) {
            if (sql.charAt(i) == '"') {
                if (!inDoubleQuotes) {
                    inDoubleQuotes = true;
                    startIndex = i + 1;
                } else {
                    lastIndex = i;
                    index = i + 1;
                    break;
                }
            }
        }

        if (startIndex != -1 && lastIndex != -1) {
            collector.accept(sql.substring(startIndex, lastIndex));
        }

        return index;
    }

    /**
     * Parses a SQL operator at the specified offset.
     *
     * @param sql the SQL to be parsed
     * @param index the position in the SQL where the operator parse starts
     * @return the index where the operator ends
     */
    protected int parseOperator(String sql, int index) {
        boolean initialSpace = false;
        for (int i = index; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            char lookAhead = (i + 1 < sql.length()) ? sql.charAt(i + 1) : 0;
            if (!initialSpace && c == ' ') {
                initialSpace = true;
            } else if (initialSpace && c == '=' && lookAhead == ' ') {
                index += 3;
                break;
            } else if (initialSpace && c == 'I' && lookAhead == 'S') {
                char lookAhead2 = (i + 2 < sql.length()) ? sql.charAt(i + 2) : 0;
                if (lookAhead2 == ' ') {
                    index += 4;
                    break;
                }
                throw new ParsingException(null, "Expected 'IS' at index " + i + ": " + sql);
            } else {
                throw new ParsingException(
                        null, "Failed to parse operator at index " + i + ": " + sql);
            }
        }
        return index;
    }

    /**
     * Parses a column value from the given SQL.
     *
     * @param sql the SQL to be parsed
     * @param index the position in the SQL where the column value parsing starts
     * @param columnIndex the column index position in the {@link #columnValues} array to populate
     * @return the index where the column value token ends
     */
    protected int parseColumnValue(String sql, int index, int columnIndex) {
        boolean inSingleQuotes = false;
        int start = -1;
        int last = -1;
        int nested = 0;
        for (int i = index; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            char lookAhead = (i + 1 < sql.length()) ? sql.charAt(i + 1) : 0;
            if (i == index && c != '\'') {
                start = i;
            } else if (c == '(' && !inSingleQuotes) {
                nested++;
            } else if (c == ')' && !inSingleQuotes) {
                nested--;
                if (nested == 0) {
                    last = i + 1;
                    index = i + 1;
                    break;
                }
            } else if (c == '\'') {
                if (inSingleQuotes && lookAhead == '\'') {
                    i += 1;
                    continue;
                }
                if (inSingleQuotes) {
                    inSingleQuotes = false;
                    if (nested == 0) {
                        last = i;
                        index = i + 1;
                        break;
                    }
                    continue;
                }
                inSingleQuotes = true;
                if (nested == 0) {
                    start = i + 1;
                }
            } else if (c == ' ' && !inSingleQuotes && nested == 0) {
                last = i;
                index = i;
                break;
            }
        }

        if (start != -1) {
            final String value;
            if (last != -1) {
                value = sql.substring(start, last);
            } else {
                value = sql.substring(start);
                index = sql.length();
            }
            if (!value.equalsIgnoreCase("null")) {
                columnValues[columnIndex] = value;
            }
        }

        return index;
    }

    /**
     * Parses the SQL where-clause.
     *
     * @param sql the SQL to be parsed
     * @param index the position in the SQL where the parsing starts
     * @param table the expected relational table being parsed
     * @return the index where the where-clause parsing ends
     */
    protected int parseWhereClause(String sql, int index, Table table) {
        for (int i = index; i < sql.length(); ++i) {
            StringBuilder parsedColumnName = new StringBuilder();
            index = parseQuotedValue(sql, index, parsedColumnName::append);
            index = parseOperator(sql, index);
            index =
                    parseColumnValue(
                            sql,
                            index,
                            LogMinerHelper.getColumnIndexByName(
                                    parsedColumnName.toString(), table));
            index += 1; // space

            if (sql.indexOf(AND, index) == index) {
                index += AND.length() + 1;
            } else if (sql.indexOf(OR, index) == index) {
                index += OR.length() + 1;
            } else if (sql.indexOf(FOR_UPDATE, index) == index) {
                break;
            } else if (index >= sql.length()) {
                break;
            }
        }
        return index;
    }

    /**
     * Creates the {@link LogMinerDmlEntry} for the column values.
     *
     * @param columnValues the column values array
     * @return the DML entry that represents the parsed statement
     */
    protected abstract LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues);

    /**
     * Checks that the specified {@code value} starts with the given value {@code startsWithValue}
     * at the given index.
     *
     * @param startsWithValue the value to match against
     * @param index the position to start the matching
     * @param value the value to search within for the match
     * @return true if the match was found, false otherwise
     */
    protected static boolean startsWithAtIndex(String startsWithValue, int index, String value) {
        for (int i = 0; i < startsWithValue.length(); ++i) {
            if (value.charAt(index++) != startsWithValue.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Performs an {@link String#indexOf(String, int)} on the given {@code value}, throwing an
     * exception if not found.
     *
     * @param findValue the text to find within the string value
     * @param value the string value to operate on
     * @param fromIndex the index where the find starts
     * @return the index of the found string
     */
    protected static int indexOfThrow(String findValue, String value, int fromIndex) {
        final int result = value.indexOf(findValue, fromIndex);
        if (result == -1) {
            throw new IllegalStateException(
                    "Expected to find \"" + findValue + "\": " + value.substring(fromIndex));
        }
        return result;
    }
}
