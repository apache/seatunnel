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

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;

public class BaseParserListener extends PlSqlParserBaseListener {

    /**
     * Resolves a table or column name from the provided string.
     *
     * <p>Oracle table and column names are inherently stored in upper-case; however, if the objects
     * are created using double-quotes, the case of the object name is retained. Therefore when
     * needing to parse a table or column name, this method will adhere to those rules and will
     * always return the name in upper-case unless the provided name is double-quoted in which the
     * returned value will have the double-quotes removed and case retained.
     *
     * @param name table or column name
     * @return parsed table or column name from the supplied name argument
     */
    private static String getTableOrColumnName(String name) {
        return removeQuotes(name, true);
    }

    /**
     * Removes leading and trailing double quote characters from the provided string.
     *
     * @param text value to have double quotes removed
     * @param upperCaseIfNotQuoted control if returned string is upper-cased if not quoted
     * @return string that has had quotes removed
     */
    @SuppressWarnings("SameParameterValue")
    private static String removeQuotes(String text, boolean upperCaseIfNotQuoted) {
        if (text != null && text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return (upperCaseIfNotQuoted && text != null) ? text.toUpperCase() : text;
    }

    String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        final String columnName;
        if (ctx.id_expression() != null && !ctx.id_expression().isEmpty()) {
            columnName =
                    getTableOrColumnName(
                            ctx.id_expression(ctx.id_expression().size() - 1).getText());
        } else {
            columnName = getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return columnName;
    }

    String getColumnName(final PlSqlParser.Old_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }

    String getColumnName(final PlSqlParser.New_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }
}
