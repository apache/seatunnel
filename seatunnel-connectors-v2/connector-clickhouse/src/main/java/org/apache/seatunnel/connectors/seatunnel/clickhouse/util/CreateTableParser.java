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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateTableParser {

    private static final Pattern COLUMN_PATTERN = Pattern.compile("`?(\\w+)`?\\s*([\\w|\\W]*)");

    public static Map<String, ColumnInfo> getColumnList(String createTableSql) {
        Map<String, ColumnInfo> columns = new HashMap<>();
        StringBuilder columnBuilder = new StringBuilder();
        int startIndex = createTableSql.indexOf("(");
        createTableSql = createTableSql.substring(startIndex + 1);

        boolean insideParentheses = false;
        for (int i = 0; i < createTableSql.length(); i++) {
            char c = createTableSql.charAt(i);
            if (c == '(') {
                insideParentheses = true;
                columnBuilder.append(c);
            } else if ((c == ',' || c == ')') && !insideParentheses) {
                parseColumn(columnBuilder.toString(), columns, startIndex + i + 1);
                columnBuilder.setLength(0);
                if (c == ')') {
                    break;
                }
            } else if (c == ')') {
                insideParentheses = false;
                columnBuilder.append(c);
            } else {
                columnBuilder.append(c);
            }
        }
        return columns;
    }

    private static void parseColumn(
            String columnString, Map<String, ColumnInfo> columnList, int suffixIndex) {
        Matcher matcher = COLUMN_PATTERN.matcher(columnString.trim());
        if (matcher.matches()) {
            String columnName = matcher.group(1);
            String otherInfo = matcher.group(2).trim();
            StringBuilder columnBuilder =
                    new StringBuilder(columnName).append(" ").append(otherInfo);
            if (columnBuilder.toString().toUpperCase().contains("PRIMARY KEY")
                    || columnBuilder.toString().toUpperCase().contains("CREATE TABLE")) {
                return;
            }
            int endIndex =
                    suffixIndex
                            - columnString
                                    .substring(
                                            columnString.indexOf(columnName) + columnName.length())
                                    .length();
            int startIndex =
                    suffixIndex - columnString.substring(columnString.indexOf(columnName)).length();
            columnList.put(columnName, new ColumnInfo(columnName, otherInfo, startIndex, endIndex));
        }
    }

    @Getter
    public static final class ColumnInfo {

        public ColumnInfo(String name, String info, int startIndex, int endIndex) {
            this.name = name;
            this.info = info;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        String name;
        String info;
        int startIndex;
        int endIndex;
    }
}
