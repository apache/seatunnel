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

package org.apache.seatunnel.connectors.seatunnel.common.sql;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility for merging key-value table options into SQL CREATE TABLE property clauses. */
public final class SqlTableClauseMerger {

    private static final Pattern DOUBLE_QUOTED_ENTRY_PATTERN =
            Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"\\s*=\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");

    private SqlTableClauseMerger() {}

    public static String merge(
            String sql, ClauseMergeFormat format, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return sql;
        }
        switch (format) {
            case DOUBLE_QUOTED_PROPERTIES:
                return mergeDoubleQuotedProperties(sql, format.getKeyword(), tableOptions);
            default:
                throw new IllegalArgumentException("Unsupported clause merge format: " + format);
        }
    }

    private static String mergeDoubleQuotedProperties(
            String sql, String keyword, Map<String, String> tableOptions) {
        int keywordStart = findLastKeywordPosition(sql, keyword);
        if (keywordStart >= 0) {
            int openParen = indexOfNonWhitespace(sql, keywordStart + keyword.length());
            if (openParen >= 0 && sql.charAt(openParen) == '(') {
                int closeParen = findMatchingCloseParen(sql, openParen);
                if (closeParen > openParen) {
                    String clauseBody = sql.substring(openParen + 1, closeParen);
                    Map<String, String> merged = parseDoubleQuotedEntries(clauseBody);
                    merged.putAll(tableOptions);
                    String newClause = renderDoubleQuotedProperties(keyword, merged);
                    return sql.substring(0, keywordStart)
                            + newClause
                            + sql.substring(closeParen + 1);
                }
            }
        }
        return appendDoubleQuotedProperties(sql, keyword, tableOptions);
    }

    private static String appendDoubleQuotedProperties(
            String sql, String keyword, Map<String, String> tableOptions) {
        String trimmed = sql.trim();
        boolean endsWithSemicolon = trimmed.endsWith(";");
        if (endsWithSemicolon) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        StringBuilder builder = new StringBuilder(trimmed);
        if (!trimmed.isEmpty()) {
            builder.append('\n');
        }
        builder.append(renderDoubleQuotedProperties(keyword, tableOptions));
        if (endsWithSemicolon) {
            builder.append(';');
        }
        return builder.toString();
    }

    private static String renderDoubleQuotedProperties(
            String keyword, Map<String, String> properties) {
        StringBuilder builder = new StringBuilder(keyword).append(" (\n");
        boolean first = true;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!first) {
                builder.append(",\n");
            }
            builder.append("    \"")
                    .append(escapeDoubleQuoted(entry.getKey()))
                    .append("\" = \"")
                    .append(escapeDoubleQuoted(entry.getValue()))
                    .append('\"');
            first = false;
        }
        builder.append("\n)");
        return builder.toString();
    }

    private static Map<String, String> parseDoubleQuotedEntries(String clauseBody) {
        Map<String, String> properties = new LinkedHashMap<>();
        Matcher matcher = DOUBLE_QUOTED_ENTRY_PATTERN.matcher(clauseBody);
        while (matcher.find()) {
            properties.put(
                    unescapeDoubleQuoted(matcher.group(1)), unescapeDoubleQuoted(matcher.group(2)));
        }
        return properties;
    }

    private static int findLastKeywordPosition(String sql, String keyword) {
        Pattern pattern = Pattern.compile("(?i)\\b" + Pattern.quote(keyword) + "\\b");
        Matcher matcher = pattern.matcher(sql);
        int lastPosition = -1;
        while (matcher.find()) {
            lastPosition = matcher.start();
        }
        return lastPosition;
    }

    private static int indexOfNonWhitespace(String sql, int fromIndex) {
        for (int i = fromIndex; i < sql.length(); i++) {
            if (!Character.isWhitespace(sql.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static int findMatchingCloseParen(String sql, int openParenIndex) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = openParenIndex; i < sql.length(); i++) {
            char current = sql.charAt(i);
            if (inSingleQuote) {
                if (current == '\'' && !isEscaped(sql, i)) {
                    inSingleQuote = false;
                }
                continue;
            }
            if (inDoubleQuote) {
                if (current == '"' && !isEscaped(sql, i)) {
                    inDoubleQuote = false;
                }
                continue;
            }
            if (current == '\'') {
                inSingleQuote = true;
                continue;
            }
            if (current == '"') {
                inDoubleQuote = true;
                continue;
            }
            if (current == '(') {
                depth++;
            } else if (current == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean isEscaped(String sql, int index) {
        int backslashCount = 0;
        for (int i = index - 1; i >= 0 && sql.charAt(i) == '\\'; i--) {
            backslashCount++;
        }
        return backslashCount % 2 == 1;
    }

    private static String escapeDoubleQuoted(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String unescapeDoubleQuoted(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current == '\\' && i + 1 < value.length()) {
                builder.append(value.charAt(i + 1));
                i++;
            } else {
                builder.append(current);
            }
        }
        return builder.toString();
    }
}
