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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Helper class for handling WHERE condition application to SQL queries. */
@Slf4j
public class SqlWhereConditionHelper {

    private static final Pattern FIELD_PATTERN =
            Pattern.compile(
                    "([a-zA-Z_][a-zA-Z0-9_]*|`[^`]+`|\"[^\"]+\"|\\[[^\\]]+\\])\\s*"
                            + "(?:=|!=|<>|<|>|<=|>=|\\s+IN\\s|\\s+BETWEEN\\s|\\s+LIKE\\s|\\s+IS\\s)",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern SELECT_PATTERN =
            Pattern.compile(
                    "^\\s*SELECT\\s+(DISTINCT\\s+|ALL\\s+)?",
                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    /**
     * Strategy: Wrap query and apply WHERE, ensuring all referenced fields are available.
     *
     * @param sql the original SQL query
     * @param whereCondition the WHERE condition to apply
     * @param addMissingFields whether to modify SELECT to include missing fields
     * @return SQL with WHERE condition applied
     */
    public static String applyWhereConditionWithWrap(
            String sql, String whereCondition, boolean addMissingFields) {
        if (StringUtils.isBlank(whereCondition)) {
            return sql;
        }

        Set<String> whereFields = extractFieldNamesFromWhere(whereCondition);
        String modifiedSql = sql;

        if (addMissingFields && !whereFields.isEmpty()) {
            modifiedSql = ensureFieldsInSelect(sql, whereFields);
        }

        return String.format(
                "SELECT * FROM (%s) tmp %s", modifiedSql, ensureWherePrefix(whereCondition));
    }

    /**
     * Extract field names referenced in a WHERE condition.
     *
     * @param whereCondition the WHERE condition
     * @return set of field names found
     */
    public static Set<String> extractFieldNamesFromWhere(String whereCondition) {
        Set<String> fields = new HashSet<>();
        if (StringUtils.isBlank(whereCondition)) {
            return fields;
        }

        String condition = normalizeWhereCondition(whereCondition);
        Matcher matcher = FIELD_PATTERN.matcher(condition);

        while (matcher.find()) {
            String field = matcher.group(1).trim();
            // Remove quotes if present
            field = field.replaceAll("^[`\"\\[]|[`\"\\]]$", "");
            if (!field.isEmpty() && !isKeyword(field)) {
                fields.add(field);
            }
        }

        log.debug("Extracted fields from WHERE condition '{}': {}", whereCondition, fields);
        return fields;
    }

    /**
     * Check if the SQL query contains specific fields.
     *
     * @param sql the SQL query
     * @param fields the fields to check
     * @return list of fields not found in the query
     */
    public static List<String> findMissingFields(String sql, Set<String> fields) {
        List<String> missingFields = new ArrayList<>();

        for (String field : fields) {
            String regex =
                    "(?i)(^|[^a-zA-Z0-9_])"
                            + Pattern.quote(field)
                            + "([^a-zA-Z0-9_]|$)|`"
                            + Pattern.quote(field)
                            + "`|\""
                            + Pattern.quote(field)
                            + "\"|\\["
                            + Pattern.quote(field)
                            + "\\]";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(sql);

            if (!matcher.find()) {
                missingFields.add(field);
            }
        }

        return missingFields;
    }

    /**
     * Ensure all required fields are in the SELECT clause.
     *
     * @param sql the original SQL
     * @param requiredFields fields that must be present
     * @return modified SQL with fields added if necessary
     */
    private static String ensureFieldsInSelect(String sql, Set<String> requiredFields) {
        int fromIndex = findMainFromClauseIndex(sql);
        String selectClause;
        if (fromIndex != -1) {
            selectClause = sql.substring(0, fromIndex);
        } else {
            selectClause = sql;
        }

        // Remove quotes to avoid matching * inside strings
        String cleanSelect =
                selectClause.replaceAll("`[^`]*`|'[^']*'|\"[^\"]*\"|\\[[^\\]]*\\]", "");
        if (cleanSelect.contains("*")) {
            return sql;
        }

        List<String> missingFields = findMissingFields(sql, requiredFields);
        if (missingFields.isEmpty()) {
            return sql;
        }

        log.warn("Adding missing fields to SELECT clause: {}", missingFields);

        // Try to insert before FROM clause (safer for column index)
        if (fromIndex != -1) {
            StringBuilder insertion = new StringBuilder();
            for (String field : missingFields) {
                insertion.append(", ").append(field);
            }
            // Insert before FROM
            StringBuilder newSql = new StringBuilder(sql);
            newSql.insert(fromIndex, insertion.toString() + " ");
            return newSql.toString();
        }

        // Fallback: insert after SELECT (may affect column index)
        Matcher matcher = SELECT_PATTERN.matcher(sql);
        if (matcher.find()) {
            int insertPosition = matcher.end();
            StringBuilder newSql = new StringBuilder(sql);
            for (String field : missingFields) {
                newSql.insert(insertPosition, " " + field + ",");
            }
            return newSql.toString();
        }

        return sql;
    }

    /** Find the index of the main FROM clause in the SQL query. */
    private static int findMainFromClauseIndex(String sql) {
        String upperSql = sql.toUpperCase();
        int length = sql.length();
        int parenthesisCount = 0;
        boolean inQuote = false;
        char quoteChar = 0;

        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);

            if (inQuote) {
                if (c == quoteChar) {
                    if (i == 0 || sql.charAt(i - 1) != '\\') {
                        inQuote = false;
                    }
                }
            } else {
                if (c == '\'' || c == '"' || c == '`') {
                    inQuote = true;
                    quoteChar = c;
                } else if (c == '(') {
                    parenthesisCount++;
                } else if (c == ')') {
                    parenthesisCount--;
                } else if (parenthesisCount == 0) {
                    // Check for FROM keyword
                    if ((c == 'F' || c == 'f')
                            && (i == 0
                                    || Character.isWhitespace(sql.charAt(i - 1))
                                    || sql.charAt(i - 1) == ')')) {

                        if (upperSql.startsWith("FROM", i)) {
                            int nextCharIdx = i + 4;
                            if (nextCharIdx >= length
                                    || Character.isWhitespace(sql.charAt(nextCharIdx))) {
                                return i;
                            }
                        }
                    }
                }
            }
        }
        return -1;
    }

    /** Normalize WHERE condition by removing WHERE keyword if present. */
    private static String normalizeWhereCondition(String whereCondition) {
        String condition = whereCondition.trim();
        if (condition.toLowerCase().startsWith("where")) {
            condition = condition.substring(5).trim();
        }
        return condition;
    }

    /** Ensure WHERE condition starts with WHERE keyword. */
    private static String ensureWherePrefix(String whereCondition) {
        String condition = normalizeWhereCondition(whereCondition);
        return "WHERE " + condition;
    }

    /** Check if a string is a SQL keyword that should be ignored. */
    private static boolean isKeyword(String word) {
        String upper = word.toUpperCase();
        return upper.equals("AND")
                || upper.equals("OR")
                || upper.equals("NOT")
                || upper.equals("NULL")
                || upper.equals("TRUE")
                || upper.equals("FALSE");
    }
}
