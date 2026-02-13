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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    /** Represents a field with both raw name and original quoted form. */
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class FieldInfo {
        private final String rawName;
        private final String originalForm;

        public String getRawName() {
            return rawName;
        }

        public String getOriginalForm() {
            return originalForm;
        }
    }

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

        Set<FieldInfo> whereFieldInfos = extractFieldInfosFromWhere(whereCondition);
        String modifiedSql = sql;

        if (addMissingFields && !whereFieldInfos.isEmpty()) {
            modifiedSql = ensureFieldsInSelect(sql, whereFieldInfos);
        }

        return String.format(
                "SELECT * FROM (%s) tmp %s", modifiedSql, ensureWherePrefix(whereCondition));
    }

    /**
     * Extract field names referenced in a WHERE condition.
     *
     * @param whereCondition the WHERE condition
     * @return set of field names found (raw names without quotes)
     */
    public static Set<String> extractFieldNamesFromWhere(String whereCondition) {
        Set<String> fields = new HashSet<>();
        if (StringUtils.isBlank(whereCondition)) {
            return fields;
        }

        for (FieldInfo fieldInfo : extractFieldInfosFromWhere(whereCondition)) {
            fields.add(fieldInfo.getRawName());
        }
        return fields;
    }

    /**
     * Extract field infos (with original quoted form) from a WHERE condition.
     *
     * @param whereCondition the WHERE condition
     * @return set of FieldInfo objects containing both raw name and original form
     */
    public static Set<FieldInfo> extractFieldInfosFromWhere(String whereCondition) {
        Set<FieldInfo> fieldInfos = new HashSet<>();
        if (StringUtils.isBlank(whereCondition)) {
            return fieldInfos;
        }

        String condition = normalizeWhereCondition(whereCondition);
        Matcher matcher = FIELD_PATTERN.matcher(condition);

        while (matcher.find()) {
            String originalForm = matcher.group(1).trim();
            // Remove quotes to get raw name
            String rawName = originalForm.replaceAll("^[`\"\\[]|[`\"\\]]$", "");
            if (!rawName.isEmpty() && !isKeyword(rawName)) {
                fieldInfos.add(new FieldInfo(rawName, originalForm));
            }
        }

        log.debug("Extracted fields from WHERE condition '{}': {}", whereCondition, fieldInfos);
        return fieldInfos;
    }

    /**
     * Check if the SQL query contains specific fields in SELECT clause only.
     *
     * @param selectClause the SELECT clause portion of the SQL
     * @param fields the fields to check (raw names)
     * @return list of field raw names not found in the SELECT clause
     */
    public static List<String> findMissingFields(String selectClause, Set<String> fields) {
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
            Matcher matcher = pattern.matcher(selectClause);

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
     * @param requiredFieldInfos fields that must be present (with original forms)
     * @return modified SQL with fields added if necessary
     */
    private static String ensureFieldsInSelect(String sql, Set<FieldInfo> requiredFieldInfos) {
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

        // Build a map from raw name to FieldInfo for lookup
        Map<String, FieldInfo> fieldInfoMap = new HashMap<>();
        Set<String> rawNames = new HashSet<>();
        for (FieldInfo info : requiredFieldInfos) {
            fieldInfoMap.put(info.getRawName(), info);
            rawNames.add(info.getRawName());
        }

        // Check missing fields in SELECT clause only (not entire SQL)
        List<String> missingRawNames = findMissingFields(selectClause, rawNames);
        if (missingRawNames.isEmpty()) {
            return sql;
        }

        // Get original forms for missing fields
        List<String> missingFieldsWithQuotes = new ArrayList<>();
        for (String rawName : missingRawNames) {
            FieldInfo info = fieldInfoMap.get(rawName);
            if (info != null) {
                missingFieldsWithQuotes.add(info.getOriginalForm());
            } else {
                // Fallback: use raw name if FieldInfo not found (should not happen)
                missingFieldsWithQuotes.add(rawName);
            }
        }

        log.warn("Adding missing fields to SELECT clause: {}", missingFieldsWithQuotes);

        // Try to insert before FROM clause (safer for column index)
        if (fromIndex != -1) {
            StringBuilder insertion = new StringBuilder();
            for (String field : missingFieldsWithQuotes) {
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
            for (String field : missingFieldsWithQuotes) {
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
