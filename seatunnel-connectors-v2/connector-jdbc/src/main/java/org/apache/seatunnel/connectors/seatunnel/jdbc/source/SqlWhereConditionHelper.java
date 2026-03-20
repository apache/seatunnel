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
     * Check if SQL contains top-level set operators (UNION/INTERSECT/EXCEPT). These operators at
     * top level would make auto-adding columns unsafe.
     */
    public static boolean hasTopLevelSetOperator(String sql) {
        String upper = sql.toUpperCase();
        int length = upper.length();
        int parenCount = 0;
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < length; i++) {
            char c = upper.charAt(i);

            // Track string literals
            if (!inString && (c == '\'' || c == '"')) {
                inString = true;
                stringChar = c;
            } else if (inString && c == stringChar) {
                // Check for escaped quote
                if (i + 1 < length && upper.charAt(i + 1) == stringChar) {
                    i++; // Skip escaped quote
                } else {
                    inString = false;
                }
            } else if (!inString) {
                if (c == '(') {
                    parenCount++;
                } else if (c == ')') {
                    parenCount--;
                } else if (parenCount == 0) {
                    // Check for set operators at top level (not inside parentheses)
                    if (i > 0
                            && (Character.isWhitespace(upper.charAt(i - 1))
                                    || upper.charAt(i - 1) == ')')) {
                        if (upper.startsWith("UNION", i) && isKeywordEnd(upper, i + 5)) {
                            return true;
                        }
                        if (upper.startsWith("INTERSECT", i) && isKeywordEnd(upper, i + 9)) {
                            return true;
                        }
                        if (upper.startsWith("EXCEPT", i) && isKeywordEnd(upper, i + 6)) {
                            return true;
                        }
                        if (upper.startsWith("MINUS", i) && isKeywordEnd(upper, i + 5)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private static boolean isKeywordEnd(String s, int index) {
        if (index >= s.length()) {
            return true;
        }
        char c = s.charAt(index);
        return Character.isWhitespace(c) || c == '(' || c == ';' || c == ')';
    }

    /**
     * Check if SELECT clause has top-level wildcard (* or table.*). Does NOT match wildcards inside
     * expressions like COUNT(*), col*2, SUM(a*b).
     */
    public static boolean hasTopLevelSelectWildcard(String sql) {
        // Remove string literals to avoid false positives
        String cleaned = sql.replaceAll("'[^']*'|\"[^\"]*\"", "");

        // Find the column list between SELECT and FROM
        String upperCleaned = cleaned.toUpperCase();
        int selectIdx = -1;

        // Find SELECT keyword (not inside parentheses)
        int parenCount = 0;
        for (int i = 0; i < upperCleaned.length(); i++) {
            char c = upperCleaned.charAt(i);
            if (c == '(') parenCount++;
            else if (c == ')') parenCount--;
            else if (parenCount == 0 && upperCleaned.startsWith("SELECT", i)) {
                // Check it's a keyword, not part of another word
                if (i == 0 || !Character.isLetterOrDigit(upperCleaned.charAt(i - 1))) {
                    int nextIdx = i + 6;
                    if (nextIdx >= upperCleaned.length()
                            || Character.isWhitespace(upperCleaned.charAt(nextIdx))) {
                        selectIdx = i;
                        break;
                    }
                }
            }
        }

        if (selectIdx == -1) {
            return false;
        }

        // Find FROM position in the original string
        int fromIdx = findMainFromClauseIndex(cleaned);
        if (fromIdx == -1) {
            // No FROM found, use entire string after SELECT
            fromIdx = cleaned.length();
        }

        // Extract column list (from after SELECT keyword to before FROM)
        int selectEndIdx = selectIdx + 6;
        String columnList = cleaned.substring(selectEndIdx, fromIdx).trim();

        // Skip DISTINCT/ALL keywords
        if (columnList.toUpperCase().startsWith("DISTINCT ")) {
            columnList = columnList.substring(9).trim();
        } else if (columnList.toUpperCase().startsWith("ALL ")) {
            columnList = columnList.substring(4).trim();
        }

        // Check for standalone * or table.*
        // Split by commas, handling nested parentheses
        List<String> columns = splitColumns(columnList);

        for (String column : columns) {
            String trimmed = column.trim();
            // Check if this column is exactly * or table.*
            if (trimmed.equals("*")) {
                return true;
            }
            // Check for table.* pattern
            if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*\\.\\*")) {
                return true;
            }
        }

        return false;
    }

    /** Split column list by commas, respecting nested parentheses. */
    private static List<String> splitColumns(String columnList) {
        List<String> columns = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parenCount = 0;

        for (int i = 0; i < columnList.length(); i++) {
            char c = columnList.charAt(i);
            if (c == '(') {
                parenCount++;
                current.append(c);
            } else if (c == ')') {
                parenCount--;
                current.append(c);
            } else if (c == ',' && parenCount == 0) {
                columns.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            columns.add(current.toString());
        }

        return columns;
    }

    /**
     * Check if SQL contains complex clauses that make auto-adding fields unsafe. Detects: GROUP BY,
     * HAVING, DISTINCT (with specific columns), LIMIT/OFFSET/FETCH/TOP, window functions (OVER),
     * and CTE (WITH ... AS).
     */
    public static boolean hasComplexClause(String sql) {
        String upper = sql.toUpperCase();
        int length = upper.length();
        int parenCount = 0;
        boolean inString = false;
        char stringChar = 0;

        // Check for CTE (WITH ... AS) at the beginning
        String trimmedUpper = upper.trim();
        if (trimmedUpper.startsWith("WITH") && isKeywordEnd(trimmedUpper, 4)) {
            return true;
        }

        for (int i = 0; i < length; i++) {
            char c = upper.charAt(i);

            if (!inString && (c == '\'' || c == '"')) {
                inString = true;
                stringChar = c;
            } else if (inString && c == stringChar) {
                if (i + 1 < length && upper.charAt(i + 1) == stringChar) {
                    i++;
                } else {
                    inString = false;
                }
            } else if (!inString) {
                if (c == '(') {
                    parenCount++;
                } else if (c == ')') {
                    parenCount--;
                } else if (parenCount == 0) {
                    boolean wordBoundary =
                            i == 0
                                    || Character.isWhitespace(upper.charAt(i - 1))
                                    || upper.charAt(i - 1) == ')';
                    if (wordBoundary) {
                        if (matchKeyword(upper, i, "GROUP")) return true;
                        if (matchKeyword(upper, i, "HAVING")) return true;
                        if (matchKeyword(upper, i, "LIMIT")) return true;
                        if (matchKeyword(upper, i, "OFFSET")) return true;
                        if (matchKeyword(upper, i, "FETCH")) return true;
                        if (matchKeyword(upper, i, "OVER")) return true;
                    }
                    // Check for TOP after SELECT
                    if (wordBoundary && matchKeyword(upper, i, "TOP")) return true;
                    // Check for DISTINCT with specific columns (not DISTINCT *)
                    if (wordBoundary
                            && upper.startsWith("DISTINCT", i)
                            && isKeywordEnd(upper, i + 8)) {
                        // Check if it's DISTINCT * (which is safe)
                        int afterDistinct = i + 8;
                        while (afterDistinct < length
                                && Character.isWhitespace(upper.charAt(afterDistinct))) {
                            afterDistinct++;
                        }
                        if (afterDistinct >= length || upper.charAt(afterDistinct) != '*') {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private static boolean matchKeyword(String upper, int pos, String keyword) {
        return upper.startsWith(keyword, pos) && isKeywordEnd(upper, pos + keyword.length());
    }

    /**
     * Ensure all required fields are in the SELECT clause.
     *
     * @param sql the original SQL
     * @param requiredFieldInfos fields that must be present (with original forms)
     * @return modified SQL with fields added if necessary
     */
    private static String ensureFieldsInSelect(String sql, Set<FieldInfo> requiredFieldInfos) {
        // For UNION/INTERSECT/EXCEPT queries, skip auto-adding columns
        // because it's unsafe to modify only the first branch.
        // The query will still be wrapped, but columns won't be auto-added.
        if (hasTopLevelSetOperator(sql)) {
            log.info(
                    "Detected UNION/INTERSECT/EXCEPT query, skipping auto-add missing fields. "
                            + "Please ensure all required fields are included in each SELECT branch. SQL: {}",
                    sql);
            return sql;
        }

        // For complex SQL (GROUP BY, HAVING, DISTINCT, LIMIT, window functions, CTE),
        // skip auto-adding columns to avoid breaking query semantics.
        if (hasComplexClause(sql)) {
            log.info(
                    "Detected complex SQL clause (GROUP BY/HAVING/DISTINCT/LIMIT/OVER/CTE), "
                            + "skipping auto-add missing fields. SQL: {}",
                    sql);
            return sql;
        }

        int fromIndex = findMainFromClauseIndex(sql);
        String selectClause;
        if (fromIndex != -1) {
            selectClause = sql.substring(0, fromIndex);
        } else {
            selectClause = sql;
        }

        // Check for top-level SELECT * or table.* wildcard
        // Do NOT match COUNT(*), col*2, SUM(a*b) etc.
        if (hasTopLevelSelectWildcard(selectClause)) {
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
