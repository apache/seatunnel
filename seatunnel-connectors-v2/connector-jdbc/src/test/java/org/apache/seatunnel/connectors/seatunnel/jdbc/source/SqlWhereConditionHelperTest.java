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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Unit tests for SqlWhereConditionHelper */
public class SqlWhereConditionHelperTest {

    @Test
    public void testExtractFieldNamesFromWhere_SimpleCondition() {
        String whereCondition = "WHERE partition_date = '2023-01-01'";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertTrue(fields.contains("partition_date"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_MultipleConditions() {
        String whereCondition =
                "WHERE partition_date = '2023-01-01' AND region_id IN (1,2,3) AND status <> 'deleted'";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(3, fields.size());
        Assertions.assertTrue(fields.contains("partition_date"));
        Assertions.assertTrue(fields.contains("region_id"));
        Assertions.assertTrue(fields.contains("status"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_QuotedFields() {
        String whereCondition = "WHERE `partition_date` = '2023-01-01' AND \"region_id\" > 100";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertTrue(fields.contains("partition_date"));
        Assertions.assertTrue(fields.contains("region_id"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithOperators() {
        String whereCondition =
                "WHERE date >= '2023-01-01' AND amount <= 1000 AND status != 'deleted'";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("date"));
        Assertions.assertTrue(fields.contains("amount"));
        Assertions.assertTrue(fields.contains("status"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithLike() {
        String whereCondition = "WHERE name LIKE '%test%' AND email LIKE '@example.com'";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("name"));
        Assertions.assertTrue(fields.contains("email"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithIsNull() {
        String whereCondition = "WHERE deleted_at IS NULL AND status IS NOT NULL";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("deleted_at"));
        Assertions.assertTrue(fields.contains("status"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_EmptyCondition() {
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere("");
        Assertions.assertTrue(fields.isEmpty());
    }

    @Test
    public void testFindMissingFields_AllPresent() {
        String sql = "SELECT col1, col2, partition_date FROM table";
        Set<String> requiredFields = new HashSet<>(Arrays.asList("col1", "partition_date"));

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, requiredFields);
        Assertions.assertTrue(missing.isEmpty());
    }

    @Test
    public void testFindMissingFields_SomeMissing() {
        String sql = "SELECT col1, col2 FROM table";
        Set<String> requiredFields =
                new HashSet<>(Arrays.asList("col1", "partition_date", "region_id"));

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, requiredFields);
        Assertions.assertEquals(2, missing.size());
        Assertions.assertTrue(missing.contains("partition_date"));
        Assertions.assertTrue(missing.contains("region_id"));
    }

    @Test
    public void testApplyWhereConditionWithWrap() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, false);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_AddMissingFields() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should insert missing field before FROM
        Assertions.assertTrue(result.contains(", partition_date FROM my_table"));
        Assertions.assertTrue(result.contains("SELECT * FROM ("));
    }

    @Test
    public void testApplyWhereConditionWithWrap_AddMissingFields_WithDistinct() {
        String sql = "SELECT DISTINCT col1, col2 FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // DISTINCT with specific columns is complex, should NOT add missing fields
        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "DISTINCT query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_AddMissingFields_ComplexFrom() {
        String sql = "SELECT col1 FROM (SELECT * FROM t) sub";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should insert missing field before FROM
        Assertions.assertTrue(result.contains(", partition_date FROM (SELECT * FROM t) sub"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_SelectStar() {
        // Even with SELECT *, if the field string isn't found, we might add it.
        // This is acceptable behavior (SELECT *, partition_date FROM ...)
        String sql = "SELECT * FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        // Should NOT contain explicit partition_date addition in the inner query because of SELECT
        // *
        Assertions.assertFalse(result.contains(", partition_date FROM"));
        Assertions.assertTrue(result.contains("(SELECT * FROM my_table)"));
    }

    // ==================== Additional Edge Case Tests ====================

    @Test
    public void testExtractFieldNamesFromWhere_WithBetween() {
        String whereCondition = "WHERE age BETWEEN 18 AND 65 AND salary BETWEEN 3000 AND 10000";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("age"));
        Assertions.assertTrue(fields.contains("salary"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithSquareBrackets() {
        String whereCondition = "WHERE [partition_date] = '2023-01-01' AND [region_id] > 100";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertTrue(fields.contains("partition_date"));
        Assertions.assertTrue(fields.contains("region_id"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithSpecialCharacters() {
        String whereCondition = "WHERE `user-name` = 'test' AND `order#id` > 100";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("user-name"));
        Assertions.assertTrue(fields.contains("order#id"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithComplexConditions() {
        String whereCondition =
                "WHERE (status = 'active' OR status = 'pending') AND (amount > 100 OR priority >= 5)";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("status"));
        Assertions.assertTrue(fields.contains("amount"));
        Assertions.assertTrue(fields.contains("priority"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithoutWhereKeyword() {
        String whereCondition = "partition_date = '2023-01-01' AND region_id IN (1,2,3)";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertTrue(fields.contains("partition_date"));
        Assertions.assertTrue(fields.contains("region_id"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithKeywordsInValues() {
        // Should not extract AND, OR, NULL as field names
        String whereCondition = "WHERE status = 'AND' AND type = 'OR' AND deleted_at IS NULL";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("status"));
        Assertions.assertTrue(fields.contains("type"));
        Assertions.assertTrue(fields.contains("deleted_at"));
        Assertions.assertFalse(fields.contains("AND"));
        Assertions.assertFalse(fields.contains("OR"));
        Assertions.assertFalse(fields.contains("NULL"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithMixedQuotes() {
        String whereCondition = "WHERE `field1` = 'value' AND \"field2\" > 100 AND [field3] < 200";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertEquals(3, fields.size());
        Assertions.assertTrue(fields.contains("field1"));
        Assertions.assertTrue(fields.contains("field2"));
        Assertions.assertTrue(fields.contains("field3"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithNotEqual() {
        String whereCondition = "WHERE status <> 'deleted' AND type != 'archived'";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("status"));
        Assertions.assertTrue(fields.contains("type"));
    }

    @Test
    public void testFindMissingFields_WithQuotedFields() {
        String sql = "SELECT `col1`, \"col2\", [col3] FROM table";
        Set<String> requiredFields = new HashSet<>(Arrays.asList("col1", "col2", "col3"));

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, requiredFields);
        Assertions.assertTrue(missing.isEmpty());
    }

    @Test
    public void testFindMissingFields_CaseInsensitive() {
        String sql = "SELECT COL1, Col2, col3 FROM table";
        Set<String> requiredFields = new HashSet<>(Arrays.asList("col1", "COL2", "CoL3"));

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, requiredFields);
        Assertions.assertTrue(missing.isEmpty());
    }

    @Test
    public void testFindMissingFields_EmptySet() {
        String sql = "SELECT col1, col2 FROM table";
        Set<String> requiredFields = new HashSet<>();

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, requiredFields);
        Assertions.assertTrue(missing.isEmpty());
    }

    @Test
    public void testApplyWhereConditionWithWrap_EmptyWhereCondition() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition = "";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, false);

        Assertions.assertEquals(sql, result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_NullWhereCondition() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition = null;

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, false);

        Assertions.assertEquals(sql, result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_WhitespaceWhereCondition() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition = "   ";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, false);

        Assertions.assertEquals(sql, result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_MultipleNestedSubqueries() {
        String sql = "SELECT a.col1 FROM (SELECT b.col1 FROM (SELECT col1 FROM base_table) b) a";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
        Assertions.assertTrue(result.contains("partition_date = '2023-01-01'"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_AddMultipleMissingFields() {
        String sql = "SELECT col1 FROM my_table";
        String whereCondition =
                "WHERE partition_date = '2023-01-01' AND region_id > 100 AND status = 'active'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should add all three missing fields
        Assertions.assertTrue(result.contains("partition_date"));
        Assertions.assertTrue(result.contains("region_id"));
        Assertions.assertTrue(result.contains("status"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithJoin() {
        String sql = "SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id";
        String whereCondition = "WHERE t1.partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithGroupBy() {
        String sql = "SELECT col1, COUNT(*) FROM my_table GROUP BY col1";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
        // GROUP BY: should NOT add missing fields (would break non-aggregate column rule)
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "GROUP BY query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithOrderBy() {
        String sql = "SELECT col1, col2 FROM my_table ORDER BY col1 DESC";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithLimit() {
        String sql = "SELECT col1, col2 FROM my_table LIMIT 100";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
        // LIMIT: should NOT add missing fields
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "LIMIT query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithUnion() {
        String sql = "SELECT col1 FROM table1 UNION SELECT col1 FROM table2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_SqlWithNewlines() {
        String sql = "SELECT col1,\n       col2,\n       col3\nFROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithStringContainingFrom() {
        String sql = "SELECT col1, 'FROM somewhere' as note FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_WithBacktickInString() {
        String sql = "SELECT col1, 'test`value' as note FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_ComplexWhereWithParentheses() {
        String sql = "SELECT col1, col2 FROM my_table";
        String whereCondition =
                "WHERE (partition_date = '2023-01-01' OR partition_date = '2023-01-02') AND (status IN ('active', 'pending'))";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
        Assertions.assertTrue(result.contains("partition_date"));
        Assertions.assertTrue(result.contains("status"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldAlreadyExists() {
        String sql = "SELECT col1, partition_date, col2 FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should not duplicate partition_date
        int firstIndex = result.indexOf("partition_date");
        int lastIndex = result.lastIndexOf("partition_date");
        // partition_date should appear at least twice (once in inner query, once in WHERE)
        Assertions.assertNotEquals(-1, firstIndex);
        Assertions.assertNotEquals(firstIndex, lastIndex);
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithUnderscoreAndNumbers() {
        String whereCondition = "WHERE field_123 = 'test' AND _private_field > 100";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("field_123"));
        Assertions.assertTrue(fields.contains("_private_field"));
    }

    @Test
    public void testExtractFieldNamesFromWhere_WithCaseInsensitiveOperators() {
        String whereCondition =
                "WHERE name like '%test%' AND status In (1,2,3) AND deleted_at is null";
        Set<String> fields = SqlWhereConditionHelper.extractFieldNamesFromWhere(whereCondition);

        Assertions.assertTrue(fields.contains("name"));
        Assertions.assertTrue(fields.contains("status"));
        Assertions.assertTrue(fields.contains("deleted_at"));
    }

    @Test
    public void testSqlWhereConditionHelper_PartialMatchBug() {
        // Case: field "id" is missing, but "user_id" exists in SQL.
        // Original bug: findMissingFields would return empty because "user_id" contains "id".
        String sql = "SELECT user_id, name FROM users";
        Set<String> fields = new HashSet<>();
        fields.add("id");

        List<String> missing = SqlWhereConditionHelper.findMissingFields(sql, fields);
        Assertions.assertTrue(
                missing.contains("id"), "Should identify 'id' as missing even if 'user_id' exists");

        // Case: field "id" exists as "id"
        sql = "SELECT id, name FROM users";
        missing = SqlWhereConditionHelper.findMissingFields(sql, fields);
        Assertions.assertFalse(missing.contains("id"), "Should find 'id'");

        // Case: field "id" exists as "`id`"
        sql = "SELECT `id`, name FROM users";
        missing = SqlWhereConditionHelper.findMissingFields(sql, fields);
        Assertions.assertFalse(missing.contains("id"), "Should find '`id`'");

        // Case: field "abc" missing, but "abcde" exists
        sql = "SELECT abcde FROM table";
        fields.clear();
        fields.add("abc");
        missing = SqlWhereConditionHelper.findMissingFields(sql, fields);
        Assertions.assertTrue(
                missing.contains("abc"), "Should identify 'abc' as missing even if 'abcde' exists");
    }

    // ==================== Tests for Problem 1: Field in WHERE/JOIN but not in SELECT
    // ====================

    @Test
    public void testFindMissingFields_FieldInWhereButNotInSelect() {
        // The field partition_date is in WHERE clause but not in SELECT clause
        // This is the core bug scenario - we should detect it as missing
        String selectClause = "SELECT id, name FROM";
        Set<String> requiredFields = new HashSet<>(Arrays.asList("partition_date"));

        List<String> missing =
                SqlWhereConditionHelper.findMissingFields(selectClause, requiredFields);
        Assertions.assertTrue(
                missing.contains("partition_date"),
                "partition_date should be missing from SELECT clause");
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldInInnerWhereButNotInSelect() {
        // Scenario: SQL has partition_date in its WHERE clause, but not in SELECT
        // When we apply additional where_condition with partition_date, we need to add it
        String sql = "SELECT id, name FROM orders WHERE partition_date > '2023-01-01'";
        String whereCondition = "WHERE partition_date = '2023-02-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // The partition_date should be added to SELECT because it's needed by outer WHERE
        Assertions.assertTrue(
                result.contains(", partition_date FROM"),
                "partition_date should be added to SELECT clause. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldInJoinButNotInSelect() {
        // Scenario: partition_date appears in JOIN ON clause but not in SELECT
        String sql =
                "SELECT t1.id, t1.name FROM orders t1 JOIN customers t2 ON t1.partition_date = t2.partition_date";
        String whereCondition = "WHERE t1.partition_date = '2023-02-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // The partition_date should be added to SELECT because it's needed by outer WHERE
        // Even though it appears in JOIN ON, it's not in SELECT
        Assertions.assertTrue(
                result.contains(", t1.partition_date FROM")
                        || result.contains(", partition_date FROM"),
                "partition_date should be added to SELECT clause. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldInSubqueryWhereButNotInOuterSelect() {
        // Scenario: partition_date is in inner query's WHERE but outer SELECT doesn't include it
        String sql =
                "SELECT id, name FROM (SELECT * FROM orders WHERE partition_date > '2023-01-01') sub";
        String whereCondition = "WHERE partition_date = '2023-02-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // The partition_date should be added to outer SELECT
        Assertions.assertTrue(
                result.contains(", partition_date FROM"),
                "partition_date should be added to outer SELECT clause. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldAlreadyInSelectNotDuplicated() {
        // Scenario: partition_date is already in SELECT, should not be duplicated
        String sql = "SELECT id, partition_date, name FROM orders WHERE status = 'active'";
        String whereCondition = "WHERE partition_date = '2023-02-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Count occurrences of partition_date in SELECT clause area (before first FROM in outer
        // query)
        String innerSql = result.substring(result.indexOf("(") + 1, result.indexOf(") tmp"));
        // The inner SQL should still have just one partition_date in SELECT
        int selectAreaEnd = innerSql.toUpperCase().indexOf(" FROM ");
        String selectArea = innerSql.substring(0, selectAreaEnd);
        long count = selectArea.chars().filter(c -> c == ',').count();
        // Original has 2 commas (id, partition_date, name), should still have 2
        Assertions.assertEquals(
                2, count, "Should not duplicate partition_date in SELECT. Result: " + result);
    }

    // ==================== Tests for Problem 2: Quoted field names preserved ====================

    @Test
    public void testExtractFieldInfosFromWhere_BacktickQuotedField() {
        String whereCondition = "WHERE `partition_date` = '2023-01-01'";
        Set<SqlWhereConditionHelper.FieldInfo> fieldInfos =
                SqlWhereConditionHelper.extractFieldInfosFromWhere(whereCondition);

        Assertions.assertEquals(1, fieldInfos.size());
        SqlWhereConditionHelper.FieldInfo info = fieldInfos.iterator().next();
        Assertions.assertEquals("partition_date", info.getRawName());
        Assertions.assertEquals("`partition_date`", info.getOriginalForm());
    }

    @Test
    public void testExtractFieldInfosFromWhere_DoubleQuoteQuotedField() {
        String whereCondition = "WHERE \"partition_date\" = '2023-01-01'";
        Set<SqlWhereConditionHelper.FieldInfo> fieldInfos =
                SqlWhereConditionHelper.extractFieldInfosFromWhere(whereCondition);

        Assertions.assertEquals(1, fieldInfos.size());
        SqlWhereConditionHelper.FieldInfo info = fieldInfos.iterator().next();
        Assertions.assertEquals("partition_date", info.getRawName());
        Assertions.assertEquals("\"partition_date\"", info.getOriginalForm());
    }

    @Test
    public void testExtractFieldInfosFromWhere_SquareBracketQuotedField() {
        String whereCondition = "WHERE [partition_date] = '2023-01-01'";
        Set<SqlWhereConditionHelper.FieldInfo> fieldInfos =
                SqlWhereConditionHelper.extractFieldInfosFromWhere(whereCondition);

        Assertions.assertEquals(1, fieldInfos.size());
        SqlWhereConditionHelper.FieldInfo info = fieldInfos.iterator().next();
        Assertions.assertEquals("partition_date", info.getRawName());
        Assertions.assertEquals("[partition_date]", info.getOriginalForm());
    }

    @Test
    public void testExtractFieldInfosFromWhere_ReservedWordField() {
        // Test with SQL reserved word "order"
        String whereCondition = "WHERE `order` = 1";
        Set<SqlWhereConditionHelper.FieldInfo> fieldInfos =
                SqlWhereConditionHelper.extractFieldInfosFromWhere(whereCondition);

        Assertions.assertEquals(1, fieldInfos.size());
        SqlWhereConditionHelper.FieldInfo info = fieldInfos.iterator().next();
        Assertions.assertEquals("order", info.getRawName());
        Assertions.assertEquals("`order`", info.getOriginalForm());
    }

    @Test
    public void testExtractFieldInfosFromWhere_SpecialCharacterField() {
        // Test with field containing special characters
        String whereCondition = "WHERE `order-id` = 1 AND `user name` = 'test'";
        Set<SqlWhereConditionHelper.FieldInfo> fieldInfos =
                SqlWhereConditionHelper.extractFieldInfosFromWhere(whereCondition);

        Assertions.assertEquals(2, fieldInfos.size());

        boolean foundOrderId = false;
        boolean foundUserName = false;
        for (SqlWhereConditionHelper.FieldInfo info : fieldInfos) {
            if (info.getRawName().equals("order-id")) {
                Assertions.assertEquals("`order-id`", info.getOriginalForm());
                foundOrderId = true;
            }
            if (info.getRawName().equals("user name")) {
                Assertions.assertEquals("`user name`", info.getOriginalForm());
                foundUserName = true;
            }
        }
        Assertions.assertTrue(foundOrderId, "Should find order-id field");
        Assertions.assertTrue(foundUserName, "Should find user name field");
    }

    @Test
    public void testApplyWhereConditionWithWrap_PreservesBacktickQuotes() {
        // When field is quoted with backticks, should preserve them when adding to SELECT
        String sql = "SELECT id, name FROM my_table";
        String whereCondition = "WHERE `partition_date` = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should use backtick-quoted form when inserting
        Assertions.assertTrue(
                result.contains(", `partition_date` FROM"),
                "Should preserve backtick quotes. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_PreservesDoubleQuotes() {
        // When field is quoted with double quotes, should preserve them when adding to SELECT
        String sql = "SELECT id, name FROM my_table";
        String whereCondition = "WHERE \"partition_date\" = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should use double-quote-quoted form when inserting
        Assertions.assertTrue(
                result.contains(", \"partition_date\" FROM"),
                "Should preserve double quotes. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_PreservesSquareBrackets() {
        // When field is quoted with square brackets, should preserve them when adding to SELECT
        String sql = "SELECT id, name FROM my_table";
        String whereCondition = "WHERE [partition_date] = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should use square-bracket-quoted form when inserting
        Assertions.assertTrue(
                result.contains(", [partition_date] FROM"),
                "Should preserve square brackets. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_ReservedWordFieldPreserved() {
        // Critical test: reserved word "order" must keep its quotes
        String sql = "SELECT id, name FROM orders";
        String whereCondition = "WHERE `order` = 1";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should preserve backticks for reserved word, otherwise SQL would be invalid
        Assertions.assertTrue(
                result.contains(", `order` FROM"),
                "Reserved word 'order' must be quoted. Result: " + result);
        // Should NOT contain unquoted version in SELECT
        Assertions.assertFalse(
                result.matches(".*, order FROM.*"),
                "Should not have unquoted 'order' in SELECT. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_SpecialCharacterFieldPreserved() {
        // Field with hyphen must keep its quotes
        String sql = "SELECT id, name FROM orders";
        String whereCondition = "WHERE `order-id` = 1";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should preserve backticks for field with hyphen
        Assertions.assertTrue(
                result.contains(", `order-id` FROM"),
                "Field with hyphen must be quoted. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_FieldWithSpacePreserved() {
        // Field with space must keep its quotes
        String sql = "SELECT id, name FROM orders";
        String whereCondition = "WHERE `user name` = 'test'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should preserve backticks for field with space
        Assertions.assertTrue(
                result.contains(", `user name` FROM"),
                "Field with space must be quoted. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_MultipleQuotedFieldsPreserved() {
        // Multiple fields with different quote styles
        String sql = "SELECT id FROM orders";
        String whereCondition = "WHERE `order-id` = 1 AND \"status\" = 'active' AND [type] = 'A'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // All should preserve their original quote styles
        Assertions.assertTrue(
                result.contains("`order-id`"),
                "Should preserve backticks for order-id. Result: " + result);
        Assertions.assertTrue(
                result.contains("\"status\""),
                "Should preserve double quotes for status. Result: " + result);
        Assertions.assertTrue(
                result.contains("[type]"),
                "Should preserve square brackets for type. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_MixedQuotedAndUnquotedFields() {
        // Mix of quoted and unquoted fields
        String sql = "SELECT id FROM orders";
        String whereCondition = "WHERE `order-id` = 1 AND status = 'active'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Quoted field should keep quotes, unquoted should stay unquoted
        Assertions.assertTrue(
                result.contains(", `order-id`") || result.contains("`order-id`,"),
                "Quoted order-id should preserve quotes. Result: " + result);
        Assertions.assertTrue(
                result.contains(", status") || result.contains("status,"),
                "Unquoted status should stay unquoted. Result: " + result);
    }

    // ==================== Integration tests combining Problem 1 and Problem 2 ====================

    @Test
    public void testIntegration_ReservedWordInWhereNotInSelect() {
        // Combined scenario: reserved word field appears in inner WHERE but not in SELECT
        String sql = "SELECT id, name FROM orders WHERE `order` > 0";
        String whereCondition = "WHERE `order` = 1";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should add `order` (with quotes) to SELECT, not plain order
        Assertions.assertTrue(
                result.contains(", `order` FROM"),
                "Should add quoted reserved word to SELECT. Result: " + result);
    }

    @Test
    public void testIntegration_SpecialCharFieldInJoinNotInSelect() {
        // Combined scenario: special char field in JOIN but not in SELECT
        String sql =
                "SELECT t1.id FROM orders t1 JOIN order_items t2 ON t1.`order-id` = t2.`order-id`";
        String whereCondition = "WHERE t1.`order-id` = 1";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should add `order-id` (with quotes) to SELECT
        Assertions.assertTrue(
                result.contains("`order-id`"),
                "Should add quoted special char field to SELECT. Result: " + result);
    }

    // ==================== Tests for hasTopLevelSelectWildcard ====================

    @Test
    public void testHasTopLevelSelectWildcard_SelectStar() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT * FROM t"),
                "SELECT * should be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_SelectTableStar() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT t.* FROM t"),
                "SELECT t.* should be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_SelectStarWithOtherColumns() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT id, * FROM t"),
                "SELECT id, * should be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_CountStar() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT COUNT(*) FROM t"),
                "SELECT COUNT(*) should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_CountStarWithOtherColumns() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT id, COUNT(*) FROM t"),
                "SELECT id, COUNT(*) should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_MultiplicationExpression() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT col1 * 2 FROM t"),
                "SELECT col1 * 2 should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_SumMultiplication() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT SUM(a * b) FROM t"),
                "SELECT SUM(a * b) should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_ComplexExpressions() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard(
                        "SELECT id, COUNT(*), col1 * col2 FROM t"),
                "Complex expressions with * should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_MultipleAggregates() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard(
                        "SELECT COUNT(*), SUM(amount), AVG(price) FROM orders"),
                "Multiple aggregates with * should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_NoWildcard() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard("SELECT id, name FROM t"),
                "SELECT id, name should NOT be detected as wildcard");
    }

    @Test
    public void testHasTopLevelSelectWildcard_StarInString() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSelectWildcard(
                        "SELECT id, '*' as pattern FROM t"),
                "* in string literal should NOT be detected as wildcard");
    }

    // ==================== Tests for hasTopLevelSetOperator ====================

    @Test
    public void testHasTopLevelSetOperator_Union() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 UNION SELECT id FROM t2"),
                "UNION should be detected");
    }

    @Test
    public void testHasTopLevelSetOperator_UnionAll() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 UNION ALL SELECT id FROM t2"),
                "UNION ALL should be detected");
    }

    @Test
    public void testHasTopLevelSetOperator_Intersect() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 INTERSECT SELECT id FROM t2"),
                "INTERSECT should be detected");
    }

    @Test
    public void testHasTopLevelSetOperator_Except() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 EXCEPT SELECT id FROM t2"),
                "EXCEPT should be detected");
    }

    @Test
    public void testHasTopLevelSetOperator_Minus() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 MINUS SELECT id FROM t2"),
                "MINUS should be detected");
    }

    @Test
    public void testHasTopLevelSetOperator_NoOperator() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSetOperator("SELECT id FROM t1"),
                "Simple query should NOT have set operator");
    }

    @Test
    public void testHasTopLevelSetOperator_UnionInSubquery() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT * FROM (SELECT id FROM t1 UNION SELECT id FROM t2) sub"),
                "UNION inside subquery should NOT be detected as top-level");
    }

    @Test
    public void testHasTopLevelSetOperator_UnionInWhere() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 UNION SELECT id FROM t3)"),
                "UNION inside WHERE clause should NOT be detected as top-level");
    }

    @Test
    public void testHasTopLevelSetOperator_UnionInString() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasTopLevelSetOperator(
                        "SELECT id, 'UNION' as type FROM t1"),
                "UNION in string literal should NOT be detected");
    }

    // ==================== Integration: COUNT(*) should still add missing fields
    // ====================

    @Test
    public void testApplyWhereConditionWithWrap_CountStar_AddsMissingFields() {
        // COUNT(*) without GROUP BY: missing fields should still be added
        String sql = "SELECT id, COUNT(*) FROM orders";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // partition_date should be added because COUNT(*) is not SELECT * and no GROUP BY
        Assertions.assertTrue(
                result.contains(", partition_date FROM"),
                "Missing field should be added for COUNT(*) query without GROUP BY. Result: "
                        + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_MultiplicationExpression_AddsMissingFields() {
        // col1 * 2 should NOT be treated as SELECT *
        String sql = "SELECT id, price * quantity as total FROM orders";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // partition_date should be added
        Assertions.assertTrue(
                result.contains(", partition_date FROM"),
                "Missing field should be added for multiplication expression query. Result: "
                        + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_ComplexAggregate_AddsMissingFields() {
        // Complex aggregate without GROUP BY: should still add missing fields
        String sql = "SELECT id, SUM(a * b) as total FROM orders";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // partition_date should be added
        Assertions.assertTrue(
                result.contains(", partition_date FROM"),
                "Missing field should be added for complex aggregate query without GROUP BY. Result: "
                        + result);
    }

    // ==================== Integration: UNION should NOT modify inner SQL ====================

    @Test
    public void testApplyWhereConditionWithWrap_UnionQuery_SkipsAddingFields() {
        // UNION query should NOT have fields auto-added to first branch
        String sql = "SELECT id FROM t1 UNION SELECT id FROM t2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should still wrap the query
        Assertions.assertTrue(
                result.contains("SELECT * FROM ("),
                "UNION query should still be wrapped. Result: " + result);
        Assertions.assertTrue(
                result.contains(") tmp WHERE"),
                "UNION query should have WHERE condition. Result: " + result);

        // Should NOT modify the inner SQL (no partition_date added to first branch)
        // The original UNION structure should be preserved
        Assertions.assertFalse(
                result.contains("SELECT id, partition_date FROM t1"),
                "UNION query should NOT have fields added to first branch only. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_UnionAllQuery_SkipsAddingFields() {
        String sql = "SELECT id FROM t1 UNION ALL SELECT id FROM t2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should still wrap
        Assertions.assertTrue(
                result.contains("SELECT * FROM ("),
                "UNION ALL query should still be wrapped. Result: " + result);

        // Should NOT add fields to inner query
        Assertions.assertFalse(
                result.contains("SELECT id, partition_date FROM t1"),
                "UNION ALL query should NOT have fields added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_IntersectQuery_SkipsAddingFields() {
        String sql = "SELECT id FROM t1 INTERSECT SELECT id FROM t2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(
                result.contains("SELECT * FROM ("),
                "INTERSECT query should still be wrapped. Result: " + result);
        Assertions.assertFalse(
                result.contains("SELECT id, partition_date FROM t1"),
                "INTERSECT query should NOT have fields added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_ExceptQuery_SkipsAddingFields() {
        String sql = "SELECT id FROM t1 EXCEPT SELECT id FROM t2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(
                result.contains("SELECT * FROM ("),
                "EXCEPT query should still be wrapped. Result: " + result);
        Assertions.assertFalse(
                result.contains("SELECT id, partition_date FROM t1"),
                "EXCEPT query should NOT have fields added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_UnionWithAllFields_WorksCorrectly() {
        // If UNION already has the field in all branches, it should work
        String sql = "SELECT id, partition_date FROM t1 UNION SELECT id, partition_date FROM t2";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should wrap and add WHERE
        Assertions.assertTrue(
                result.contains("SELECT * FROM ("),
                "UNION with all fields should be wrapped. Result: " + result);
        Assertions.assertTrue(
                result.contains(") tmp WHERE partition_date = '2023-01-01'"),
                "WHERE condition should be applied. Result: " + result);
    }

    // ==================== Edge cases: combination scenarios ====================

    @Test
    public void testApplyWhereConditionWithWrap_CountStarWithQuotedField() {
        // COUNT(*) with GROUP BY: should NOT add fields
        String sql = "SELECT id, COUNT(*) FROM orders GROUP BY id";
        String whereCondition = "WHERE `partition_date` = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // GROUP BY present, should NOT add quoted field
        Assertions.assertFalse(
                result.contains(", `partition_date` FROM"),
                "GROUP BY query should NOT have fields auto-added. Result: " + result);
        // But should still wrap
        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE"));
    }

    @Test
    public void testApplyWhereConditionWithWrap_SelectTableStar_NotAddFields() {
        // SELECT t.* should NOT add missing fields
        String sql = "SELECT t.* FROM orders t";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should NOT add partition_date because t.* covers all columns
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "SELECT t.* should not have fields added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_MultipleWildcards() {
        // Multiple table.* patterns
        String sql = "SELECT t1.*, t2.id FROM t1 JOIN t2 ON t1.id = t2.id";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should NOT add fields because t1.* is a wildcard
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "Query with table.* should not have fields added. Result: " + result);
    }

    // ==================== Tests for hasComplexClause ====================

    @Test
    public void testHasComplexClause_GroupBy() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT dept, COUNT(*) FROM emp GROUP BY dept"));
    }

    @Test
    public void testHasComplexClause_Having() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5"));
    }

    @Test
    public void testHasComplexClause_Limit() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause("SELECT id FROM t LIMIT 100"));
    }

    @Test
    public void testHasComplexClause_Offset() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause("SELECT id FROM t LIMIT 100 OFFSET 10"));
    }

    @Test
    public void testHasComplexClause_Fetch() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT id FROM t FETCH FIRST 10 ROWS ONLY"));
    }

    @Test
    public void testHasComplexClause_Top() {
        Assertions.assertTrue(SqlWhereConditionHelper.hasComplexClause("SELECT TOP 10 id FROM t"));
    }

    @Test
    public void testHasComplexClause_WindowFunction() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t"));
    }

    @Test
    public void testHasComplexClause_Distinct() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause("SELECT DISTINCT col1, col2 FROM t"));
    }

    @Test
    public void testHasComplexClause_DistinctStar_Safe() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause("SELECT DISTINCT * FROM t"),
                "DISTINCT * is safe, should not be detected as complex");
    }

    @Test
    public void testHasComplexClause_CTE() {
        Assertions.assertTrue(
                SqlWhereConditionHelper.hasComplexClause(
                        "WITH cte AS (SELECT id FROM t) SELECT * FROM cte"));
    }

    @Test
    public void testHasComplexClause_SimpleQuery() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause("SELECT id, name FROM users"));
    }

    @Test
    public void testHasComplexClause_SimpleQueryWithWhere() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT id, name FROM users WHERE status = 'active'"));
    }

    @Test
    public void testHasComplexClause_SimpleQueryWithJoin() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.id"));
    }

    @Test
    public void testHasComplexClause_GroupByInSubquery() {
        // GROUP BY inside subquery should NOT be detected at top level
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT * FROM (SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept) sub"),
                "GROUP BY inside subquery should NOT be detected as top-level complex clause");
    }

    @Test
    public void testHasComplexClause_LimitInSubquery() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause(
                        "SELECT * FROM (SELECT id FROM t LIMIT 100) sub"),
                "LIMIT inside subquery should NOT be detected as top-level complex clause");
    }

    @Test
    public void testHasComplexClause_KeywordInString() {
        Assertions.assertFalse(
                SqlWhereConditionHelper.hasComplexClause("SELECT id, 'GROUP BY' as label FROM t"),
                "GROUP BY in string literal should NOT be detected");
    }

    // ==================== Tests for complex clause fallback behavior ====================

    @Test
    public void testApplyWhereConditionWithWrap_GroupBy_SkipsAddingFields() {
        String sql = "SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept";
        String whereCondition = "WHERE region = 'US'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        // Should still wrap
        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertTrue(result.contains(") tmp WHERE region = 'US'"));
        // Should NOT add region to SELECT (GROUP BY makes it unsafe)
        Assertions.assertFalse(
                result.contains(", region FROM"),
                "GROUP BY query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_Having_SkipsAddingFields() {
        String sql = "SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept HAVING COUNT(*) > 5";
        String whereCondition = "WHERE region = 'US'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertFalse(
                result.contains(", region FROM"),
                "HAVING query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_Distinct_SkipsAddingFields() {
        String sql = "SELECT DISTINCT col1, col2 FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        // DISTINCT with specific columns: should NOT add fields (changes dedup semantics)
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "DISTINCT query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_Limit_SkipsAddingFields() {
        String sql = "SELECT col1, col2 FROM my_table LIMIT 100";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "LIMIT query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_WindowFunction_SkipsAddingFields() {
        String sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM my_table";
        String whereCondition = "WHERE partition_date = '2023-01-01'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        Assertions.assertFalse(
                result.contains(", partition_date FROM"),
                "Window function query should NOT have fields auto-added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_CTE_SkipsAddingFields() {
        String sql = "WITH cte AS (SELECT id, name FROM users) SELECT id FROM cte";
        String whereCondition = "WHERE name = 'test'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(result.contains("SELECT * FROM ("));
        // The inner SQL should remain unchanged (CTE detected, no fields added)
        Assertions.assertTrue(
                result.contains("WITH cte AS (SELECT id, name FROM users) SELECT id FROM cte"),
                "CTE inner SQL should not be modified. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_SimpleQuery_StillAddsFields() {
        // Simple query without complex clauses: should still add missing fields
        String sql = "SELECT id, name FROM users";
        String whereCondition = "WHERE age > 18";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(
                result.contains(", age FROM"),
                "Simple query should still have missing fields added. Result: " + result);
    }

    @Test
    public void testApplyWhereConditionWithWrap_SubqueryGroupBy_StillAddsFields() {
        // GROUP BY is inside subquery, outer query is simple — should still add fields
        String sql =
                "SELECT dept, cnt FROM (SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept) sub";
        String whereCondition = "WHERE region = 'US'";

        String result =
                SqlWhereConditionHelper.applyWhereConditionWithWrap(sql, whereCondition, true);

        Assertions.assertTrue(
                result.contains(", region FROM"),
                "Outer simple query with GROUP BY only in subquery should add fields. Result: "
                        + result);
    }
}
