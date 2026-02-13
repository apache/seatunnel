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

        // Should insert missing field before FROM
        Assertions.assertTrue(result.contains(", partition_date FROM my_table"));
        Assertions.assertTrue(result.contains("SELECT DISTINCT col1, col2"));
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
}
