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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FieldNamedPreparedStatementTest {

    private static final String[] SPECIAL_FIELDNAMES =
            new String[] {
                "USER@TOKEN",
                "字段%名称",
                "field_name",
                "field.name",
                "field-name",
                "$fieldName",
                "field&key",
                "field*value",
                "field#1",
                "field~test",
                "field!data",
                "field?question",
                "field^caret",
                "field+add",
                "field=value",
                "fieldmax",
                "field|pipe"
            };

    @Test
    public void testParseNamedStatementWithSpecialCharacters() {
        String sql =
                "INSERT INTO `nhp_emr_ws`.`cm_prescriptiondetails_cs` (`USER@TOKEN`, `字段%名称`, `field_name`, `field.name`, `field-name`, `$fieldName`, `field&key`, `field*value`, `field#1`, `field~test`, `field!data`, `field?question`, `field^caret`, `field+add`, `field=value`, `fieldmax`, `field|pipe`) VALUES (:USER@TOKEN, :字段%名称, :field_name, :field.name, :field-name, :$fieldName, :field&key, :field*value, :field#1, :field~test, :field!data, :field?question, :field^caret, :field+add, :field=value, :fieldmax, :field|pipe) ON DUPLICATE KEY UPDATE `USER@TOKEN`=VALUES(`USER@TOKEN`), `字段%名称`=VALUES(`字段%名称`), `field_name`=VALUES(`field_name`), `field.name`=VALUES(`field.name`), `field-name`=VALUES(`field-name`), `$fieldName`=VALUES(`$fieldName`), `field&key`=VALUES(`field&key`), `field*value`=VALUES(`field*value`), `field#1`=VALUES(`field#1`), `field~test`=VALUES(`field~test`), `field!data`=VALUES(`field!data`), `field?question`=VALUES(`field?question`), `field^caret`=VALUES(`field^caret`), `field+add`=VALUES(`field+add`), `field=value`=VALUES(`field=value`), `fieldmax`=VALUES(`fieldmax`), `field|pipe`=VALUES(`field|pipe`)";

        String exceptPreparedstatement =
                "INSERT INTO `nhp_emr_ws`.`cm_prescriptiondetails_cs` (`USER@TOKEN`, `字段%名称`, `field_name`, `field.name`, `field-name`, `$fieldName`, `field&key`, `field*value`, `field#1`, `field~test`, `field!data`, `field?question`, `field^caret`, `field+add`, `field=value`, `fieldmax`, `field|pipe`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `USER@TOKEN`=VALUES(`USER@TOKEN`), `字段%名称`=VALUES(`字段%名称`), `field_name`=VALUES(`field_name`), `field.name`=VALUES(`field.name`), `field-name`=VALUES(`field-name`), `$fieldName`=VALUES(`$fieldName`), `field&key`=VALUES(`field&key`), `field*value`=VALUES(`field*value`), `field#1`=VALUES(`field#1`), `field~test`=VALUES(`field~test`), `field!data`=VALUES(`field!data`), `field?question`=VALUES(`field?question`), `field^caret`=VALUES(`field^caret`), `field+add`=VALUES(`field+add`), `field=value`=VALUES(`field=value`), `fieldmax`=VALUES(`fieldmax`), `field|pipe`=VALUES(`field|pipe`)";

        Map<String, List<Integer>> paramMap = new HashMap<>();
        String actualSQL = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);
        assertEquals(exceptPreparedstatement, actualSQL);
        for (int i = 0; i < SPECIAL_FIELDNAMES.length; i++) {
            assertTrue(paramMap.containsKey(SPECIAL_FIELDNAMES[i]));
            assertEquals(i + 1, paramMap.get(SPECIAL_FIELDNAMES[i]).get(0));
        }
    }

    @Test
    public void testParseNamedStatement() {
        String sql = "UPDATE table SET col1 = :param1, col2 = :param1 WHERE col3 = :param2";
        Map<String, List<Integer>> paramMap = new HashMap<>();
        String expectedSQL = "UPDATE table SET col1 = ?, col2 = ? WHERE col3 = ?";

        String actualSQL = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);

        assertEquals(expectedSQL, actualSQL);
        assertTrue(paramMap.containsKey("param1"));
        assertTrue(paramMap.containsKey("param2"));
        assertEquals(1, paramMap.get("param1").get(0).intValue());
        assertEquals(2, paramMap.get("param1").get(1).intValue());
        assertEquals(3, paramMap.get("param2").get(0).intValue());
    }

    @Test
    public void testParseNamedStatementWithNoNamedParameters() {
        String sql = "SELECT * FROM table";
        Map<String, List<Integer>> paramMap = new HashMap<>();
        String expectedSQL = "SELECT * FROM table";

        String actualSQL = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);

        assertEquals(expectedSQL, actualSQL);
        assertTrue(paramMap.isEmpty());
    }

    @Test
    public void testParseNamedStatementWithSpacesInColumnNames() {
        String sql = "INSERT INTO test (first_name, last_name) VALUES (:first name, :last name)";
        String[] fieldNames = new String[] {"first name", "last name"};
        String expectedSQL = "INSERT INTO test (first_name, last_name) VALUES (?, ?)";

        Map<String, List<Integer>> paramMap = new HashMap<>();
        String actualSQL =
                FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap, fieldNames);

        assertEquals(expectedSQL, actualSQL);
        assertTrue(paramMap.containsKey("first name"));
        assertTrue(paramMap.containsKey("last name"));
        assertEquals(1, paramMap.get("first name").get(0).intValue());
        assertEquals(2, paramMap.get("last name").get(0).intValue());
    }

    @Test
    public void testParseNamedStatementWithSpacesKeepsDefaultBehaviorWithoutKnownNames() {
        // Without the known field names the default tokenizer must behave exactly as before:
        // it cuts the token at the first character outside the name class.
        String sql = "INSERT INTO test VALUES (:first name)";
        Map<String, List<Integer>> paramMap = new HashMap<>();

        String actualSQL = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);

        assertEquals("INSERT INTO test VALUES (? name)", actualSQL);
        assertTrue(paramMap.containsKey("first"));
        assertFalse(paramMap.containsKey("first name"));
    }

    @Test
    public void testParseNamedStatementWithRepeatedSpacesInColumnNames() {
        String sql =
                "INSERT INTO log (message, user, message_backup) VALUES (:user message, :user, :user message)";
        String[] fieldNames = new String[] {"user message", "user"};
        String expectedSQL = "INSERT INTO log (message, user, message_backup) VALUES (?, ?, ?)";

        Map<String, List<Integer>> paramMap = new HashMap<>();
        String actualSQL =
                FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap, fieldNames);

        assertEquals(expectedSQL, actualSQL);
        assertEquals(Arrays.asList(1, 3), paramMap.get("user message"));
        assertEquals(Arrays.asList(2), paramMap.get("user"));
    }

    @Test
    public void testParseNamedStatementPrefersLongestKnownName() {
        // Both known names start at the same offset; the longest one that is not followed by
        // another name-class character must win.
        String sql = "SELECT :MY COL \"MY COL\", :MY COLUMN \"MY COLUMN\" FROM t";
        String[] fieldNames = new String[] {"MY COL", "MY COLUMN"};

        Map<String, List<Integer>> paramMap = new HashMap<>();
        String actualSQL =
                FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap, fieldNames);

        assertEquals("SELECT ? \"MY COL\", ? \"MY COLUMN\" FROM t", actualSQL);
        assertEquals(Arrays.asList(1), paramMap.get("MY COL"));
        assertEquals(Arrays.asList(2), paramMap.get("MY COLUMN"));
    }

    @Test
    public void testPrepareStatementWithSpacesInColumnNames() throws Exception {
        String sql = "INSERT INTO test (first_name, last_name) VALUES (:first name, :last name)";
        String[] fieldNames = new String[] {"first name", "last name"};
        String expectedSQL = "INSERT INTO test (first_name, last_name) VALUES (?, ?)";

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(expectedSQL)).thenReturn(statement);

        FieldNamedPreparedStatement namedStatement =
                FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames);

        verify(connection).prepareStatement(expectedSQL);
        namedStatement.setString(1, "John");
        namedStatement.setString(2, "Doe");
        verify(statement).setString(1, "John");
        verify(statement).setString(2, "Doe");
    }
}
