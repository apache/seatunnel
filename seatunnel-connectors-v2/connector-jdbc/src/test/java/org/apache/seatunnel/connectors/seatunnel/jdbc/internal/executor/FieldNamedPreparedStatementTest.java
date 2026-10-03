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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FieldNamedPreparedStatementTest {

    @TempDir private Path tempDir;

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

    @ParameterizedTest
    @ValueSource(strings = {"field?question", "field name", "field:colon", "field\"quote"})
    public void testPrepareAndExecuteWithSpecialFieldName(String fieldName) throws Exception {
        String column = "\"" + fieldName.replace("\"", "\"\"") + "\"";
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                Statement ddl = connection.createStatement()) {
            ddl.execute("CREATE TABLE target (" + column + " INTEGER)");
            try (FieldNamedPreparedStatement statement =
                    FieldNamedPreparedStatement.prepareStatement(
                            connection,
                            "INSERT INTO target (" + column + ") VALUES (:" + fieldName + ")",
                            new String[] {fieldName})) {
                statement.setInt(1, 42);
                statement.executeUpdate();
            }
            try (ResultSet result = ddl.executeQuery("SELECT " + column + " FROM target")) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
                assertFalse(result.next());
            }
        }
    }

    @Test
    public void testPrepareRepeatedReorderedAndUnusedFields() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                "SELECT :left name, :right?value, :left name",
                                new String[] {"unused", "right?value", "left name"})) {
            statement.setInt(1, 999);
            statement.setInt(2, 7);
            statement.setInt(3, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
                assertEquals(7, result.getInt(2));
                assertEquals(42, result.getInt(3));
            }
        }
    }

    @Test
    public void testPrepareIgnoresQuotedTextAndComments() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                "SELECT ':missing?''text', :id AS \"alias:missing?\" /* :missing ? */ -- :missing ?\n",
                                new String[] {"id"})) {
            statement.setInt(1, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(":missing?'text", result.getString(1));
                assertEquals(42, result.getInt(2));
            }
        }
    }

    @Test
    public void testPreparePreservesPositionalSql() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                "SELECT ?, ':missing?', ? AS \"alias?\"",
                                new String[] {"second", "first"})) {
            statement.setInt(1, 7);
            statement.setInt(2, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(7, result.getInt(1));
                assertEquals(":missing?", result.getString(2));
                assertEquals(42, result.getInt(3));
            }
        }
    }

    @Test
    public void testPrepareDoesNotTruncateUnknownParameter() throws Exception {
        try (Connection connection =
                DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("parameters.db"))) {
            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    FieldNamedPreparedStatement.prepareStatement(
                                            connection, "SELECT :id2", new String[] {"id"}));
            assertTrue(error.getMessage().contains("[id2] not in source columns"));
        }
    }

    @Test
    public void testPrepareNamedParameterBeforeCast() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, "SELECT :id::INTEGER", new String[] {"id"})) {
            statement.setInt(1, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
            }
        }
    }

    @Test
    public void testPrepareChoosesCompleteNameAndPreservesDollarQuotedText() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                "SELECT :id2, :id, $tag$:missing?$tag$, $$:other?$$",
                                new String[] {"id", "id2"})) {
            statement.setInt(1, 7);
            statement.setInt(2, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
                assertEquals(7, result.getInt(2));
                assertEquals(":missing?", result.getString(3));
                assertEquals(":other?", result.getString(4));
            }
        }
    }

    @Test
    public void testPrepareDoesNotRewriteMixedParameterStyles() throws Exception {
        try (Connection connection =
                DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("parameters.db"))) {
            SQLException error =
                    assertThrows(
                            SQLException.class,
                            () ->
                                    FieldNamedPreparedStatement.prepareStatement(
                                            connection, "SELECT :id, ?", new String[] {"id"}));
            assertTrue(error.getMessage().contains("syntax error"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {":id", "?"})
    public void testPrepareBindsParametersInArrayExpressions(String placeholder) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                "SELECT list_extract([" + placeholder + "], 1)",
                                new String[] {"id"})) {
            statement.setInt(1, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"?", ":id"})
    public void testConfiguredSqlPreservesExistingBindingStyles(String placeholder)
            throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:duckdb:" + tempDir.resolve("parameters.db"));
                FieldNamedPreparedStatement statement =
                        FieldNamedPreparedStatement.prepareStatementForCustomSql(
                                connection, "SELECT " + placeholder, new String[] {"id"})) {
            statement.setInt(1, 42);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
            }
        }
    }

    @Test
    public void testConfiguredNamedSqlPreservesEscapedString() throws Exception {
        try (Connection connection =
                DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("parameters.db"))) {
            try (FieldNamedPreparedStatement statement =
                    FieldNamedPreparedStatement.prepareStatementForCustomSql(
                            connection,
                            "SELECT E'can\\'t' AS literal, :id AS id, 'fixed' AS other",
                            new String[] {"id"})) {
                statement.setInt(1, 7);
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals("can't", resultSet.getString("literal"));
                    assertEquals(7, resultSet.getInt("id"));
                    assertEquals("fixed", resultSet.getString("other"));
                    assertFalse(resultSet.next());
                }
            }
        }
    }
}
