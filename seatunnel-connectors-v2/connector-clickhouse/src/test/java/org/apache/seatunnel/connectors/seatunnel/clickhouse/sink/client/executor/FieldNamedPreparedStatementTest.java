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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldNamedPreparedStatementTest {

    @Test
    public void testParseNamedInsertStatementWithBasicType() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        Map<String, String> clickhouseTableSchema =
                new HashMap<String, String>() {
                    {
                        put("id", "UInt32");
                        put("name", "String");
                        put("age", "UInt32");
                    }
                };
        String sql = SqlUtils.getInsertIntoStatement(tableName, rowType, clickhouseTableSchema);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement =
                "INSERT INTO users (\"id\", \"name\", \"age\") " + "VALUES (?, ?, ?)";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, fieldNames, sql);
    }

    @Test
    public void testParseNamedInsertStatementWithJsonType() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age", "subject_scores"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE
                };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        Map<String, String> clickhouseTableSchema =
                new HashMap<String, String>() {
                    {
                        put("id", "UInt32");
                        put("name", "String");
                        put("age", "UInt32");
                        put("subject_scores", "JSON");
                    }
                };
        String sql = SqlUtils.getInsertIntoStatement(tableName, rowType, clickhouseTableSchema);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement =
                "INSERT INTO users (\"id\", \"name\", \"age\", \"subject_scores\") "
                        + "VALUES (?, ?, ?, CAST(? AS String))";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, fieldNames, sql);
    }

    @Test
    public void testParseNamedDeleteStatement() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        String sql = SqlUtils.getDeleteStatement(tableName, fieldNames, true);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement =
                "DELETE FROM \"users\" WHERE \"id\" = ? AND \"name\" = ? AND \"age\" = ? "
                        + "settings allow_experimental_lightweight_delete = true";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, fieldNames, sql);
    }

    @Test
    public void testParseNamedAlterTableUpdateStatement() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getAlterTableUpdateStatement(tableName, fieldNames, conditionFields);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement =
                "ALTER TABLE users UPDATE \"age\" = ? WHERE \"id\" = ? AND \"name\" = ? "
                        + "settings mutations_sync = 1";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, fieldNames, sql);
    }

    @Test
    public void testParseNamedAlterTableDeleteStatement() {
        String tableName = "users";
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getAlterTableDeleteStatement(tableName, conditionFields);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement =
                "ALTER TABLE users DELETE WHERE \"id\" = ? AND \"name\" = ? "
                        + "settings mutations_sync = 1";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, conditionFields, sql);
    }

    @Test
    public void testParseNamedRowExistsStatement() {
        String tableName = "users";
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getRowExistsStatement(tableName, conditionFields);

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap);
        String expectedStatement = "SELECT 1 FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?";
        Assertions.assertEquals(expectedStatement, statement);
        validateParameterMap(parameterMap, conditionFields, sql);
    }

    private void validateParameterMap(
            Map<String, List<Integer>> parameterMap, String[] fieldNames, String sql) {
        Assertions.assertTrue(
                parameterMap.size() >= fieldNames.length,
                "the statements must contain all the field parameters");
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            Assertions.assertTrue(
                    parameterMap.containsKey(fieldName),
                    fieldName + " doesn't exist in the parameters of SQL statement: " + sql);
        }
    }
}
