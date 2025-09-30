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
import java.util.Map;

public class SqlUtilsTest {

    @Test
    public void testInsertStatementWithBasicType() {
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
        String expectedSql =
                "INSERT INTO users (\"id\", \"name\", \"age\") "
                        + "VALUES ({INT}:id, {STRING}:name, {INT}:age)";
        Assertions.assertEquals(expectedSql, sql);
    }

    @Test
    public void testInsertStatementWithJsonType() {
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
        String expectedSql =
                "INSERT INTO users (\"id\", \"name\", \"age\", \"subject_scores\") "
                        + "VALUES ({INT}:id, {STRING}:name, {INT}:age, {JSON}:subject_scores)";
        Assertions.assertEquals(expectedSql, sql);
    }

    @Test
    public void testDeleteStatement() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        String sql = SqlUtils.getDeleteStatement(tableName, fieldNames, true);
        String expectedSql =
                "DELETE FROM \"users\" WHERE \"id\" = {}:id AND \"name\" = {}:name AND \"age\" = {}:age "
                        + "settings allow_experimental_lightweight_delete = true";
        Assertions.assertEquals(expectedSql, sql);

        sql = SqlUtils.getDeleteStatement(tableName, fieldNames, false);
        expectedSql =
                "DELETE FROM \"users\" WHERE \"id\" = {}:id AND \"name\" = {}:name AND \"age\" = {}:age";
        Assertions.assertEquals(expectedSql, sql);
    }

    @Test
    public void testAlterTableUpdateStatement() {
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getAlterTableUpdateStatement(tableName, fieldNames, conditionFields);
        String expectedSql =
                "ALTER TABLE users UPDATE \"age\" = {}:age WHERE \"id\" = {}:id AND \"name\" = {}:name "
                        + "settings mutations_sync = 1";
        Assertions.assertEquals(expectedSql, sql);
    }

    @Test
    public void testAlterTableDeleteStatement() {
        String tableName = "users";
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getAlterTableDeleteStatement(tableName, conditionFields);
        String expectedSql =
                "ALTER TABLE users DELETE WHERE \"id\" = {}:id AND \"name\" = {}:name "
                        + "settings mutations_sync = 1";
        Assertions.assertEquals(expectedSql, sql);
    }

    @Test
    public void testRowExistsStatement() {
        String tableName = "users";
        String[] conditionFields = {"id", "name"};
        String sql = SqlUtils.getRowExistsStatement(tableName, conditionFields);
        String expectedSql = "SELECT 1 FROM \"users\" WHERE \"id\" = {}:id AND \"name\" = {}:name";
        Assertions.assertEquals(expectedSql, sql);
    }
}
