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

public class FieldNamedPreparedStatementTest {
    @Test
    public void testParseBasicNamedInsertStatement() {
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

        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, new HashMap<>());
        String expectedStatement =
                "INSERT INTO users (\"id\", \"name\", \"age\") " + "VALUES (?, ?, ?)";
        Assertions.assertEquals(expectedStatement, statement);
    }

    @Test
    public void testParseMultiNamedInsertStatement() {
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

        String statement = FieldNamedPreparedStatement.parseNamedStatement(sql, new HashMap<>());
        String expectedStatement =
                "INSERT INTO users (\"id\", \"name\", \"age\", \"subject_scores\") "
                        + "VALUES (?, ?, ?, CAST(? AS String))";
        Assertions.assertEquals(expectedStatement, statement);
    }
}
