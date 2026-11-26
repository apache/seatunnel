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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests generated ClickHouse sink SQL for special characters in column names. */
class SqlUtilsTest {

    @Test
    void parseNamedStatementWithSpecialClickHouseColumnNames() {
        String insertSQL =
                SqlUtils.getInsertIntoStatement(
                        "sink_table",
                        new String[] {"GLREG", "GLREG#", "MY COL", "COL-1", "db.name"});
        String[] fieldNames = {"GLREG", "GLREG#", "MY COL", "COL-1", "db.name"};

        Map<String, List<Integer>> parameterMap = new HashMap<>();
        String parsedSQL =
                FieldNamedPreparedStatement.parseNamedStatement(
                        insertSQL, parameterMap, fieldNames);

        Assertions.assertEquals(5, parameterMap.size());
        Assertions.assertEquals("[1]", parameterMap.get("GLREG").toString());
        Assertions.assertEquals("[2]", parameterMap.get("GLREG#").toString());
        Assertions.assertEquals("[3]", parameterMap.get("MY COL").toString());
        Assertions.assertEquals("[4]", parameterMap.get("COL-1").toString());
        Assertions.assertEquals("[5]", parameterMap.get("db.name").toString());
        Assertions.assertEquals(
                "INSERT INTO sink_table (\"GLREG\", \"GLREG#\", \"MY COL\", \"COL-1\", \"db.name\") VALUES (?, ?, ?, ?, ?)",
                parsedSQL);

        Map<String, List<Integer>> legacyMap = new HashMap<>();
        String legacyParsed = FieldNamedPreparedStatement.parseNamedStatement(insertSQL, legacyMap);
        // Keep documenting the old parse signature: the special-name fix is only enabled when the
        // current field list is passed to the parser.
        Assertions.assertTrue(legacyMap.containsKey("GLREG"));
        Assertions.assertTrue(legacyMap.containsKey("MY"));
        Assertions.assertFalse(legacyMap.containsKey("GLREG#"));
        Assertions.assertTrue(legacyParsed.contains("?#"));
    }

    @Test
    void parseNamedStatementShouldNotSwallowNextClickHousePlaceholder() {
        String sql = SqlUtils.getInsertIntoStatement("sink_table", new String[] {"A", "B"});
        String[] fieldNames = {"A, :B", "A", "B"};

        Map<String, List<Integer>> parameterMap = new HashMap<>();
        String parsedSQL =
                FieldNamedPreparedStatement.parseNamedStatement(sql, parameterMap, fieldNames);

        Assertions.assertEquals("INSERT INTO sink_table (\"A\", \"B\") VALUES (?, ?)", parsedSQL);
        Assertions.assertFalse(parameterMap.containsKey("A, :B"));
        Assertions.assertEquals("[1]", parameterMap.get("A").toString());
        Assertions.assertEquals("[2]", parameterMap.get("B").toString());
    }

    @Test
    void prepareStatementBindsSpecialClickHouseColumnNames() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement delegate = Mockito.mock(PreparedStatement.class);
        String insertSQL =
                SqlUtils.getInsertIntoStatement(
                        "sink_table", new String[] {"GLREG", "GLREG#", "MY COL", "COL-1"});

        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(delegate);

        PreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(
                        connection, insertSQL, new String[] {"GLREG", "GLREG#", "MY COL", "COL-1"});
        statement.setString(2, "region");
        statement.setString(3, "space");

        Mockito.verify(connection)
                .prepareStatement(
                        "INSERT INTO sink_table (\"GLREG\", \"GLREG#\", \"MY COL\", \"COL-1\") VALUES (?, ?, ?, ?)");
        Mockito.verify(delegate).setString(2, "region");
        Mockito.verify(delegate).setString(3, "space");
    }
}
