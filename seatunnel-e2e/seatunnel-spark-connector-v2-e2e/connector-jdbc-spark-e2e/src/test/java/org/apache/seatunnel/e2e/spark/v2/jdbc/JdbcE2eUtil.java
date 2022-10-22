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

package org.apache.seatunnel.e2e.spark.v2.jdbc;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JdbcE2eUtil {

    public static void compare(Connection conn, String sourceSql, String sinkSQL, String columns) throws SQLException, IOException {
        String[] split = StringUtils.split(columns, ",");
        List<String> columnList = Arrays.stream(split)
            .map(o -> StringUtils.remove(o, "\n"))
            .map(String::trim)
            .collect(Collectors.toList());
        compare(conn, sourceSql, sinkSQL, columnList);
    }

    @SuppressWarnings("magicnumber")
    public static void compare(Connection conn, String sourceSql, String sinkSQL, List<String> columns) throws SQLException, IOException {
        Assertions.assertTrue(conn.isValid(10));
        try (Statement sourceState = conn.createStatement();
            Statement sinkState = conn.createStatement()
        ) {
            ResultSet source = sourceState.executeQuery(sourceSql);
            ResultSet sink = sinkState.executeQuery(sinkSQL);
            compareResultSet(source, sink, columns);
        }
    }

    private static void compareResultSet(ResultSet source, ResultSet sink, List<String> columns) throws SQLException, IOException {
        Assertions.assertNotNull(source, "source can't be null");
        Assertions.assertNotNull(sink, "source can't be null");
        ResultSetMetaData sourceMetaData = source.getMetaData();
        ResultSetMetaData sinkMetaData = sink.getMetaData();
        Assertions.assertEquals(sourceMetaData.getColumnCount(), sinkMetaData.getColumnCount());
        while (source.next()) {
            if (sink.next()) {
                // compare by name
                if (CollectionUtils.isNotEmpty(columns)) {
                    for (String column : columns) {
                        int sourceCnt = source.findColumn(column);
                        int sinkCnt = sink.findColumn(column);
                        Assertions.assertTrue(compareColumn(source, sink, sourceCnt, sinkCnt));
                    }
                } else {
                    // compare all column
                    for (int i = 1; i <= sourceMetaData.getColumnCount(); i++) {
                        Assertions.assertTrue(compareColumn(source, sink, i, i));
                    }
                }
                continue;
            }
            Assertions.fail("the row of source != sink");
        }
    }

    private static boolean compareColumn(ResultSet source, ResultSet sink, int sourceCnt, int sinkCnt) throws SQLException, IOException {
        Object sourceObject = source.getObject(sourceCnt);
        Object sinkObject = sink.getObject(sinkCnt);
        if (Objects.deepEquals(sourceObject, sinkObject)) {
            return true;
        }
        InputStream sourceAsciiStream = source.getBinaryStream(sourceCnt);
        InputStream sinkAsciiStream = sink.getBinaryStream(sinkCnt);
        String sourceValue = IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
        String sinkValue = IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
        return StringUtils.equals(sourceValue, sinkValue);
    }
}
