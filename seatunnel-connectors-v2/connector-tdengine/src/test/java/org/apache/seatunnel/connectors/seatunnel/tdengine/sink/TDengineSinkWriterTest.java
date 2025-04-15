/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.tdengine.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class TDengineSinkWriterTest {
    TDengineSinkWriter writer;

    @SneakyThrows
    @BeforeEach
    public void setup() {
        SeaTunnelRowType rowType;
        Config config;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        String[] fieldNames = new String[] {"id", "name", "description", "weight"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                };
        rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        config =
                ConfigFactory.empty()
                        .withValue(
                                "url", ConfigValueFactory.fromAnyRef("jdbc:TAOS://localhost:6030/"))
                        .withValue("database", ConfigValueFactory.fromAnyRef("test_db"))
                        .withValue("stable", ConfigValueFactory.fromAnyRef("test_stable"))
                        .withValue("username", ConfigValueFactory.fromAnyRef("root"))
                        .withValue("password", ConfigValueFactory.fromAnyRef("taosdata"))
                        .withValue("timezone", ConfigValueFactory.fromAnyRef("UTC"));

        // Mock JDBC objects
        Connection mockConnection = Mockito.mock(Connection.class);
        Statement mockStatement = Mockito.mock(Statement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);

        // Mock ResultSet behavior
        Mockito.when(mockResultSet.next())
                .thenReturn(true, false); // First call returns true, second call returns false
        Mockito.when(mockResultSet.getString("note")).thenReturn("TAG");

        // Mock Statement behavior
        Mockito.when(mockStatement.executeQuery("desc test_db.test_stable"))
                .thenReturn(mockResultSet);

        // Mock Connection behavior
        Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);

        try (MockedStatic<DriverManager> mockedStatic = Mockito.mockStatic(DriverManager.class)) {
            Mockito.when(DriverManager.getConnection(Mockito.anyString()))
                    .thenReturn(mockConnection);
            writer = new TDengineSinkWriter(config, rowType);
        }
    }

    @Test
    void testConvertDataTypeWithNull() {
        // Prepare test data
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 14, 15, 30, 45); // 2023-04-14 15:30:45
        Object[] input = {
            null, // Test for null value
            dateTime, // Test for LocalDateTime
            "test_string", // Test for String
            123, // Test for other types (Integer)
            45.67 // Test for other types (Double)
        };

        // Expected output
        Object[] expectedOutput = {
            null, // null remains unchanged
            "'2023-04-14 15:30:45.000'", // LocalDateTime is converted to a formatted string with
            // the specified timezone
            "'test_string'", // String is wrapped in single quotes
            123, // Integer remains unchanged
            45.67 // Double remains unchanged
        };

        Object[] result = writer.convertDataType(input);
        // Verify the results
        assertArrayEquals(expectedOutput, result);

        // Test for an empty array
        Object[] input1 = {};
        Object[] expectedOutput1 = {};
        Object[] result1 = writer.convertDataType(input1);
        assertArrayEquals(
                expectedOutput1, result1, "Empty input array should return an empty output array.");

        // Test for an array containing only null
        Object[] input2 = {null};
        Object[] expectedOutput2 = {null};
        Object[] result2 = writer.convertDataType(input2);
        assertArrayEquals(
                expectedOutput2, result2, "Array with only null should return an array with null.");
    }
}
