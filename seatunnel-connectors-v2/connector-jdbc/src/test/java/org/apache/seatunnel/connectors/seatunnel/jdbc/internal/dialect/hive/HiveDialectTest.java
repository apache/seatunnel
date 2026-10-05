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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HiveDialectTest {

    @Test
    void testHiveDoesNotSupportPrimaryKeyMetadata() {
        Assertions.assertFalse(new HiveDialect().supportsPrimaryKeyMetadata());
    }

    @Test
    void testGetPartitionKeysFromDescribeOutput() throws Exception {
        HiveDialect hiveDialect = new HiveDialect();
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("DESCRIBE test_db.test_table"))
                .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, true, true, false);
        when(resultSet.getString(1))
                .thenReturn("id", "# Partition Information", "# col_name", "dt", "hr");

        Assertions.assertEquals(
                Arrays.asList("dt", "hr"),
                hiveDialect.getPartitionKeys(connection, TablePath.of("test_db.test_table")));
    }

    @Test
    void testGetPartitionKeysStopsAtDetailedTableInformation() throws Exception {
        HiveDialect hiveDialect = new HiveDialect();
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("DESCRIBE test_db.test_table"))
                .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
        when(resultSet.getString(1))
                .thenReturn(
                        "id",
                        "# Partition Information",
                        "# col_name",
                        "dt",
                        "hr",
                        "# Detailed Table Information",
                        "Database: default");

        Assertions.assertEquals(
                Arrays.asList("dt", "hr"),
                hiveDialect.getPartitionKeys(connection, TablePath.of("test_db.test_table")));
    }

    @Test
    void testGetPartitionKeysWithoutPartitionSection() throws Exception {
        HiveDialect hiveDialect = new HiveDialect();
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("DESCRIBE test_db.test_table"))
                .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(1)).thenReturn("id");

        Assertions.assertEquals(
                Collections.emptyList(),
                hiveDialect.getPartitionKeys(connection, TablePath.of("test_db.test_table")));
    }
}
