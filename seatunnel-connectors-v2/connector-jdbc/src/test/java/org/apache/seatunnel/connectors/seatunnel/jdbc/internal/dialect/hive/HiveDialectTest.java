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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies Hive metadata limiting and partition discovery. Exact query-output cases protect SQL
 * syntax and column qualification without reflecting into private implementation methods.
 */
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

    /**
     * Checks metadata limiting against exact SQL outputs for literals, identifiers and comments.
     * Existing limits and non-query statements remain unchanged so schema discovery preserves the
     * original statement contract.
     */
    @ParameterizedTest
    @MethodSource("metadataQueries")
    void testLimitMetadataQuery(String query, String expected) {
        Assertions.assertEquals(expected, HiveDialect.limitMetadataQuery(query));
    }

    private static Stream<Arguments> metadataQueries() {
        return Stream.of(
                Arguments.of("SELECT id FROM users;", "SELECT id FROM users\nLIMIT 1\n"),
                Arguments.of("SELECT id FROM users;; ", "SELECT id FROM users\nLIMIT 1\n "),
                Arguments.of(
                        "SELECT id FROM users -- trailing comment",
                        "SELECT id FROM users\nLIMIT 1\n -- trailing comment"),
                Arguments.of(
                        "SELECT id FROM users; -- trailing comment",
                        "SELECT id FROM users\nLIMIT 1\n -- trailing comment"),
                Arguments.of(
                        "/* leading */ WITH t AS (SELECT 1 AS id) SELECT * FROM t",
                        "/* leading */ WITH t AS (SELECT 1 AS id) SELECT * FROM t\nLIMIT 1\n"),
                Arguments.of(
                        "-- leading\nWITH t AS (SELECT 1 AS id) SELECT * FROM t;",
                        "-- leading\nWITH t AS (SELECT 1 AS id) SELECT * FROM t\nLIMIT 1\n"),
                Arguments.of(
                        "SELECT * FROM (SELECT id FROM users LIMIT 5) t",
                        "SELECT * FROM (SELECT id FROM users LIMIT 5) t\nLIMIT 1\n"),
                Arguments.of(
                        "SELECT 'limit;--' AS `limit` FROM users /* LIMIT 5 */",
                        "SELECT 'limit;--' AS `limit` FROM users\nLIMIT 1\n /* LIMIT 5 */"),
                Arguments.of(
                        "SELECT 'it''s a limit' AS name FROM users",
                        "SELECT 'it''s a limit' AS name FROM users\nLIMIT 1\n"),
                Arguments.of("SELECT id FROM users LIMIT 5;", "SELECT id FROM users LIMIT 5;"),
                Arguments.of("SELECT id FROM users LIMIT 2, 5", "SELECT id FROM users LIMIT 2, 5"),
                Arguments.of(
                        "SELECT id -- note\rFROM users",
                        "SELECT id -- note\rFROM users\nLIMIT 1\n"),
                Arguments.of(
                        "-- leading\rSELECT id FROM users",
                        "-- leading\rSELECT id FROM users\nLIMIT 1\n"),
                Arguments.of(
                        "WITH t AS (SELECT 1 AS id) INSERT INTO users SELECT id FROM t",
                        "WITH t AS (SELECT 1 AS id) INSERT INTO users SELECT id FROM t"),
                Arguments.of("SELECT 1; SELECT 2", "SELECT 1; SELECT 2"),
                Arguments.of("SELECT 'unterminated", "SELECT 'unterminated"),
                Arguments.of("SELECT (id FROM users", "SELECT (id FROM users"),
                Arguments.of("SELECT id /* unterminated", "SELECT id /* unterminated"),
                Arguments.of("-- comment only", "-- comment only"),
                Arguments.of("EXPLAIN SELECT id FROM users", "EXPLAIN SELECT id FROM users"));
    }
}
