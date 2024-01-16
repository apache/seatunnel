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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Disabled("we need resolve the issue of network between containers")
public class DorisCDCSinkIT extends AbstractDorisIT {

    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String DDL_SINK =
            "CREATE TABLE IF NOT EXISTS "
                    + DATABASE
                    + "."
                    + SINK_TABLE
                    + " (\n"
                    + "  uuid   BIGINT,\n"
                    + "  name    VARCHAR(128),\n"
                    + "  score   INT\n"
                    + ")ENGINE=OLAP\n"
                    + "UNIQUE KEY(`uuid`)\n"
                    + "DISTRIBUTED BY HASH(`uuid`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_allocation\" = \"tag.location.default: 1\""
                    + ")";

    @BeforeAll
    public void init() {
        initializeJdbcTable();
    }

    @TestTemplate
    public void testDorisCDCSink(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/write-cdc-changelog-to-doris.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String sinkSql = String.format("select * from %s.%s", DATABASE, SINK_TABLE);
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = jdbcConnection.createStatement()) {
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            while (sinkResultSet.next()) {
                List<Object> row =
                        Arrays.asList(
                                sinkResultSet.getLong("uuid"),
                                sinkResultSet.getString("name"),
                                sinkResultSet.getInt("score"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute(CREATE_DATABASE);
            // create sink table
            statement.execute(DDL_SINK);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }
}
