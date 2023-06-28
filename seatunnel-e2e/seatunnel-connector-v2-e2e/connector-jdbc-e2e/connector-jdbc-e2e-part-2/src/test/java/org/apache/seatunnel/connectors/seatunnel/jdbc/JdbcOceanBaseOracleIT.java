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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Disabled("Disabled because it needs user's personal oceanbase account to run this test!")
public class JdbcOceanBaseOracleIT extends AbstractJdbcIT {
    private static final String OCEANBASE_IMAGE = "shihd/oceanbase:1.0";
    private static final String OCEANBASE_CONTAINER_HOST = "spark_e2e_oceanbase";
    private static final String OCEANBASE_DATABASE = "testdb";

    private static final String OCEANBASE_SOURCE = "people";
    private static final String OCEANBASE_SINK = "test3";

    private static final String OCEANBASE_USERNAME = "root";
    private static final String OCEANBASE_PASSWORD = "abcd";
    private static final int OCEANBASE_CONTAINER_PORT = 2881;
    private static final String OCEANBASE_URL = "jdbc:oceanbase://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.alipay.oceanbase.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_oceanbase_oracle_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE `people` (\n"
                    + "  `age` varchar(255)  DEFAULT NULL,\n"
                    + "  `name` varchar(255) DEFAULT NULL\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(OCEANBASE_URL, OCEANBASE_CONTAINER_PORT, OCEANBASE_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(OCEANBASE_DATABASE, OCEANBASE_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(OCEANBASE_IMAGE)
                .networkAliases(OCEANBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(OCEANBASE_CONTAINER_PORT)
                .localPort(OCEANBASE_CONTAINER_PORT)
                .jdbcTemplate(OCEANBASE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(OCEANBASE_USERNAME)
                .password(OCEANBASE_PASSWORD)
                .database(OCEANBASE_DATABASE)
                .sourceTable(OCEANBASE_SOURCE)
                .sinkTable(OCEANBASE_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() {}

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.0/oceanbase-client-2.4.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "age", "name",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, "f_" + i,
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(OCEANBASE_IMAGE);

        GenericContainer<?> container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(OCEANBASE_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(OCEANBASE_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", OCEANBASE_CONTAINER_PORT, OCEANBASE_CONTAINER_PORT)));
        return container;
    }
}
