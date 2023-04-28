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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.tidb.TiDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "TODO: Spark has jar conflicts. ")
public class JdbcTidbIT extends AbstractJdbcIT {

    private static final String IMAGE = "pingcap/tidb:v6.5.1";

    private static final String CONTAINER_HOST = "seatunnel_e2e_tidb";
    private static final String DATABASE = "test";

    private static final String SCHEMA = "public";
    private static final String SOURCE = "source";
    private static final String SINK = "sink";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private static final int CONTAINER_PORT = 4000;

    private static final int PORT = 4000;
    private static final String URL = "jdbc:mysql://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_tidb_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n" + "age INT NOT NULL,\n" + "name VARCHAR(255) NOT NULL\n" + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DATABASE, SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(IMAGE)
                .networkAliases(CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(CONTAINER_PORT)
                .localPort(PORT)
                .jdbcTemplate(URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(DATABASE)
                .sourceTable(SOURCE)
                .sinkTable(SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() throws SQLException, IOException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM sink")) {
            ResultSet rs = statement.executeQuery();
            int i = 0;
            while (rs.next()) {
                Assertions.assertEquals(i, rs.getInt(1));
                Assertions.assertEquals("f_" + i, rs.getString(2));
                i++;
            }
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
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

    @BeforeAll
    @Override
    public void startUp() {
        dbServer = initContainer();
        jdbcCase = getJdbcCase();

        Startables.deepStart(Stream.of(dbServer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                this.initializeJdbcConnection(
                                        ((TiDBContainer) dbServer).getJdbcUrl()));
        createSchemaIfNeeded();
        createNeededTables();
        insertTestData();
    }

    @Override
    TiDBContainer initContainer() {
        DockerImageName imageName = DockerImageName.parse(IMAGE);

        TiDBContainer container =
                new TiDBContainer(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 4000, PORT)));
        return container;
    }
}
