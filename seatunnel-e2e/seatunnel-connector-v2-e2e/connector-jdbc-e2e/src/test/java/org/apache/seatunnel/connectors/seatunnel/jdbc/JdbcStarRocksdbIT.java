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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class JdbcStarRocksdbIT extends AbstractJdbcIT {

    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.5.0";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String NETWORK_ALIASES = "e2e_starRocksdb";
    private static final int SR_PORT = 9030;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "default";
    private static final String URL = "jdbc:mysql://" + HOST + ":%s/%s?createDatabaseIfNotExist=true";

    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";

    private static final String CONFIG_FILE = "/jdbc_starrocks_source_to_sink.conf";

    private static final String CREATE_SQL = "create table %s (\n" +
        "  BIGINT_COL BIGINT,\n" +
        "  LARGEINT_COL LARGEINT,\n" +
        "  SMALLINT_COL SMALLINT,\n" +
        "  TINYINT_COL TINYINT,\n" +
        "  BOOLEAN_COL BOOLEAN,\n" +
        "  DECIMAL_COL DECIMAL,\n" +
        "  DOUBLE_COL DOUBLE,\n" +
        "  FLOAT_COL FLOAT,\n" +
        "  INT_COL INT,\n" +
        "  CHAR_COL CHAR,\n" +
        "  VARCHAR_11_COL VARCHAR(11),\n" +
        "  STRING_COL STRING,\n" +
        "  DATETIME_COL DATETIME,\n" +
        "  DATE_COL DATE\n" +
        ") ENGINE = OLAP DUPLICATE KEY(`BIGINT_COL`) DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1 PROPERTIES (\n" +
        "  replication_num = 1,\n" +
        "  in_memory = false,\n" +
        "  storage_format = DEFAULT\n" +
        ");";

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(URL, SR_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
            .dockerImage(DOCKER_IMAGE)
            .networkAliases(NETWORK_ALIASES)
            .driverClass(DRIVER_CLASS)
            .host(HOST)
            .port(SR_PORT)
            .localPort(SR_PORT)
            .jdbcTemplate(URL)
            .jdbcUrl(jdbcUrl)
            .userName(USERNAME)
            .password(PASSWORD)
            .database(DATABASE)
            .sourceTable(SOURCE_TABLE)
            .sinkTable(SINK_TABLE)
            .createSql(CREATE_SQL)
            .configFile(CONFIG_FILE)
            .insertSql(insertSql)
            .testData(testDataSet)
            .build();
    }

    @Override
    void compareResult() throws SQLException, IOException {

    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {
            "BIGINT_COL",
            "LARGEINT_COL",
            "SMALLINT_COL",
            "TINYINT_COL",
            "BOOLEAN_COL",
            "DECIMAL_COL",
            "DOUBLE_COL",
            "FLOAT_COL",
            "INT_COL",
            "CHAR_COL",
            "VARCHAR_11_COL",
            "STRING_COL",
            "DATETIME_COL",
            "DATE_COL"
        };
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[] {
                    i,
                    i,
                    i,
                    i,
                    i % 2 == 0,
                    BigDecimal.valueOf(22.22),
                    Double.parseDouble("2.22"),
                    Float.parseFloat("2.22"),
                    i,
                    'f',
                    String.format("a_%s", i),
                    String.format("a_%s", i),
                    Timestamp.valueOf(LocalDateTime.now()),
                    Date.valueOf(LocalDate.now())
                });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(NETWORK_ALIASES)
            .withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 5236, 5236)));

        return container;
    }
}
