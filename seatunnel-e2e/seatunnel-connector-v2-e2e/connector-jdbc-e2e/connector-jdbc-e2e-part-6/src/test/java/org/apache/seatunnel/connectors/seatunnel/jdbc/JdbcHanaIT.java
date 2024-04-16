/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeMapper;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason = "a")
public class JdbcHanaIT extends AbstractJdbcIT {

    private static final String HANA_IMAGE = "saplabs/hanaexpress:latest";
    private static final String HANA_NETWORK_ALIASES = "e2e_hana3";
    private static final String DRIVER_CLASS = "com.sap.db.jdbc.Driver";
    private static final int HANA_PORT = 39017;
    private static final String HANA_URL = "jdbc:sap://" + HOST + ":%s";
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "QA_SINK";
    private static final String SOURCE_TABLE = "QA_SINK.SOURCE_TABLE";

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    F_ID INT PRIMARY KEY,\n"
                    + "    F_NAME                   NVARCHAR(100),\n"
                    + "    F_DATE                      DATE\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(HANA_URL, HANA_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(null, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(HANA_IMAGE)
                .networkAliases(HANA_NETWORK_ALIASES)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(HANA_PORT)
                .localPort(HANA_PORT)
                .jdbcTemplate(HANA_URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(DATABASE)
                .sourceTable(SOURCE_TABLE)
                .createSql(CREATE_SQL)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult(String executeKey) {}

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/2.16.14/ngdbc-2.16.14.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {"F_ID", "F_NAME", "F_DATE"};

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, String.format("f%s", i), Date.valueOf(LocalDate.now())
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(HANA_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HANA_NETWORK_ALIASES)
                        .withCommand("--master-password", PASSWORD, "--agree-to-sap-license")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HANA_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", HANA_PORT, HANA_PORT)));

        return container;
    }

    @SneakyThrows
    @Test
    public void testCatalog() {
        CatalogTable catalogTable =
                CatalogUtils.getCatalogTable(
                        connection, TablePath.of(SOURCE_TABLE), new SapHanaTypeMapper());
        List<String> columnNames = catalogTable.getTableSchema().getPrimaryKey().getColumnNames();
        Assertions.assertEquals(1, columnNames.size());
    }
}
