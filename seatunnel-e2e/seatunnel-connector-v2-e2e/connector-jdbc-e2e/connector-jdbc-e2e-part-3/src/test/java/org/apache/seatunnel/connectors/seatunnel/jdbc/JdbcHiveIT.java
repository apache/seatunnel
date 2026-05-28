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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcHiveIT extends AbstractJdbcIT {

    private static final String HIVE_IMAGE = "apache/hive:3.1.3";
    private static final String HIVE_CONTAINER_HOST = "e2ehivejdbc";

    private static final String HIVE_DATABASE = "default";

    private static final String HIVE_SOURCE = "hive_e2e_source_table";
    private static final String HIVE_PARTITION_TABLE = "hive_e2e_partition_table";
    private static final String HIVE_USERNAME = "root";
    private static final String HIVE_PASSWORD = null;
    private static final int HIVE_PORT = 10000;
    private static final String HIVE_URL = "jdbc:hive2://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_hive_source_and_assert.conf",
                    "/jdbc_hive_partition_source_and_assert.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE hive_e2e_source_table"
                    + "("
                    + "    int_column              INT,"
                    + "    integer_column          INTEGER,"
                    + "    bigint_column           BIGINT,"
                    + "    smallint_column         SMALLINT,"
                    + "    tinyint_column          TINYINT,"
                    + "    double_column           DOUBLE,"
                    + "    double_PRECISION_column DOUBLE PRECISION,"
                    + "    float_column            FLOAT,"
                    + "    string_column           STRING,"
                    + "    char_column             CHAR(10),"
                    + "    varchar_column          VARCHAR(20),"
                    + "    boolean_column          BOOLEAN,"
                    + "    date_column             DATE,"
                    + "    timestamp_column        TIMESTAMP,"
                    + "    decimal_column          DECIMAL(10, 2),"
                    + "    numeric_column          NUMERIC(10, 2)"
                    + ")";

    private static final String PARTITION_TABLE_CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS "
                    + HIVE_DATABASE
                    + "."
                    + HIVE_PARTITION_TABLE
                    + "("
                    + "    id                      INT,"
                    + "    name                    STRING,"
                    + "    amount                  DOUBLE"
                    + ")"
                    + " PARTITIONED BY (dt STRING, hr STRING)";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(HIVE_URL, HIVE_PORT, HIVE_DATABASE);
        return JdbcCase.builder()
                .dockerImage(HIVE_IMAGE)
                .networkAliases(HIVE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(HIVE_PORT)
                .localPort(HIVE_PORT)
                .jdbcTemplate(HIVE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(HIVE_USERNAME)
                .password(HIVE_PASSWORD)
                .database(HIVE_DATABASE)
                .sourceTable(HIVE_SOURCE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .tablePathFullName(TablePath.DEFAULT.getFullName())
                .build();
    }

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();
            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getDatabase(), jdbcCase.getSourceTable()));
            statement.execute(createSource);
            statement.execute(PARTITION_TABLE_CREATE_SQL);
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    protected void insertTestData() {
        try (Statement statement = connection.createStatement()) {
            for (int i = 1; i <= 3; i++) {
                statement.execute(
                        "INSERT INTO hive_e2e_source_table "
                                + "VALUES (2,"
                                + "        1,"
                                + "        1234567890,"
                                + "        32767,"
                                + "        127,"
                                + "        123.45,"
                                + "        123.45,"
                                + "        67.89,"
                                + "        'Hello, Hive',"
                                + "        'CharCol',"
                                + "        'VarcharCol',"
                                + "        TRUE,"
                                + "        '2023-09-04',"
                                + "        '2023-09-04 10:30:00',"
                                + "        42.10,"
                                + "        42.12)");
            }
            // Insert data into partitioned table across 3 dates and 2 hours
            String[] dates = {"2023-09-01", "2023-09-02", "2023-09-03"};
            String[] hours = {"00", "01"};
            int id = 1;
            for (String dt : dates) {
                for (String hr : hours) {
                    statement.execute(
                            String.format(
                                    "INSERT INTO %s.%s PARTITION(dt='%s', hr='%s') "
                                            + "VALUES(%d, 'name_%d', %.2f)",
                                    HIVE_DATABASE,
                                    HIVE_PARTITION_TABLE,
                                    dt,
                                    hr,
                                    id,
                                    id,
                                    id * 10.5));
                    id++;
                }
            }
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.INSERT_DATA_FAILED, exception);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        return null;
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(HIVE_IMAGE)
                        .withExposedPorts(HIVE_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HIVE_CONTAINER_HOST)
                        .withEnv("SERVICE_NAME", "hiveserver2")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HIVE_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", HIVE_PORT, HIVE_PORT)));
        return container;
    }

    public void clearTable(String schema, String table) {
        // do nothing.
    }

    @Override
    void checkResult(
            String executeKey,
            org.apache.seatunnel.e2e.common.container.TestContainer container,
            org.testcontainers.containers.Container.ExecResult execResult) {
        try {
            TablePath partitionTablePath = TablePath.of(HIVE_DATABASE + "." + HIVE_PARTITION_TABLE);
            JdbcSourceTableConfig tableConfig =
                    JdbcSourceTableConfig.builder()
                            .tablePath(partitionTablePath.getFullName())
                            .useSelectCount(false)
                            .build();
            Map<TablePath, JdbcSourceTable> tables =
                    JdbcCatalogUtils.getTables(
                            JdbcConnectionConfig.builder()
                                    .url(jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost()))
                                    .driverName(jdbcCase.getDriverClass())
                                    .username(jdbcCase.getUserName())
                                    .password(jdbcCase.getPassword())
                                    .build(),
                            Lists.newArrayList(tableConfig));
            JdbcSourceTable partitionSourceTable = tables.get(partitionTablePath);
            Assertions.assertNotNull(
                    partitionSourceTable,
                    String.format(
                            "JdbcCatalogUtils should load partitioned table %s",
                            partitionTablePath.getFullName()));
            List<String> partitionKeys = partitionSourceTable.getCatalogTable().getPartitionKeys();
            log.info(
                    "Detected catalog partition keys for {}: {}",
                    partitionTablePath.getFullName(),
                    partitionKeys);
            Assertions.assertEquals(
                    Lists.newArrayList("dt", "hr"),
                    partitionKeys,
                    "JdbcCatalogUtils should carry Hive partition keys into CatalogTable");
        } catch (Exception e) {
            Assertions.fail("Failed to load Hive partition keys from JdbcCatalogUtils", e);
        }
    }
}
