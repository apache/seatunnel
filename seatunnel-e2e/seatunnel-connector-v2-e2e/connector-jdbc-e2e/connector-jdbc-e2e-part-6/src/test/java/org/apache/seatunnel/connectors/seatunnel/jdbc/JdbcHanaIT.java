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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeMapper;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class JdbcHanaIT extends AbstractJdbcIT {
    private static final String HANA_IMAGE = "saplabs/hanaexpress:2.00.076.00.20240701.1";
    private static final String HANA_NETWORK_ALIASES = "e2e_saphana";
    private static final String DRIVER_CLASS = "com.sap.db.jdbc.Driver";
    private static final int HANA_PORT = 39017;
    private static final String HANA_URL = "jdbc:sap://" + HOST + ":%s";
    private static final String USERNAME = "SYSTEM";
    private static final String PASSWORD = "testPassword123";
    private static final String DATABASE = "TEST";
    private static final String SOURCE_TABLE = "ALLDATATYPES";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_sap_hana_source_and_sink.conf",
                    "/jdbc_sap_hana_test_view_and_synonym.conf");

    // TODO The current Docker image cannot handle the annotated type normally,
    //  but the corresponding type can be handled normally on the standard HANA service
    private static final String CREATE_SOURCE_SQL =
            "CREATE TABLE %s (\n"
                    + "INT_VALUE INT PRIMARY KEY, \n"
                    + "VARCHAR_VALUE VARCHAR, \n"
                    + "VARCHAR_VALUE_255 VARCHAR(255), \n"
                    + "NVARCHAR_VALUE NVARCHAR, \n"
                    + "NVARCHAR_VALUE_255 NVARCHAR(255), \n"
                    + "TEXT_VALUE TEXT, \n"
                    + "BINTEXT_VALUE BINTEXT, \n"
                    //                + "DECIMAL_VALUE DECIMAL, \n"
                    + "DECIMAL_VALUE_10_2 DECIMAL(10, 2), \n"
                    //                + "SAMLL_DECIMAL_VALUE SMALLDECIMAL, \n"
                    + "TIMESTAMP_VALUE TIMESTAMP, \n"
                    + "SECOND_DATE_VALUE SECONDDATE,\n"
                    + "BOOLEAN_VALUE BOOLEAN, \n"
                    + "DATE_VALUE DATE, \n"
                    + "TIME_VALUE TIME, \n"
                    + "BIGINT_VALUE BIGINT, \n"
                    + "SMALLINT_VALUE SMALLINT, \n"
                    + "TINYINT_VALUE TINYINT, \n"
                    + "REAL_VALUE REAL, \n"
                    + "DOUBLE_VALUE DOUBLE, \n"
                    + "FLOAT_VALUE FLOAT, \n"
                    + "FLOAT_VALUE_10 FLOAT(10), \n"
                    //                + "BLOB_VALUE BLOB, \n"
                    + "CLOB_VALUE CLOB, \n"
                    + "NCLOB_VALUE NCLOB, \n"
                    //                + "BINARY_VALUE BINARY(16), \n"
                    //                + "VARBINARY_VALUE VARBINARY, \n"
                    //                + "VARBINARY_VALUE_256 VARBINARY(256), \n"
                    //                + "GEOMETRY_VALUE ST_GEOMETRY, \n"
                    //                + "GEOGRAPHY_VALUE ST_POINT, \n"
                    + "ALPHANUM_VALUE ALPHANUM, \n"
                    + "ALPHANUM_VALUE_20 ALPHANUM(20), \n"
                    + "SHORTTEXT_VALUE_255 SHORTTEXT(255) \n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(HANA_URL, HANA_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);

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
                .sinkTable(SOURCE_TABLE + "_SINK")
                .createSql(CREATE_SOURCE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .useSaveModeCreateTable(true)
                .testData(testDataSet)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/2.21.11/ngdbc-2.21.11.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "INT_VALUE",
                    "VARCHAR_VALUE",
                    "VARCHAR_VALUE_255",
                    "NVARCHAR_VALUE",
                    "NVARCHAR_VALUE_255",
                    "TEXT_VALUE",
                    "BINTEXT_VALUE",
                    "DECIMAL_VALUE_10_2",
                    "TIMESTAMP_VALUE",
                    "SECOND_DATE_VALUE",
                    "BOOLEAN_VALUE",
                    "DATE_VALUE",
                    "TIME_VALUE",
                    "BIGINT_VALUE",
                    "SMALLINT_VALUE",
                    "TINYINT_VALUE",
                    "REAL_VALUE",
                    "DOUBLE_VALUE",
                    "FLOAT_VALUE",
                    "FLOAT_VALUE_10",
                    "CLOB_VALUE",
                    "NCLOB_VALUE",
                    "ALPHANUM_VALUE",
                    "ALPHANUM_VALUE_20",
                    "SHORTTEXT_VALUE_255"
                };
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i,
                                "v",
                                "varchar_value_255",
                                "n",
                                "nvarchar_value_255",
                                "text_value",
                                "bintext_value",
                                1.0,
                                Date.valueOf(LocalDate.now()),
                                Date.valueOf(LocalDate.now()),
                                true,
                                Date.valueOf(LocalDate.now()),
                                Date.valueOf(LocalDate.now()),
                                1L,
                                1,
                                1,
                                1.0,
                                1.0,
                                1.0,
                                1.0,
                                "clob_value",
                                "nclob_value",
                                "a",
                                "alphanum_value_20",
                                "shorttext_value_255"
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql = "CREATE SCHEMA " + DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getDatabase(),
                                    jdbcCase.getSchema(),
                                    jdbcCase.getSourceTable()));
            statement.execute(createSource);

            if (!jdbcCase.isUseSaveModeCreateTable()) {
                if (jdbcCase.getSinkCreateSql() != null) {
                    createTemplate = jdbcCase.getSinkCreateSql();
                }
                String createSink =
                        String.format(
                                createTemplate,
                                buildTableInfoWithSchema(
                                        jdbcCase.getDatabase(),
                                        jdbcCase.getSchema(),
                                        jdbcCase.getSinkTable()));
                statement.execute(createSink);
            }
            // create view and synonym
            String createViewSql =
                    "CREATE VIEW TEST.ALLDATATYPES_VIEW AS SELECT * FROM TEST.ALLDATATYPES;";
            String createSynonymSql =
                    "CREATE SYNONYM TEST.ALLDATATYPES_SYNONYM FOR TEST.ALLDATATYPES;";
            statement.execute(createViewSql);
            statement.execute(createSynonymSql);
            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(HANA_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HANA_NETWORK_ALIASES)
                        .withCommand("--master-password", PASSWORD, "--agree-to-sap-license")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HANA_IMAGE)))
                        .waitingFor(
                                Wait.forLogMessage(".*Startup finished!.*", 1)
                                        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES)));
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
        Assertions.assertEquals(25, catalogTable.getTableSchema().getColumns().size());
    }
}
