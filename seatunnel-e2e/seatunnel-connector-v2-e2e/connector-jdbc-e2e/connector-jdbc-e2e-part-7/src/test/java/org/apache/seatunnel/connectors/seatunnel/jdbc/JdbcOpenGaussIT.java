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

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.opengauss.OpenGaussCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class JdbcOpenGaussIT extends AbstractJdbcIT {
    protected static final String OPENGAUSS_IMAGE = "opengauss/opengauss:5.0.0";

    private static final String OPEN_GAUSS_ALIASES = "e2e_OpenGauss";
    private static final String DRIVER_CLASS = "org.opengauss.Driver";
    private static final int OPEN_GAUSS_PORT = 5432;
    private static final String OPEN_GAUSS_URL = "jdbc:opengauss://" + HOST + ":%s/%s";
    private static final String USERNAME = "gaussdb";
    private static final String PASSWORD = "openGauss@123";
    private static final String DATABASE = "postgres";
    private static final String SCHEMA = "public";
    private static final String SOURCE_TABLE = "gs_e2e_source_table";
    private static final String SINK_TABLE = "gs_e2e_sink_table";
    private static final String CATALOG_TABLE = "e2e_table_catalog";
    private static final Integer GEN_ROWS = 100;
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_opengauss_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n"
                    + "  gid                    SERIAL PRIMARY KEY,\n"
                    + "  uuid_col               UUID,\n"
                    + "  text_col               TEXT,\n"
                    + "  varchar_col            VARCHAR(255),\n"
                    + "  char_col               CHAR(10),\n"
                    + "  boolean_col            bool,\n"
                    + "  smallint_col           int2,\n"
                    + "  integer_col            int4,\n"
                    + "  bigint_col             BIGINT,\n"
                    + "  decimal_col            DECIMAL(10, 2),\n"
                    + "  numeric_col            NUMERIC(8, 4),\n"
                    + "  real_col               float4,\n"
                    + "  double_precision_col   float8,\n"
                    + "  smallserial_col        SMALLSERIAL,\n"
                    + "  bigserial_col          BIGSERIAL,\n"
                    + "  date_col               DATE,\n"
                    + "  timestamp_col          TIMESTAMP,\n"
                    + "  bpchar_col             BPCHAR(10),\n"
                    + "  age                    INT NOT null\n"
                    + ");";

    private static final String[] fieldNames =
            new String[] {
                "gid",
                "uuid_col",
                "text_col",
                "varchar_col",
                "char_col",
                "boolean_col",
                "smallint_col",
                "integer_col",
                "bigint_col",
                "decimal_col",
                "numeric_col",
                "real_col",
                "double_precision_col",
                "smallserial_col",
                "bigserial_col",
                "date_col",
                "timestamp_col",
                "bpchar_col",
                "age"
            };

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @Test
    @Override
    public void testCatalog() {
        if (catalog == null) {
            return;
        }
        TablePath sourceTablePath =
                new TablePath(
                        jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSourceTable());
        TablePath targetTablePath =
                new TablePath(
                        jdbcCase.getCatalogDatabase(),
                        jdbcCase.getCatalogSchema(),
                        jdbcCase.getCatalogTable());

        CatalogTable catalogTable = catalog.getTable(sourceTablePath);
        catalog.createTable(targetTablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(targetTablePath));

        catalog.dropTable(targetTablePath, false);
        Assertions.assertFalse(catalog.tableExists(targetTablePath));
    }

    @Test
    public void testCreateIndex() {
        String schema = "public";
        String databaseName = jdbcCase.getDatabase();
        TablePath sourceTablePath = TablePath.of(databaseName, "public", "gs_e2e_source_table");
        TablePath targetTablePath = TablePath.of(databaseName, "public", "gs_ide_sink_table_2");
        OpenGaussCatalog openGaussCatalog = (OpenGaussCatalog) catalog;
        CatalogTable catalogTable = openGaussCatalog.getTable(sourceTablePath);
        dropTableWithAssert(openGaussCatalog, targetTablePath, true);
        // not create index
        createIndexOrNot(openGaussCatalog, targetTablePath, catalogTable, false);
        Assertions.assertFalse(hasIndex(openGaussCatalog, targetTablePath));

        dropTableWithAssert(openGaussCatalog, targetTablePath, true);
        // create index
        createIndexOrNot(openGaussCatalog, targetTablePath, catalogTable, true);
        Assertions.assertTrue(hasIndex(openGaussCatalog, targetTablePath));

        dropTableWithAssert(openGaussCatalog, targetTablePath, true);
    }

    protected boolean hasIndex(Catalog catalog, TablePath targetTablePath) {
        TableSchema tableSchema = catalog.getTable(targetTablePath).getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        if (primaryKey != null && StringUtils.isNotBlank(primaryKey.getPrimaryKey())) {
            return true;
        }
        if (!constraintKeys.isEmpty()) {
            return true;
        }
        return false;
    }

    private void dropTableWithAssert(
            OpenGaussCatalog openGaussCatalog,
            TablePath targetTablePath,
            boolean ignoreIfNotExists) {
        openGaussCatalog.dropTable(targetTablePath, ignoreIfNotExists);
        Assertions.assertFalse(openGaussCatalog.tableExists(targetTablePath));
    }

    private void createIndexOrNot(
            OpenGaussCatalog openGaussCatalog,
            TablePath targetTablePath,
            CatalogTable catalogTable,
            boolean createIndex) {
        openGaussCatalog.createTable(targetTablePath, catalogTable, false, createIndex);
        Assertions.assertTrue(openGaussCatalog.tableExists(targetTablePath));
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("OPEN_GAUSS_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(OPEN_GAUSS_URL, OPEN_GAUSS_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(OPENGAUSS_IMAGE)
                .networkAliases(OPEN_GAUSS_ALIASES)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(OPEN_GAUSS_PORT)
                .localPort(OPEN_GAUSS_PORT)
                .jdbcTemplate(OPEN_GAUSS_URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(DATABASE)
                .schema(SCHEMA)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .catalogDatabase(DATABASE)
                .catalogSchema(SCHEMA)
                .catalogTable(CATALOG_TABLE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar";
    }

    @Override
    protected Class<?> loadDriverClass() {
        return super.loadDriverClassFromUrl();
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (Integer i = 0; i < GEN_ROWS; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i,
                                UUID.randomUUID(),
                                String.valueOf(i),
                                String.valueOf(i),
                                String.valueOf(i),
                                i % 2 == 0,
                                i,
                                i,
                                Long.valueOf(i),
                                BigDecimal.valueOf(i * 10.0),
                                BigDecimal.valueOf(i * 0.01),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.111"),
                                i,
                                Long.valueOf(i),
                                LocalDate.of(2022, 1, 1).atStartOfDay(),
                                LocalDateTime.of(2022, 1, 1, 10, 0),
                                "Testing",
                                i
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(OPENGAUSS_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(OPEN_GAUSS_ALIASES)
                        .withEnv("GS_PASSWORD", PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(OPENGAUSS_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", OPEN_GAUSS_PORT, OPEN_GAUSS_PORT)));

        return container;
    }

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new OpenGaussCatalog(
                        DatabaseIdentifier.OPENGAUSS,
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        SCHEMA);
        // set connection
        ((OpenGaussCatalog) catalog).setConnection(jdbcUrl, connection);
        catalog.open();
    }
}
