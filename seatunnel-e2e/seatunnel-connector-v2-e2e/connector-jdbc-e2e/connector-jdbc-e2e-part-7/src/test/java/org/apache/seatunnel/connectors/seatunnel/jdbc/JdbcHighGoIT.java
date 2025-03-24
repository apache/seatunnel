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

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.highgo.HighGoCatalog;
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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class JdbcHighGoIT extends AbstractJdbcIT {
    protected static final String HIGHGO_IMAGE = "xuxuclassmate/highgo";

    private static final String HIGHGO_ALIASES = "e2e_highgo";
    private static final String DRIVER_CLASS = "com.highgo.jdbc.Driver";
    private static final int HIGHGO_PORT = 5866;
    private static final String HIGHGO_URL = "jdbc:highgo://" + HOST + ":%s/%s";
    private static final String USERNAME = "highgo";
    private static final String PASSWORD = "Highgo@123";
    private static final String DATABASE = "highgo";
    private static final String SCHEMA = "public";
    private static final String SOURCE_TABLE = "highgo_e2e_source_table";
    private static final String SINK_TABLE = "highgo_e2e_sink_table";
    private static final String CATALOG_TABLE = "e2e_table_catalog";
    private static final Integer GEN_ROWS = 100;
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_highgo_source_and_sink_with_full_type.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n"
                    + "  gid                    SERIAL PRIMARY KEY,\n"
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
                    + "  bpchar_col             BPCHAR(10)\n"
                    + ");";

    private static final String[] fieldNames =
            new String[] {
                "gid",
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
                "bpchar_col"
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
        TablePath sourceTablePath = TablePath.of(databaseName, "public", "highgo_e2e_source_table");
        TablePath targetTablePath = TablePath.of(databaseName, "public", "highgo_e2e_sink_table");
        HighGoCatalog highGoCatalog = (HighGoCatalog) catalog;
        CatalogTable catalogTable = highGoCatalog.getTable(sourceTablePath);
        dropTableWithAssert(highGoCatalog, targetTablePath, true);
        // not create index
        createIndexOrNot(highGoCatalog, targetTablePath, catalogTable, false);
        Assertions.assertFalse(hasIndex(highGoCatalog, targetTablePath));

        dropTableWithAssert(highGoCatalog, targetTablePath, true);
        // create index
        createIndexOrNot(highGoCatalog, targetTablePath, catalogTable, true);
        Assertions.assertTrue(hasIndex(highGoCatalog, targetTablePath));

        dropTableWithAssert(highGoCatalog, targetTablePath, true);
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
            HighGoCatalog highGoCatalog, TablePath targetTablePath, boolean ignoreIfNotExists) {
        highGoCatalog.dropTable(targetTablePath, ignoreIfNotExists);
        Assertions.assertFalse(highGoCatalog.tableExists(targetTablePath));
    }

    private void createIndexOrNot(
            HighGoCatalog highGoCatalog,
            TablePath targetTablePath,
            CatalogTable catalogTable,
            boolean createIndex) {
        highGoCatalog.createTable(targetTablePath, catalogTable, false, createIndex);
        Assertions.assertTrue(highGoCatalog.tableExists(targetTablePath));
    }

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(HIGHGO_URL, HIGHGO_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(HIGHGO_IMAGE)
                .networkAliases(HIGHGO_ALIASES)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(HIGHGO_PORT)
                .localPort(HIGHGO_PORT)
                .jdbcTemplate(HIGHGO_URL)
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
        return "https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar";
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
                                LocalDate.of(2024, 12, 12).atStartOfDay(),
                                LocalDateTime.of(2024, 12, 12, 10, 0),
                                "Testing"
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
                new GenericContainer<>(HIGHGO_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HIGHGO_ALIASES)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HIGHGO_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", HIGHGO_PORT, HIGHGO_PORT)));

        return container;
    }

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new HighGoCatalog(
                        DatabaseIdentifier.HIGHGO,
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        SCHEMA,
                        null);
        catalog.open();
    }
}
