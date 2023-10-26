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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase.OceanBaseOracleCatalog;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Disabled("Oracle mode of OceanBase Enterprise Edition does not provide docker environment")
public class JdbcOceanBaseOracleIT extends JdbcOceanBaseITBase {

    private static final String HOSTNAME = "e2e_oceanbase_oracle";
    private static final int PORT = 2883;
    private static final String USERNAME = "TESTUSER@test";
    private static final String PASSWORD = "";
    private static final String SCHEMA = "TESTUSER";

    @Override
    List<String> configFile() {
        return Lists.newArrayList("/jdbc_oceanbase_oracle_source_and_sink.conf");
    }

    @Override
    GenericContainer<?> initContainer() {
        throw new UnsupportedOperationException();
    }

    @BeforeAll
    @Override
    public void startUp() {
        jdbcCase = getJdbcCase();

        try {
            initializeJdbcConnection(jdbcCase.getJdbcUrl().replace(HOST, HOSTNAME));
        } catch (Exception e) {
            throw new RuntimeException("Failed to initial jdbc connection", e);
        }

        createNeededTables();
        insertTestData();
        initCatalog();
    }

    @Override
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection
                    .createStatement()
                    .execute("DROP TABLE " + getFullTableName(OCEANBASE_SOURCE));
            connection.createStatement().execute("DROP TABLE " + getFullTableName(OCEANBASE_SINK));
        }
        super.tearDown();
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(OCEANBASE_JDBC_TEMPLATE, PORT, SCHEMA);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, OCEANBASE_SOURCE.toUpperCase(), fieldNames);

        return JdbcCase.builder()
                .dockerImage(null)
                .networkAliases(HOSTNAME)
                .containerEnv(containerEnv)
                .driverClass(OCEANBASE_DRIVER_CLASS)
                .host(HOST)
                .port(PORT)
                .localPort(PORT)
                .jdbcTemplate(OCEANBASE_JDBC_TEMPLATE)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .schema(SCHEMA)
                .sourceTable(OCEANBASE_SOURCE.toUpperCase())
                .sinkTable(OCEANBASE_SINK.toUpperCase())
                .catalogSchema(SCHEMA)
                .catalogTable(OCEANBASE_CATALOG_TABLE)
                .createSql(createSqlTemplate())
                .configFile(configFile())
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    String createSqlTemplate() {
        return "create table %s\n"
                + "(\n"
                + "    VARCHAR_10_COL                varchar2(10),\n"
                + "    CHAR_10_COL                   char(10),\n"
                + "    CLOB_COL                      clob,\n"
                + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                + "    INTEGER_COL                   integer,\n"
                + "    FLOAT_COL                     float(10),\n"
                + "    REAL_COL                      real,\n"
                + "    BINARY_FLOAT_COL              binary_float,\n"
                + "    BINARY_DOUBLE_COL             binary_double,\n"
                + "    DATE_COL                      date,\n"
                + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3)\n"
                + ")";
    }

    @Override
    String[] getFieldNames() {
        return new String[] {
            "VARCHAR_10_COL",
            "CHAR_10_COL",
            "CLOB_COL",
            "NUMBER_3_SF_2_DP",
            "INTEGER_COL",
            "FLOAT_COL",
            "REAL_COL",
            "BINARY_FLOAT_COL",
            "BINARY_DOUBLE_COL",
            "DATE_COL",
            "TIMESTAMP_WITH_3_FRAC_SEC_COL"
        };
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = getFieldNames();

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f%s", i),
                                String.format("f%s", i),
                                String.format("f%s", i),
                                BigDecimal.valueOf(1.1),
                                i,
                                Float.parseFloat("2.2"),
                                Float.parseFloat("2.2"),
                                Float.parseFloat("22.2"),
                                Double.parseDouble("2.2"),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now())
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    String getFullTableName(String tableName) {
        return buildTableInfoWithSchema(SCHEMA, tableName.toUpperCase());
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
    protected void initCatalog() {
        catalog =
                new OceanBaseOracleCatalog(
                        "oceanbase",
                        USERNAME,
                        PASSWORD,
                        JdbcUrlUtil.getUrlInfo(jdbcCase.getJdbcUrl().replace(HOST, HOSTNAME)),
                        SCHEMA);
        catalog.open();
    }

    @Test
    @Override
    public void testCatalog() {
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
}
