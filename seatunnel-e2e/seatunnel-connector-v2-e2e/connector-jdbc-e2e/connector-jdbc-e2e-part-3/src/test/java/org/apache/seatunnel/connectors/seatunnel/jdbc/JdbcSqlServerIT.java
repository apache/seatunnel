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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class JdbcSqlServerIT extends AbstractJdbcIT {

    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver";
    private static final String SQLSERVER_SOURCE = "source";
    private static final String SQLSERVER_SINK = "sink";
    private static final String SQLSERVER_DATABASE = "master";
    private static final String SQLSERVER_SCHEMA = "dbo";
    private static final String SQLSERVER_CATALOG_DATABASE = "catalog_test";
    private static final int SQLSERVER_CONTAINER_PORT = 1433;
    private static final String SQLSERVER_URL =
            "jdbc:sqlserver://"
                    + AbstractJdbcIT.HOST
                    + ":%s;encrypt=false;databaseName="
                    + SQLSERVER_DATABASE;
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_sqlserver_source_to_sink.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n"
                    + "\tINT_IDENTITY_TEST int identity,\n"
                    + "\tBIGINT_TEST bigint NOT NULL,\n"
                    + "\tBINARY_TEST binary(255) NULL,\n"
                    + "\tBIT_TEST bit NULL,\n"
                    + "\tCHAR_TEST char(255) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tDATE_TEST date NULL,\n"
                    + "\tDATETIME_TEST datetime NULL,\n"
                    + "\tDATETIME2_TEST datetime2 NULL,\n"
                    + "\tDATETIMEOFFSET_TEST datetimeoffset NULL,\n"
                    + "\tDECIMAL_TEST decimal(18,2) NULL,\n"
                    + "\tFLOAT_TEST float NULL,\n"
                    + "\tIMAGE_TEST image NULL,\n"
                    + "\tINT_TEST int NULL,\n"
                    + "\tMONEY_TEST money NULL,\n"
                    + "\tNCHAR_TEST nchar(1) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNTEXT_TEST ntext COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNUMERIC_TEST numeric(18,2) NULL,\n"
                    + "\tNVARCHAR_TEST nvarchar(16) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNVARCHAR_MAX_TEST nvarchar(MAX) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tREAL_TEST real NULL,\n"
                    + "\tSMALLDATETIME_TEST smalldatetime NULL,\n"
                    + "\tSMALLINT_TEST smallint NULL,\n"
                    + "\tSMALLMONEY_TEST smallmoney NULL,\n"
                    + "\tSQL_VARIANT_TEST sql_variant NULL,\n"
                    + "\tTEXT_TEST text COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tTIME_TEST time NULL,\n"
                    + "\tTINYINT_TEST tinyint NULL,\n"
                    + "\tUNIQUEIDENTIFIER_TEST uniqueidentifier NULL,\n"
                    + "\tVARBINARY_TEST varbinary(255) NULL,\n"
                    + "\tVARBINARY_MAX_TEST varbinary(MAX) NULL,\n"
                    + "\tVARCHAR_TEST varchar(16) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tVARCHAR_MAX_TEST varchar(MAX) COLLATE Chinese_PRC_CS_AS DEFAULT NULL NULL,\n"
                    + "\tXML_TEST xml NULL,\n"
                    + "\tUDT_TEST UDTDECIMAL NULL,\n"
                    + "\tCONSTRAINT PK_TEST_INDEX PRIMARY KEY (INT_IDENTITY_TEST)\n"
                    + ");";

    private static final String SINK_CREATE_SQL =
            "CREATE TABLE %s (\n"
                    + "\tINT_IDENTITY_TEST int NULL,\n"
                    + "\tBIGINT_TEST bigint NOT NULL,\n"
                    + "\tBINARY_TEST binary(255) NULL,\n"
                    + "\tBIT_TEST bit NULL,\n"
                    + "\tCHAR_TEST char(255) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tDATE_TEST date NULL,\n"
                    + "\tDATETIME_TEST datetime NULL,\n"
                    + "\tDATETIME2_TEST datetime2 NULL,\n"
                    + "\tDATETIMEOFFSET_TEST datetimeoffset NULL,\n"
                    + "\tDECIMAL_TEST decimal(18,2) NULL,\n"
                    + "\tFLOAT_TEST float NULL,\n"
                    + "\tIMAGE_TEST image NULL,\n"
                    + "\tINT_TEST int NULL,\n"
                    + "\tMONEY_TEST money NULL,\n"
                    + "\tNCHAR_TEST nchar(1) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNTEXT_TEST ntext COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNUMERIC_TEST numeric(18,2) NULL,\n"
                    + "\tNVARCHAR_TEST nvarchar(16) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tNVARCHAR_MAX_TEST nvarchar(MAX) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tREAL_TEST real NULL,\n"
                    + "\tSMALLDATETIME_TEST smalldatetime NULL,\n"
                    + "\tSMALLINT_TEST smallint NULL,\n"
                    + "\tSMALLMONEY_TEST smallmoney NULL,\n"
                    + "\tSQL_VARIANT_TEST sql_variant NULL,\n"
                    + "\tTEXT_TEST text COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tTIME_TEST time NULL,\n"
                    + "\tTINYINT_TEST tinyint NULL,\n"
                    + "\tUNIQUEIDENTIFIER_TEST uniqueidentifier NULL,\n"
                    + "\tVARBINARY_TEST varbinary(255) NULL,\n"
                    + "\tVARBINARY_MAX_TEST varbinary(MAX) NULL,\n"
                    + "\tVARCHAR_TEST varchar(16) COLLATE Chinese_PRC_CS_AS NULL,\n"
                    + "\tVARCHAR_MAX_TEST varchar(MAX) COLLATE Chinese_PRC_CS_AS DEFAULT NULL NULL,\n"
                    + "\tXML_TEST xml NULL,\n"
                    + "\tUDT_TEST UDTDECIMAL NULL\n"
                    + ");";

    private String username;

    private String password;

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(SQLSERVER_URL, SQLSERVER_CONTAINER_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable("", SQLSERVER_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(SQLSERVER_IMAGE)
                .networkAliases(SQLSERVER_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(AbstractJdbcIT.HOST)
                .port(SQLSERVER_CONTAINER_PORT)
                .localPort(SQLSERVER_CONTAINER_PORT)
                .jdbcTemplate(SQLSERVER_URL)
                .jdbcUrl(jdbcUrl)
                .userName(username)
                .password(password)
                .database(SQLSERVER_DATABASE)
                .schema(SQLSERVER_SCHEMA)
                .sourceTable(SQLSERVER_SOURCE)
                .sinkTable(SQLSERVER_SINK)
                .catalogDatabase(SQLSERVER_CATALOG_DATABASE)
                .catalogSchema(SQLSERVER_SCHEMA)
                .catalogTable(SQLSERVER_SINK)
                .createSql(CREATE_SQL)
                .sinkCreateSql(SINK_CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(TablePath.DEFAULT.getFullName())
                .build();
    }

    @Override
    protected void createSchemaIfNeeded() {
        // create user-defined type
        String sql = "CREATE TYPE UDTDECIMAL FROM decimal(12, 2);";
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "BIGINT_TEST",
                    "BINARY_TEST",
                    "BIT_TEST",
                    "CHAR_TEST",
                    "DATE_TEST",
                    "DATETIME_TEST",
                    "DATETIME2_TEST",
                    "DATETIMEOFFSET_TEST",
                    "DECIMAL_TEST",
                    "FLOAT_TEST",
                    "IMAGE_TEST",
                    "INT_TEST",
                    "MONEY_TEST",
                    "NCHAR_TEST",
                    "NTEXT_TEST",
                    "NUMERIC_TEST",
                    "NVARCHAR_TEST",
                    "NVARCHAR_MAX_TEST",
                    "REAL_TEST",
                    "SMALLDATETIME_TEST",
                    "SMALLINT_TEST",
                    "SMALLMONEY_TEST",
                    "SQL_VARIANT_TEST",
                    "TEXT_TEST",
                    "TIME_TEST",
                    "TINYINT_TEST",
                    "UNIQUEIDENTIFIER_TEST",
                    "VARBINARY_TEST",
                    "VARBINARY_MAX_TEST",
                    "VARCHAR_TEST",
                    "VARCHAR_MAX_TEST",
                    "XML_TEST",
                    "UDT_TEST"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                (long) i, // BIGINT_TEST
                                new byte[255], // BINARY_TEST
                                i % 2 == 0, // BIT_TEST
                                "CharValue" + i, // CHAR_TEST
                                LocalDate.now(), // DATE_TEST
                                LocalDateTime.now(), // DATETIME_TEST
                                LocalDateTime.now(), // DATETIME2_TEST
                                OffsetDateTime.now(), // DATETIMEOFFSET_TEST
                                new BigDecimal("123.45"), // DECIMAL_TEST
                                3.14f, // FLOAT_TEST
                                new byte[255], // IMAGE_TEST
                                42, // INT_TEST
                                new BigDecimal("567.89"), // MONEY_TEST
                                "N", // NCHAR_TEST
                                "NTextValue" + i, // NTEXT_TEST
                                new BigDecimal("987.65"), // NUMERIC_TEST
                                "NVarCharValue" + i, // NVARCHAR_TEST
                                "NVarCharMaxValue" + i, // NVARCHAR_MAX_TEST
                                2.71f, // REAL_TEST
                                LocalDateTime.now(), // SMALLDATETIME_TEST
                                (short) 123, // SMALLINT_TEST
                                new BigDecimal("456.78"), // SMALLMONEY_TEST
                                "SQL Variant Value" + i, // SQL_VARIANT_TEST
                                "TextValue" + i, // TEXT_TEST
                                LocalTime.now(), // TIME_TEST
                                (short) 5, // TINYINT_TEST
                                UUID.randomUUID(), // UNIQUEIDENTIFIER_TEST
                                new byte[255], // VARBINARY_TEST
                                new byte[8000], // VARBINARY_MAX_TEST
                                "VarCharValue" + i, // VARCHAR_TEST
                                "VarCharMaxValue" + i, // VARCHAR_MAX_TEST
                                "<xml>Test" + i + "</xml>", // XML_TEST
                                new BigDecimal("123.45") // UDT_TEST
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(SQLSERVER_IMAGE);

        MSSQLServerContainer<?> container =
                new MSSQLServerContainer<>(imageName)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .acceptLicense()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", SQLSERVER_CONTAINER_PORT, SQLSERVER_CONTAINER_PORT)));

        try {
            Class.forName(container.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.DRIVER_NOT_FOUND, "Not found suitable driver for mssql", e);
        }

        username = container.getUsername();
        password = container.getPassword();

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "[" + field + "]";
    }

    @Override
    public void clearTable(String schema, String table) {
        // do nothing.
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    @Override
    protected void initCatalog() {
        catalog =
                new SqlServerCatalog(
                        "sqlserver",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        SqlServerURLParser.parse(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        SQLSERVER_SCHEMA);
        catalog.open();
    }

    @Test
    public void testCatalog() {
        TablePath tablePathSqlserver = TablePath.of("master", "dbo", "source");
        TablePath tablePathSqlserverSink = TablePath.of("master", "dbo", "sink_lw");
        SqlServerCatalog sqlServerCatalog = (SqlServerCatalog) catalog;
        // add comment
        sqlServerCatalog.executeSql(
                tablePathSqlserver,
                "execute sp_addextendedproperty 'MS_Description','\"#¥%……&*();\\\\;'',,..``````//''@Xx''\\''\"','user','dbo','table','source','column','BIGINT_TEST';");
        CatalogTable catalogTable = sqlServerCatalog.getTable(tablePathSqlserver);
        // sink tableExists ?
        boolean tableExistsBefore = sqlServerCatalog.tableExists(tablePathSqlserverSink);
        Assertions.assertFalse(tableExistsBefore);
        // create table
        sqlServerCatalog.createTable(tablePathSqlserverSink, catalogTable, true);
        boolean tableExistsAfter = sqlServerCatalog.tableExists(tablePathSqlserverSink);
        Assertions.assertTrue(tableExistsAfter);
        // comment
        final CatalogTable sinkTable = sqlServerCatalog.getTable(tablePathSqlserverSink);
        Assertions.assertEquals(
                sinkTable.getTableSchema().getColumns().get(1).getComment(),
                "\"#¥%……&*();\\\\;',,..``````//'@Xx'\\'\"");
        // isExistsData ?
        boolean existsDataBefore = sqlServerCatalog.isExistsData(tablePathSqlserverSink);
        Assertions.assertFalse(existsDataBefore);
        // insert one data
        sqlServerCatalog.executeSql(
                tablePathSqlserverSink,
                "insert into sink_lw(INT_IDENTITY_TEST, BIGINT_TEST) values(1, 12)");
        boolean existsDataAfter = sqlServerCatalog.isExistsData(tablePathSqlserverSink);
        Assertions.assertTrue(existsDataAfter);
        // truncateTable
        sqlServerCatalog.truncateTable(tablePathSqlserverSink, true);
        Assertions.assertFalse(sqlServerCatalog.isExistsData(tablePathSqlserverSink));
        // drop table
        sqlServerCatalog.dropTable(tablePathSqlserverSink, true);
        Assertions.assertFalse(sqlServerCatalog.tableExists(tablePathSqlserverSink));
        sqlServerCatalog.close();
    }
}
