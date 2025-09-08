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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.awaitility.Durations.TWO_SECONDS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public class OracleCDCWithSchemaChangeIT extends AbstractOracleCDCIT implements TestResource {

    private static final String BASIC_QUERY = "select * from %s.%s";

    private static final String QUERY = BASIC_QUERY + " ORDER BY ID ASC";

    private static final String ORACLE_DESC =
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns WHERE table_name = '%s' AND owner = '%s'";

    private static final String PROJECTION_QUERY =
            "select ID,VAL_VARCHAR,VAL_VARCHAR2,VAL_NVARCHAR2,VAL_CHAR,VAL_NCHAR,VAL_BF,VAL_BD,VAL_F,VAL_F_10,VAL_NUM,VAL_DP,VAL_R,VAL_DECIMAL,VAL_NUMERIC,VAL_NUM_VS,VAL_INT,VAL_INTEGER,VAL_SMALLINT,VAL_NUMBER_38_NO_SCALE,VAL_NUMBER_38_SCALE_0,VAL_NUMBER_1,VAL_NUMBER_2,VAL_NUMBER_4,VAL_NUMBER_9,VAL_NUMBER_18,VAL_NUMBER_2_NEGATIVE_SCALE,VAL_NUMBER_4_NEGATIVE_SCALE,VAL_NUMBER_9_NEGATIVE_SCALE,VAL_NUMBER_18_NEGATIVE_SCALE,VAL_NUMBER_36_NEGATIVE_SCALE,VAL_DATE,VAL_TS,VAL_TS_PRECISION2,VAL_TS_PRECISION4,VAL_TS_PRECISION9,VAL_TSLTZ from %s.%s ORDER BY ID ASC";

    private static final String PROJECTION_QUERY_ADD_COLUMN1 =
            "select ID,ADD_COLUMN1,ADD_COLUMN2 from %s.%s where ID >=5 ORDER BY ID ASC";

    private static final String PROJECTION_QUERY_ADD_COLUMN2 =
            "select ID,ADD_COLUMN3,ADD_COLUMN4 from %s.%s where ID >=7 ORDER BY ID ASC";

    private static final String PROJECTION_QUERY_ADD_COLUMN3 =
            "select ID,VAL_VARCHAR,VAL_VARCHAR2,VAL_NVARCHAR2,VAL_CHAR,VAL_NCHAR,VAL_BF,VAL_BD,VAL_F,VAL_F_10,VAL_NUM,VAL_DP,VAL_R,VAL_DECIMAL,VAL_NUMERIC,VAL_NUM_VS,VAL_INT,VAL_INTEGER,VAL_SMALLINT,VAL_NUMBER_38_NO_SCALE,VAL_NUMBER_38_SCALE_0,VAL_NUMBER_1,VAL_NUMBER_2,VAL_NUMBER_4,VAL_NUMBER_9,VAL_NUMBER_18,VAL_NUMBER_2_NEGATIVE_SCALE,VAL_NUMBER_4_NEGATIVE_SCALE,VAL_NUMBER_9_NEGATIVE_SCALE,VAL_NUMBER_18_NEGATIVE_SCALE,VAL_NUMBER_36_NEGATIVE_SCALE,VAL_DATE,VAL_TS,VAL_TS_PRECISION2,VAL_TS_PRECISION4,VAL_TS_PRECISION9,VAL_TSLTZ,ADD_COLUMN1,ADD_COLUMN2,ADD_COLUMN3 from %s.%s ORDER BY ID ASC";

    private static final String MYSQL_SINK = "oracle_cdc_2_mysql_sink_table";

    private static final String MYSQL_DATABASE = "oracle_sink";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String MYSQL_CONNECTOR_NAME = "st_user_sink";
    private static final String MYSQL_CONNECTOR_PASSWORD = "mysqlpw";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withEnv("TZ", "Asia/Shanghai")
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String mysqlDriverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Oracle-CDC/lib && cd /tmp/seatunnel/plugins/Oracle-CDC/lib && wget "
                                        + oracleDriverUrl()
                                        + " && mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + mysqlDriverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");

        ORACLE_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));
        log.info("Starting Oracle containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        log.info("Oracle containers are started.");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        ORACLE_CONTAINER.stop();
        MYSQL_CONTAINER.stop();
    }

    @Order(1)
    @TestTemplate
    public void testOracleCdc2OracleWithSchemaEvolutionCase(TestContainer container)
            throws Exception {

        createAndInitialize("full_types", ADMIN_USER, ADMIN_PWD);
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/oraclecdc_to_oracle_with_schema_change.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // Waiting to job running for auto create sink table
        Thread.sleep(10000L);

        assertSchemaEvolution(
                ORACLE_CONTAINER.getJdbcUrl(),
                ORACLE_CONTAINER.getJdbcUrl(),
                SCEHMA_NAME,
                SOURCE_TABLE1 + "_SINK",
                false);
    }

    @Order(2)
    @TestTemplate
    public void testOracleCdc2MysqlWithSchemaEvolutionCase(TestContainer container)
            throws Exception {
        dropTable(ORACLE_CONTAINER.getJdbcUrl(), SCEHMA_NAME, SOURCE_TABLE1);
        dropTable(ORACLE_CONTAINER.getJdbcUrl(), SCEHMA_NAME, SOURCE_TABLE1 + "_SINK");
        createAndInitialize("full_types", ADMIN_USER, ADMIN_PWD);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/oraclecdc_to_mysql_with_schema_change.conf", jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        given().pollDelay(10, TimeUnit.SECONDS)
                .await()
                .pollDelay(5000L, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                        });

        assertSchemaEvolution(
                ORACLE_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_DATABASE,
                MYSQL_SINK,
                true);
    }

    private void assertSchemaEvolution(
            String sourceJdbcUrl,
            String sinkJdbcUrl,
            String sinkSchemaName,
            String sinkTableName,
            boolean oracle2Mysql)
            throws Exception {
        await().atMost(300, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                checkData(
                                        QUERY,
                                        sourceJdbcUrl,
                                        sinkJdbcUrl,
                                        sinkSchemaName,
                                        sinkTableName,
                                        oracle2Mysql));

        // case1 add columns with cdc data at same time
        createAndInitialize("add_columns", CONNECTOR_USER, CONNECTOR_PWD);
        Thread.sleep(40 * 1000);
        // verify the schema: oracle -> oracle
        if (!oracle2Mysql) {
            await().atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    checkSchema(
                                            ORACLE_DESC,
                                            sourceJdbcUrl,
                                            sinkJdbcUrl,
                                            sinkSchemaName,
                                            sinkTableName));
            // verify the data
            with().pollInterval(TWO_SECONDS)
                    .pollDelay(10, TimeUnit.SECONDS)
                    .and()
                    .await()
                    .atMost(20, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                checkData(
                                        PROJECTION_QUERY_ADD_COLUMN3,
                                        sourceJdbcUrl,
                                        sinkJdbcUrl,
                                        sinkSchemaName,
                                        sinkTableName,
                                        oracle2Mysql);
                                // The default value of add_column4 is current_timestamp()，so the
                                // history data of sink table with this column may be different from
                                // the source table because delay of apply schema change.
                                String query =
                                        String.format(
                                                "SELECT t1.id, t1.add_column4, "
                                                        + "t2.id, t2.add_column4, "
                                                        + "ABS(EXTRACT(SECOND FROM (t1.add_column4 - t2.add_column4))) AS time_diff "
                                                        + "FROM %s.%s t1 "
                                                        + "INNER JOIN %s.%s t2 ON t1.id = t2.id",
                                                SCEHMA_NAME,
                                                SOURCE_TABLE1,
                                                SCEHMA_NAME,
                                                sinkTableName);
                                try (Connection jdbcConnection =
                                                getJdbcConnection(
                                                        ORACLE_CONTAINER.getJdbcUrl(),
                                                        CONNECTOR_USER,
                                                        CONNECTOR_PWD);
                                        Statement statement = jdbcConnection.createStatement();
                                        ResultSet resultSet = statement.executeQuery(query); ) {
                                    while (resultSet.next()) {
                                        int timeDiff = resultSet.getInt("time_diff");
                                        Assertions.assertTrue(
                                                timeDiff <= 50,
                                                "Time difference exceeds 50 seconds: "
                                                        + timeDiff
                                                        + " seconds");
                                    }
                                }
                            });
        } else {
            // verify the schema: oracle -> mysql
            await().atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(OracleCDCWithSchemaChangeIT::verifyOracle2MysqlSchema);
            await().atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                checkData(
                                        PROJECTION_QUERY,
                                        sourceJdbcUrl,
                                        sinkJdbcUrl,
                                        sinkSchemaName,
                                        sinkTableName,
                                        oracle2Mysql);
                                checkData(
                                        PROJECTION_QUERY_ADD_COLUMN1,
                                        sourceJdbcUrl,
                                        sinkJdbcUrl,
                                        sinkSchemaName,
                                        sinkTableName,
                                        oracle2Mysql);
                                checkData(
                                        PROJECTION_QUERY_ADD_COLUMN2,
                                        sourceJdbcUrl,
                                        sinkJdbcUrl,
                                        sinkSchemaName,
                                        sinkTableName,
                                        oracle2Mysql);
                            });
        }

        // case2 drop columns with cdc data at same time
        assertCaseByDdlName(
                sourceJdbcUrl,
                sinkJdbcUrl,
                "drop_columns",
                sinkSchemaName,
                sinkTableName,
                oracle2Mysql);

        // case3 change column name with cdc data at same time
        assertCaseByDdlName(
                sourceJdbcUrl,
                sinkJdbcUrl,
                "rename_columns",
                sinkSchemaName,
                sinkTableName,
                oracle2Mysql);

        // case4 modify column data type with cdc data at same time
        assertCaseByDdlName(
                sourceJdbcUrl,
                sinkJdbcUrl,
                "modify_columns",
                sinkSchemaName,
                sinkTableName,
                oracle2Mysql);
    }

    private void assertCaseByDdlName(
            String sourceJdbcUrl,
            String sinkJdbcUrl,
            String ddlSqlName,
            String sinkSchemaname,
            String sinkTable,
            boolean oracle2Mysql)
            throws Exception {
        createAndInitialize(ddlSqlName, CONNECTOR_USER, CONNECTOR_PWD);
        Thread.sleep(10 * 1000);
        assertTableStructureAndData(
                sourceJdbcUrl, sinkJdbcUrl, sinkSchemaname, sinkTable, oracle2Mysql);
    }

    private void assertTableStructureAndData(
            String sourceJdbcUrl,
            String sinkJdbcUrl,
            String sinkSchemaName,
            String sinkTableName,
            boolean oracle2Mysql) {
        // verify the schema: oracle -> oracle
        if (!oracle2Mysql) {
            await().atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    checkSchema(
                                            ORACLE_DESC,
                                            sourceJdbcUrl,
                                            sinkJdbcUrl,
                                            sinkSchemaName,
                                            sinkTableName));
        } else {
            // verify the schema: oracle -> mysql
            await().atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(OracleCDCWithSchemaChangeIT::verifyOracle2MysqlSchema);
        }

        // verify the data
        await().atMost(300, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            checkData(
                                    QUERY,
                                    sourceJdbcUrl,
                                    sinkJdbcUrl,
                                    sinkSchemaName,
                                    sinkTableName,
                                    oracle2Mysql);
                        });
    }

    private static void verifyOracle2MysqlSchema() {
        try (MySqlCatalog mySqlCatalog =
                        new MySqlCatalog(
                                "mysql",
                                MYSQL_CONNECTOR_NAME,
                                MYSQL_CONNECTOR_PASSWORD,
                                JdbcUrlUtil.getUrlInfo(MYSQL_CONTAINER.getJdbcUrl()),
                                null);
                OracleCatalog oracleCatalog =
                        new OracleCatalog(
                                "oracle",
                                CONNECTOR_USER,
                                CONNECTOR_PWD,
                                OracleURLParser.parse(ORACLE_CONTAINER.getJdbcUrl()),
                                null,
                                null)) {
            mySqlCatalog.open();
            oracleCatalog.open();

            CatalogTable mySqlCatalogTable =
                    mySqlCatalog.getTable(TablePath.of(MYSQL_DATABASE, MYSQL_SINK));
            TableSchema sinkTableSchemaInMysql = mySqlCatalogTable.getTableSchema();
            List<Column> sinkColumnsInMysql = sinkTableSchemaInMysql.getColumns();

            CatalogTable oracleCatalogTable =
                    oracleCatalog.getTable(TablePath.of("ORCLCDB", SCEHMA_NAME, SOURCE_TABLE1));
            TableSchema sourceTableSchemaInOracle = oracleCatalogTable.getTableSchema();
            List<Column> sourceColumnsInOracle = sourceTableSchemaInOracle.getColumns();

            MySqlTypeConverter mySqlTypeConverter =
                    new MySqlTypeConverter(
                            org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql
                                    .MySqlVersion.V_8);
            Assertions.assertEquals(sourceColumnsInOracle.size(), sinkColumnsInMysql.size());
            for (int i = 0; i < sourceColumnsInOracle.size(); i++) {
                Column sourceColumn = sourceColumnsInOracle.get(i);
                BasicTypeDefine<MysqlType> typeBasicTypeDefine =
                        mySqlTypeConverter.reconvert(sourceColumn);
                Column sinkColumn = sinkColumnsInMysql.get(i);
                BasicTypeDefine<MysqlType> typeBasicTypeDefine1 =
                        mySqlTypeConverter.reconvert(sinkColumn);
                Assertions.assertEquals(
                        typeBasicTypeDefine.getName(), typeBasicTypeDefine1.getName());
                Assertions.assertEquals(
                        typeBasicTypeDefine.getDataType(), typeBasicTypeDefine1.getDataType());
                Assertions.assertEquals(
                        typeBasicTypeDefine.getNativeType(), typeBasicTypeDefine1.getNativeType());
                Assertions.assertEquals(
                        typeBasicTypeDefine.getLength(), typeBasicTypeDefine1.getLength());
                Assertions.assertEquals(
                        typeBasicTypeDefine.getPrecision(), typeBasicTypeDefine1.getPrecision());
                Assertions.assertEquals(
                        typeBasicTypeDefine.getScale(), typeBasicTypeDefine1.getScale());
                if (!typeBasicTypeDefine1.getDataType().equalsIgnoreCase("datetime")) {
                    Assertions.assertEquals(
                            typeBasicTypeDefine.isNullable(), typeBasicTypeDefine1.isNullable());
                }
                if (StringUtils.isNotBlank(typeBasicTypeDefine.getComment())) {
                    Assertions.assertTrue(
                            typeBasicTypeDefine1
                                    .getComment()
                                    .equalsIgnoreCase(typeBasicTypeDefine.getComment()));
                }
            }
        }
    }

    private void checkData(
            String querySql,
            String sourceJdbcUrl,
            String sinkJdbcUrl,
            String sinkSchemaName,
            String sinkTableName,
            boolean oracle2Mysql) {
        Assertions.assertIterableEquals(
                query(
                        sourceJdbcUrl,
                        String.format(querySql, SCEHMA_NAME, SOURCE_TABLE1),
                        CONNECTOR_USER,
                        CONNECTOR_PWD),
                oracle2Mysql
                        ? query(
                                sinkJdbcUrl,
                                String.format(querySql, sinkSchemaName, sinkTableName),
                                MYSQL_CONNECTOR_NAME,
                                MYSQL_CONNECTOR_PASSWORD)
                        : query(
                                sinkJdbcUrl,
                                String.format(querySql, sinkSchemaName, sinkTableName),
                                CONNECTOR_USER,
                                CONNECTOR_PWD));
    }

    private void checkSchema(
            String querySql,
            String sourceJdbcUrl,
            String sinkJdbcUrl,
            String sinkSchemaName,
            String sinkTableName) {
        Assertions.assertIterableEquals(
                query(
                        sourceJdbcUrl,
                        String.format(querySql, SCEHMA_NAME, SOURCE_TABLE1),
                        CONNECTOR_USER,
                        CONNECTOR_PWD),
                query(
                        sinkJdbcUrl,
                        String.format(querySql, sinkSchemaName, sinkTableName),
                        CONNECTOR_USER,
                        CONNECTOR_PWD));
    }
}
