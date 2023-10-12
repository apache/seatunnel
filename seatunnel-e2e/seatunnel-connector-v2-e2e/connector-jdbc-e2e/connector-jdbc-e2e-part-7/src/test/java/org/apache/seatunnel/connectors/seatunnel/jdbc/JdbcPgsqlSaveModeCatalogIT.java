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
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class JdbcPgsqlSaveModeCatalogIT extends TestSuiteBase implements TestResource {

    // mysql config
    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    private static final String MYSQL_IMAGE = "mysql:latest";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "auto";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3310;
    private MySQLContainer<?> mysql_container;
    static JdbcUrlUtil.UrlInfo MysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:mysql://localhost:3310/auto?useSSL=false");
    // pgsql
    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PG_JDBC_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-jdbc/2.5.1/postgis-jdbc-2.5.1.jar";
    private static final String PG_GEOMETRY_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-geometry/2.5.1/postgis-geometry-2.5.1.jar";
    private static final String USERNAME = "testUser";
    private static final String DATABASE = "TESTUSER";
    private static final String PASSWORD = "Abc!@#135_seatunnel";
    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;
    static JdbcUrlUtil.UrlInfo pgUrl =
            JdbcUrlUtil.getUrlInfo("jdbc:postgresql://localhost:54325/pg");

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS mysql_auto_create\n"
                    + "(\n  "
                    + "`id` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "  `f_binary` binary(64) DEFAULT NULL,\n"
                    + "  `f_smallint` smallint(6) DEFAULT NULL,\n"
                    + "  `f_smallint_unsigned` smallint(5) unsigned DEFAULT NULL,\n"
                    + "  `f_mediumint` mediumint(9) DEFAULT NULL,\n"
                    + "  `f_mediumint_unsigned` mediumint(8) unsigned DEFAULT NULL,\n"
                    + "  `f_int` int(11) DEFAULT NULL,\n"
                    + "  `f_int_unsigned` int(10) unsigned DEFAULT NULL,\n"
                    + "  `f_integer` int(11) DEFAULT NULL,\n"
                    + "  `f_integer_unsigned` int(10) unsigned DEFAULT NULL,\n"
                    + "  `f_bigint` bigint(20) DEFAULT NULL,\n"
                    + "  `f_bigint_unsigned` bigint(20) unsigned DEFAULT NULL,\n"
                    + "  `f_numeric` decimal(10,0) DEFAULT NULL,\n"
                    + "  `f_decimal` decimal(10,0) DEFAULT NULL,\n"
                    + "  `f_float` float DEFAULT NULL,\n"
                    + "  `f_double` double DEFAULT NULL,\n"
                    + "  `f_double_precision` double DEFAULT NULL,\n"
                    + "  `f_tinytext` tinytext COLLATE utf8mb4_unicode_ci,\n"
                    + "  `f_varchar` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                    + "  `f_datetime` datetime DEFAULT NULL,\n"
                    + "  `f_timestamp` timestamp NULL DEFAULT NULL,\n"
                    + "  `f_bit1` bit(1) DEFAULT NULL,\n"
                    + "  `f_bit64` bit(64) DEFAULT NULL,\n"
                    + "  `f_char` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                    + "  `f_enum` enum('enum1','enum2','enum3') COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                    + "  `f_real` double DEFAULT NULL,\n"
                    + "  `f_tinyint` tinyint(4) DEFAULT NULL,\n"
                    + "  `f_bigint8` bigint(8) DEFAULT NULL,\n"
                    + "  `f_bigint1` bigint(1) DEFAULT NULL,\n"
                    + "  `f_data` date DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ");";

    private String getInsertSql =
            "INSERT INTO mysql_auto_create"
                    + "(id, f_binary, f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_tinytext, f_varchar, f_datetime, f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_real, f_tinyint, f_bigint8, f_bigint1, f_data)\n"
                    + "VALUES(575, 0x654458436C70336B7357000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, 194, 549, 633, 835, 719, 253, 742, 265, 806, 736, 474, 254, 120.8, 476.42, 264.95, 'In other words, Navicat provides the ability for data in different databases and/or schemas to be kept up-to-date so that each repository contains the same information.', 'jF9X70ZqH4', '2011-10-20 23:10:08', '2017-09-10 19:33:51', 1, b'0001001101100000001010010100010111000010010110110101110011111100', 'u', 'enum2', 876.55, 25, 503, 1, '2011-03-06');\n";

    private String customSql =
            "INSERT INTO public.pgsql_auto_create_sink"
                    + "(id, f_binary, f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_tinytext, f_varchar, f_datetime, f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_real, f_tinyint, f_bigint8, f_bigint1, f_data)\n"
                    + "VALUES(575, '0x654458436C70336B7357000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 194, 549, 633, 835, 719, 253, 742, 265, 806, 736, 474, 254, 120.8, 476.42, 264.95, 'In other words, Navicat provides the ability for data in different databases and/or schemas to be kept up-to-date so that each repository contains the same information.', 'jF9X70ZqH4', '2011-10-20 23:10:08', '2017-09-10 19:33:51', '1', '0001001101100000001010010100010111000010010110110101110011111100', 'u', 'enum2', 876.55, 25, 503, 1, '2011-03-06');\n";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + MYSQL_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    void initContainer() throws ClassNotFoundException {
        // ============= mysql
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);
        mysql_container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        mysql_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, 3306)));
        // ============= PG
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withDatabaseName("pg")
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        POSTGRESQL_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", 54325, 5432)));
        log.info("PostgreSQL container started");
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        log.info("pg data initialization succeeded. Procedure");
        Startables.deepStart(Stream.of(mysql_container, POSTGRESQL_CONTAINER)).join();
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        initContainer();
        initializeJdbcTable();
    }

    @TestTemplate
    public void testCatalog(TestContainer container) throws IOException, InterruptedException {
        TablePath tablePathMySql = TablePath.of("auto", "mysql_auto_create");
        TablePath tablePathPgsql_Sink = TablePath.of("pg", "public", "pgsql_auto_create_sink");
        MySqlCatalog mySqlCatalog = new MySqlCatalog("mysql", "root", MYSQL_PASSWORD, MysqlUrlInfo);
        final PostgresCatalog postgresCatalog =
                new PostgresCatalog("Postgres", USERNAME, PASSWORD, pgUrl, "public");
        postgresCatalog.open();
        mySqlCatalog.open();
        CatalogTable catalogTable = mySqlCatalog.getTable(tablePathMySql);
        // sink tableExists ?
        boolean tableExistsBefore = postgresCatalog.tableExists(tablePathPgsql_Sink);
        Assertions.assertFalse(tableExistsBefore);
        // create table
        postgresCatalog.createTable(tablePathPgsql_Sink, catalogTable, true);
        boolean tableExistsAfter = postgresCatalog.tableExists(tablePathPgsql_Sink);
        Assertions.assertTrue(tableExistsAfter);
        // isExistsData ?
        boolean existsDataBefore = postgresCatalog.isExistsData(tablePathPgsql_Sink);
        Assertions.assertFalse(existsDataBefore);
        // insert one data
        postgresCatalog.executeSql(tablePathPgsql_Sink, customSql);
        boolean existsDataAfter = postgresCatalog.isExistsData(tablePathPgsql_Sink);
        Assertions.assertTrue(existsDataAfter);
        // truncateTable
        postgresCatalog.truncateTable(tablePathPgsql_Sink, true);
        Assertions.assertFalse(postgresCatalog.isExistsData(tablePathPgsql_Sink));
        // drop table
        postgresCatalog.dropTable(tablePathPgsql_Sink, true);
        Assertions.assertFalse(postgresCatalog.tableExists(tablePathPgsql_Sink));
        mySqlCatalog.close();
        postgresCatalog.close();
    }

    @Override
    public void tearDown() throws Exception {
        mysql_container.close();
        POSTGRESQL_CONTAINER.close();
    }

    private Connection getJdbcMySqlConnection() throws SQLException {
        return DriverManager.getConnection(
                mysql_container.getJdbcUrl(),
                mysql_container.getUsername(),
                mysql_container.getPassword());
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcMySqlConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CREATE_TABLE_SQL);
            statement.execute(getInsertSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Mysql table failed!", e);
        }
    }
}
