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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently testcase does not depend on a specific engine, but needs to be started with the engine")
public class JdbcMySqlCreateTableIT extends TestSuiteBase implements TestResource {
    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver";
    private static final int SQLSERVER_CONTAINER_PORT = 14333;
    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PG_JDBC_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-jdbc/2.5.1/postgis-jdbc-2.5.1.jar";
    private static final String PG_GEOMETRY_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-geometry/2.5.1/postgis-geometry-2.5.1.jar";

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "auto";

    private static final String MYSQL_USERNAME = "root";
    private static final String PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 33061;
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String USERNAME = "testUser";

    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    private MSSQLServerContainer<?> sqlserver_container;
    private MySQLContainer<?> mysql_container;

    private static final String mysqlCheck =
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'auto' AND table_name = 'mysql_auto_create_mysql') AS table_exists";
    private static final String sqlserverCheck =
            "IF EXISTS (\n"
                    + "    SELECT 1\n"
                    + "    FROM testauto.sys.tables t\n"
                    + "    JOIN testauto.sys.schemas s ON t.schema_id = s.schema_id\n"
                    + "    WHERE t.name = 'mysql_auto_create_sql' AND s.name = 'dbo'\n"
                    + ")\n"
                    + "    SELECT 1 AS table_exists;\n"
                    + "ELSE\n"
                    + "    SELECT 0 AS table_exists;";
    private static final String pgCheck =
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'mysql_auto_create_pg') AS table_exists;\n";

    String driverSqlServerUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    private static final String CREATE_SQL_DATABASE =
            "IF NOT EXISTS (\n"
                    + "   SELECT name \n"
                    + "   FROM sys.databases \n"
                    + "   WHERE name = N'testauto'\n"
                    + ")\n"
                    + "CREATE DATABASE testauto;\n";

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

    @TestContainerExtension
    private final ContainerExtendedFactory extendedSqlServerFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR
                                        + " && curl -O "
                                        + PG_JDBC_JAR
                                        + " && curl -O "
                                        + PG_GEOMETRY_JAR
                                        + " && curl -O "
                                        + MYSQL_DRIVER_CLASS
                                        + " && curl -O "
                                        + driverSqlserverUrl()
                                        + " && curl -O "
                                        + driverMySqlUrl());
                //                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    String driverMySqlUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    String driverSqlserverUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    void initContainer() throws ClassNotFoundException {
        DockerImageName imageName = DockerImageName.parse(SQLSERVER_IMAGE);
        sqlserver_container =
                new MSSQLServerContainer<>(imageName)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .withPassword(PASSWORD)
                        .acceptLicense()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));

        sqlserver_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", SQLSERVER_CONTAINER_PORT, 1433)));

        try {
            Class.forName(sqlserver_container.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.DRIVER_NOT_FOUND, "Not found suitable driver for mssql", e);
        }

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
                Lists.newArrayList(String.format("%s:%s", 54323, 5432)));

        log.info("PostgreSQL container started");
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());

        log.info("pg data initialization succeeded. Procedure");
        DockerImageName mysqlImageName = DockerImageName.parse(MYSQL_IMAGE);
        mysql_container =
                new MySQLContainer<>(mysqlImageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        mysql_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, 3306)));
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER, sqlserver_container, mysql_container))
                .join();
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        initContainer();
        initializeSqlJdbcTable();
        initializeJdbcTable();
    }

    static JdbcUrlUtil.UrlInfo sqlParse =
            SqlServerURLParser.parse("jdbc:sqlserver://localhost:14333;database=testauto");
    static JdbcUrlUtil.UrlInfo MysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:mysql://localhost:33061/auto?useSSL=false");
    static JdbcUrlUtil.UrlInfo pg = JdbcUrlUtil.getUrlInfo("jdbc:postgresql://localhost:54323/pg");

    @Test
    public void testAutoCreateTable() {
        TablePath tablePathMySql = TablePath.of("auto", "mysql_auto_create");
        TablePath tablePathMySql_Mysql = TablePath.of("auto", "mysql_auto_create_mysql");
        TablePath tablePathSQL = TablePath.of("testauto", "dbo", "mysql_auto_create_sql");
        TablePath tablePathPG = TablePath.of("pg", "public", "mysql_auto_create_pg");

        SqlServerCatalog sqlServerCatalog =
                new SqlServerCatalog("sqlserver", "sa", PASSWORD, sqlParse, "dbo", null);
        MySqlCatalog mySqlCatalog = new MySqlCatalog("mysql", "root", PASSWORD, MysqlUrlInfo, null);
        PostgresCatalog postgresCatalog =
                new PostgresCatalog("postgres", "testUser", PASSWORD, pg, "public", null);

        mySqlCatalog.open();
        sqlServerCatalog.open();
        postgresCatalog.open();

        CatalogTable mysqlTable = mySqlCatalog.getTable(tablePathMySql);

        sqlServerCatalog.createTable(tablePathSQL, mysqlTable, true);
        postgresCatalog.createTable(tablePathPG, mysqlTable, true);
        mySqlCatalog.createTable(tablePathMySql_Mysql, mysqlTable, true);

        Assertions.assertTrue(checkMysql(mysqlCheck));
        Assertions.assertTrue(checkSqlServer(sqlserverCheck));
        Assertions.assertTrue(checkPG(pgCheck));

        // delete table
        log.info("delete table");
        mySqlCatalog.dropTable(tablePathMySql_Mysql, true);
        sqlServerCatalog.dropTable(tablePathSQL, true);
        postgresCatalog.dropTable(tablePathPG, true);
        mySqlCatalog.dropTable(tablePathMySql, true);

        sqlServerCatalog.close();
        mySqlCatalog.close();
        postgresCatalog.close();
        // delete table
    }

    @Test
    public void testGetCatalogTablePrimaryKeyFromQuery() throws SQLException {
        try (Connection connection = getJdbcMySqlConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE IF NOT EXISTS mysql_pk_e2e(\n"
                                + "id int NOT NULL PRIMARY KEY,\n"
                                + "name varchar(100) NULL\n"
                                + ");");
            }

            JdbcDialectTypeMapper typeMapper =
                    new JdbcDialectTypeMapper() {
                        @Override
                        public org.apache.seatunnel.api.table.catalog.Column mappingColumn(
                                org.apache.seatunnel.api.table.converter.BasicTypeDefine
                                        typeDefine) {
                            return org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                    typeDefine.getName(),
                                    org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE,
                                    typeDefine.getLength(),
                                    typeDefine.isNullable(),
                                    typeDefine.getScale(),
                                    typeDefine.getComment());
                        }
                    };

            CatalogTable catalogTable =
                    CatalogUtils.getCatalogTable(
                            connection,
                            "select id, name from mysql_pk_e2e where id >= 0",
                            typeMapper);

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            Assertions.assertNotNull(primaryKey);
            Assertions.assertTrue(primaryKey.getColumnNames().contains("id"));

            Set<String> columnNames =
                    catalogTable.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toSet());
            Assertions.assertTrue(columnNames.contains("id"));
            Assertions.assertTrue(columnNames.contains("name"));
        }
    }

    @Test
    public void testGetCatalogTablePrimaryKeyFromGroupByQuery() throws SQLException {
        try (Connection connection = getJdbcMySqlConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE IF NOT EXISTS orders_group_by_e2e("
                                + "id INT NOT NULL PRIMARY KEY,"
                                + "order_date DATE,"
                                + "total_amount DECIMAL(10,2)"
                                + ")");
                statement.execute(
                        "INSERT INTO orders_group_by_e2e(id, order_date, total_amount) VALUES "
                                + "(1,'2023-01-01',100.00),"
                                + "(2,'2023-01-02',50.00),"
                                + "(3,'2023-02-01',30.00)");
            }

            JdbcDialectTypeMapper typeMapper =
                    new JdbcDialectTypeMapper() {
                        @Override
                        public org.apache.seatunnel.api.table.catalog.Column mappingColumn(
                                org.apache.seatunnel.api.table.converter.BasicTypeDefine
                                        typeDefine) {
                            return org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                    typeDefine.getName(),
                                    org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE,
                                    typeDefine.getLength(),
                                    typeDefine.isNullable(),
                                    typeDefine.getScale(),
                                    typeDefine.getComment());
                        }
                    };

            String sql =
                    "SELECT id, COUNT(*) AS order_cnt "
                            + "FROM orders_group_by_e2e "
                            + "WHERE order_date >= '2023-01-01' "
                            + "GROUP BY id";

            CatalogTable catalogTable = CatalogUtils.getCatalogTable(connection, sql, typeMapper);

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            Assertions.assertNotNull(primaryKey);
            Assertions.assertEquals(1, primaryKey.getColumnNames().size());
            Assertions.assertEquals("id", primaryKey.getColumnNames().get(0));

            Set<String> columnNames =
                    catalogTable.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toSet());
            Assertions.assertTrue(columnNames.contains("id"));
            Assertions.assertTrue(columnNames.contains("order_cnt"));
        }
    }

    @Test
    public void testGetCatalogTablePrimaryKeyFromJoinQuery() throws SQLException {
        try (Connection connection = getJdbcMySqlConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE IF NOT EXISTS users_join_e2e("
                                + "id INT NOT NULL PRIMARY KEY,"
                                + "user_name VARCHAR(100),"
                                + "city VARCHAR(100)"
                                + ")");
                statement.execute(
                        "CREATE TABLE IF NOT EXISTS orders_join_e2e("
                                + "order_id INT NOT NULL PRIMARY KEY,"
                                + "user_id INT,"
                                + "order_date DATE,"
                                + "total_amount DECIMAL(10,2)"
                                + ")");
                statement.execute(
                        "INSERT INTO users_join_e2e(id, user_name, city) VALUES "
                                + "(1,'user1','Beijing'),"
                                + "(2,'user2','Shanghai')");
                statement.execute(
                        "INSERT INTO orders_join_e2e(order_id, user_id, order_date, total_amount) VALUES "
                                + "(100,1,'2023-01-01',100.00)");
            }

            JdbcDialectTypeMapper typeMapper =
                    new JdbcDialectTypeMapper() {
                        @Override
                        public org.apache.seatunnel.api.table.catalog.Column mappingColumn(
                                org.apache.seatunnel.api.table.converter.BasicTypeDefine
                                        typeDefine) {
                            return org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                    typeDefine.getName(),
                                    org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE,
                                    typeDefine.getLength(),
                                    typeDefine.isNullable(),
                                    typeDefine.getScale(),
                                    typeDefine.getComment());
                        }
                    };

            String sql =
                    "SELECT o.order_id, u.id, u.user_name, u.city "
                            + "FROM orders_join_e2e o "
                            + "INNER JOIN users_join_e2e u ON o.user_id = u.id "
                            + "WHERE o.order_date >= '2023-01-01'";

            CatalogTable catalogTable = CatalogUtils.getCatalogTable(connection, sql, typeMapper);

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            // complex join query should still infer primary key from main table
            Assertions.assertNotNull(primaryKey);
            Assertions.assertEquals(1, primaryKey.getColumnNames().size());
            Assertions.assertEquals("order_id", primaryKey.getColumnNames().get(0));

            Set<String> columnNames =
                    catalogTable.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toSet());
            Assertions.assertTrue(columnNames.contains("order_id"));
            Assertions.assertTrue(columnNames.contains("id"));
            Assertions.assertTrue(columnNames.contains("user_name"));
            Assertions.assertTrue(columnNames.contains("city"));
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (sqlserver_container != null) {
            sqlserver_container.close();
            dockerClient.removeContainerCmd(sqlserver_container.getContainerId()).exec();
        }
        if (mysql_container != null) {
            mysql_container.close();
            dockerClient.removeContainerCmd(mysql_container.getContainerId()).exec();
        }
        if (POSTGRESQL_CONTAINER != null) {
            POSTGRESQL_CONTAINER.close();
            dockerClient.removeContainerCmd(POSTGRESQL_CONTAINER.getContainerId()).exec();
        }
    }

    private Connection getJdbcSqlServerConnection() throws SQLException {
        return DriverManager.getConnection(
                sqlserver_container.getJdbcUrl(),
                sqlserver_container.getUsername(),
                sqlserver_container.getPassword());
    }

    private Connection getJdbcMySqlConnection() throws SQLException {
        return DriverManager.getConnection(
                mysql_container.getJdbcUrl(),
                mysql_container.getUsername(),
                mysql_container.getPassword());
    }

    private Connection getJdbcPgConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRESQL_CONTAINER.getJdbcUrl(),
                POSTGRESQL_CONTAINER.getUsername(),
                POSTGRESQL_CONTAINER.getPassword());
    }

    private void initializeSqlJdbcTable() {
        try (Connection connection = getJdbcSqlServerConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CREATE_SQL_DATABASE);
            //            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcMySqlConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CREATE_TABLE_SQL);
            statement.execute(getInsertSql);

            //            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private boolean checkMysql(String sql) {
        try (Connection connection = getJdbcMySqlConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getBoolean(1);
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkPG(String sql) {
        try (Connection connection = getJdbcPgConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getBoolean(1);
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkSqlServer(String sql) {
        try (Connection connection = getJdbcSqlServerConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getInt(1) == 1;
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
