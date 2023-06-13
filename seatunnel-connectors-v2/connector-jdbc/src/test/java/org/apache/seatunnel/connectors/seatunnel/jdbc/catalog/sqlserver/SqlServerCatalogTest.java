package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("Please Test it in your local environment")
class SqlServerCatalogTest {

    static JdbcUrlUtil.UrlInfo sqlParse =
            SqlServerURLParser.parse("jdbc:sqlserver://127.0.0.1:1434;database=TestDB");
    static JdbcUrlUtil.UrlInfo MysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:33061/liuliTest?useSSL=false");
    static JdbcUrlUtil.UrlInfo pg =
            JdbcUrlUtil.getUrlInfo("jdbc:postgresql://127.0.0.1:5432/liulitest");
    static TablePath tablePathSQL;
    static TablePath tablePathMySql;
    static TablePath tablePathPG;
    static TablePath tablePathOracle;
    private static String databaseName = "TestDB";
    private static String schemaName = "dbo";
    private static String tableName = "AllDataTest";

    static SqlServerCatalog sqlServerCatalog;
    static MySqlCatalog mySqlCatalog;
    static PostgresCatalog postgresCatalog;

    static CatalogTable postgresCatalogTable;
    static CatalogTable mySqlCatalogTable;
    static CatalogTable sqlServerCatalogTable;

    @BeforeAll
    static void before() {
        tablePathSQL = TablePath.of(databaseName, schemaName, "sqlserver_to_sqlserver");
        tablePathMySql = TablePath.of(databaseName, schemaName, "mysql_to_sqlserver");
        tablePathPG = TablePath.of(databaseName, schemaName, "pg_to_sqlserver");
        tablePathOracle = TablePath.of(databaseName, schemaName, "oracle_to_sqlserver");
        sqlServerCatalog = new SqlServerCatalog("sqlserver", "sa", "root@123", sqlParse, null);
        mySqlCatalog = new MySqlCatalog("mysql", "root", "root@123", MysqlUrlInfo);
        postgresCatalog = new PostgresCatalog("postgres", "postgres", "postgres", pg, null);
        mySqlCatalog.open();
        sqlServerCatalog.open();
        postgresCatalog.open();
    }

    @Test
    void listDatabases() {
        List<String> list = sqlServerCatalog.listDatabases();
    }

    @Test
    void listTables() {
        List<String> list = sqlServerCatalog.listTables(databaseName);
    }

    @Test
    void tableExists() {

        //        boolean b = sqlServerCatalog.tableExists(tablePath);
    }

    @Test
    @Order(1)
    void getTable() {
        postgresCatalogTable =
                postgresCatalog.getTable(
                        TablePath.of("liulitest", "public", "pg_types_table_no_array"));
        mySqlCatalogTable = mySqlCatalog.getTable(TablePath.of("liuliTest", "AllTypeCol"));
        sqlServerCatalogTable =
                sqlServerCatalog.getTable(TablePath.of("TestDB", "dbo", "AllDataTest"));
    }

    @Test
    @Order(2)
    void createTableInternal() {
        sqlServerCatalog.createTable(tablePathMySql, mySqlCatalogTable, true);
        sqlServerCatalog.createTable(tablePathPG, postgresCatalogTable, true);
        sqlServerCatalog.createTable(tablePathSQL, sqlServerCatalogTable, true);
    }

    @Disabled
    // Manually dropping tables
    @Test
    void dropTableInternal() {
        sqlServerCatalog.dropTable(tablePathSQL, true);
        sqlServerCatalog.dropTable(tablePathMySql, true);
        sqlServerCatalog.dropTable(tablePathPG, true);
    }

    @Test
    void createDatabaseInternal() {}

    @Test
    void dropDatabaseInternal() {}

    @AfterAll
    static void after() {
        sqlServerCatalog.close();
        mySqlCatalog.close();
        postgresCatalog.close();
    }
}
