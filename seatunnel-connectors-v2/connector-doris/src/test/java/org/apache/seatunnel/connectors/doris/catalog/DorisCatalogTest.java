package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

class DorisCatalogTest {

    private static DorisCatalog dorisCatalog = null;

    @BeforeAll
    static void beforeAll() {

        String catalogName = "doris";
        String frontEndHosts = "10.16.10.6:8939";
        Integer queryPort = 9939;
        String username = "root";
        String password = "";

        if (dorisCatalog == null) {
            dorisCatalog =
                    new DorisCatalog(catalogName, frontEndHosts, queryPort, username, password);
            dorisCatalog.open();
        }
    }

    @AfterAll
    static void afterAll() {

        if (dorisCatalog != null) {
            dorisCatalog.close();
        }
    }

    @Test
    void databaseExists() {

        boolean res1 = dorisCatalog.databaseExists("test");
        boolean res2 = dorisCatalog.databaseExists("test1");

        Assertions.assertTrue(res1);
        Assertions.assertFalse(res2);
    }

    @Test
    void listDatabases() {

        List<String> databases = dorisCatalog.listDatabases();
        Assertions.assertEquals(databases.size(), 3);
    }

    @Test
    void listTables() {

        List<String> tables = dorisCatalog.listTables("test");
        Assertions.assertEquals(tables.size(), 15);
    }

    @Test
    void tableExists() {

        boolean res = dorisCatalog.tableExists(TablePath.of("test", "t1"));
        Assertions.assertTrue(res);
    }

    @Test
    void getTable() {

        CatalogTable table = dorisCatalog.getTable(TablePath.of("test", "t1"));
        Assertions.assertEquals(table.getTableId(), TableIdentifier.of("doris", "test", "t1"));
        Assertions.assertEquals(table.getTableSchema().getColumns().size(), 3);
    }

    @Test
    void createTable() {

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> dorisCatalog.createTable(TablePath.of("test", "test"), null, false));
    }

    @Test
    void dropTable() {

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> dorisCatalog.dropTable(TablePath.of("test", "test"), false));
    }

    @Test
    void createDatabase() {

        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), false));
        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), true));
        Assertions.assertThrows(
                CatalogException.class,
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), false));
    }

    @Test
    void dropDatabase() {

        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), false));
        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), true));
        Assertions.assertThrows(
                CatalogException.class,
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), false));
    }
}
