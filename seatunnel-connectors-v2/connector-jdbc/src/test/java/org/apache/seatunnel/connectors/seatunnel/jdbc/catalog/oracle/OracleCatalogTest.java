package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled("Please Test it in your local environment")
class OracleCatalogTest {
    @Test
    void testCatalog() {
        OracleCatalog catalog =
                new OracleCatalog(
                        "oracle",
                        "test",
                        "oracle",
                        OracleURLParser.parse("jdbc:oracle:thin:@127.0.0.1:1521:xe"),
                        null);

        catalog.open();

        MySqlCatalog mySqlCatalog =
                new MySqlCatalog(
                        "mysql",
                        "root",
                        "root@123",
                        JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:33062/mingdongtest"));

        mySqlCatalog.open();

        CatalogTable table1 =
                mySqlCatalog.getTable(TablePath.of("mingdongtest", "all_types_table_02"));

        List<String> strings = catalog.listDatabases();
        System.out.println(strings);

        List<String> strings1 = catalog.listTables("XE");

        CatalogTable table = catalog.getTable(TablePath.of("XE", "TEST", "PG_TYPES_TABLE_CP1"));

        catalog.createTableInternal(new TablePath("XE", "TEST", "TEST003"), table);
    }
}
