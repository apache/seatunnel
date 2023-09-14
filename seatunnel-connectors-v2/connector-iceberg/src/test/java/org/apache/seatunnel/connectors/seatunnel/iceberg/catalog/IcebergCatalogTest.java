package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TYPE;
import static org.junit.Assert.assertThrows;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled
class IcebergCatalogTest {

    static IcebergCatalogFactory catalogFactory =
            new IcebergCatalogFactory(
                    "iceberg",
                    IcebergCatalogType.HIVE,
                    "warehouse",
                    "thrift://datasource01:9083",
                    "hadoop/liuli@SILENCE.COM",
                    "/Users/liliu/krb5.conf",
                    "/Users/liliu/liuli.keytab",
                    "/Users/liliu/hdfs-site.xml",
                    "/Users/liliu/hive-site.xml");

    static IcebergCatalog icebergCatalog = new IcebergCatalog(catalogFactory, "iceberg");

    static String databaseName = "default";
    static String tableName = "tbl6";

    TablePath tablePath = TablePath.of(databaseName, null, tableName);
    TableIdentifier tableIdentifier = TableIdentifier.of("iceberg", databaseName, null, tableName);

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        icebergCatalog.open();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        icebergCatalog.close();
    }

    @Test
    @Order(1)
    void getDefaultDatabase() {
        Assertions.assertEquals(icebergCatalog.getDefaultDatabase(), databaseName);
    }

    @Test
    @Order(2)
    void createDatabase() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    icebergCatalog.createDatabase(tablePath, true);
                });
    }

    @Test
    @Order(3)
    void databaseExists() {
        Assertions.assertTrue(icebergCatalog.databaseExists(databaseName));
        Assertions.assertFalse(icebergCatalog.databaseExists("sssss"));
    }

    @Test
    @Order(3)
    void listDatabases() {
        icebergCatalog.listDatabases().forEach(System.out::println);
        Assertions.assertTrue(icebergCatalog.listDatabases().contains(databaseName));
    }

    @Test
    @Order(5)
    void createTable() {
        CatalogTable catalogTable = buildAllTypesTable(tableIdentifier);
        icebergCatalog.createTable(tablePath, catalogTable, true);
        Assertions.assertTrue(icebergCatalog.tableExists(tablePath));
    }

    @Test
    @Order(5)
    void listTables() {
        Assertions.assertTrue(icebergCatalog.listTables(databaseName).contains(tableName));
    }

    @Test
    @Order(7)
    void tableExists() {
        Assertions.assertTrue(icebergCatalog.tableExists(tablePath));
        Assertions.assertFalse(icebergCatalog.tableExists(TablePath.of(databaseName, "ssssss")));
    }

    @Test
    @Order(8)
    void getTable() {
        CatalogTable table = icebergCatalog.getTable(tablePath);
        CatalogTable templateTable = buildAllTypesTable(tableIdentifier);
        Assertions.assertEquals(table.toString(), templateTable.toString());
    }

    @Test
    @Order(9)
    void dropTable() {
        icebergCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(icebergCatalog.tableExists(tablePath));
    }

    @Test
    @Order(10)
    void dropDatabase() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    icebergCatalog.dropDatabase(tablePath, true);
                });
    }

    CatalogTable buildAllTypesTable(TableIdentifier tableIdentifier) {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(
                PhysicalColumn.of("id", BasicType.INT_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "boolean_col", BasicType.BOOLEAN_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "integer_col", BasicType.INT_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "long_col", BasicType.LONG_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "float_col", BasicType.FLOAT_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "double_col", BasicType.DOUBLE_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of("date_col", LOCAL_DATE_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "timestamp_col", LOCAL_DATE_TIME_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of("string_col", STRING_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "binary_col",
                        PrimitiveByteArrayType.INSTANCE,
                        null,
                        true,
                        null,
                        "test comment"));
        builder.column(
                PhysicalColumn.of(
                        "decimal_col", new DecimalType(38, 18), null, true, null, "test comment"));
        builder.column(PhysicalColumn.of("dt_col", STRING_TYPE, null, true, null, "test comment"));

        TableSchema schema = builder.build();
        HashMap<String, String> options = new HashMap<>();

        return CatalogTable.of(
                tableIdentifier, schema, options, Collections.singletonList("dt_col"), "null");
    }
}
