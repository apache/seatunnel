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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DuckDBCatalogTest {

    private static final String DATABASE_NAME = "test_db";
    private static final String TABLE_NAME = "test_table";
    private static final String CATALOG_NAME = "duckdb";
    private static final String DB_FILE = "test.db";

    private DuckDBCatalog catalog;
    private String jdbcUrl;

    @BeforeEach
    public void setUp() throws Exception {
        // Delete existing database file if it exists
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        // Setup JDBC connection
        jdbcUrl = "jdbc:duckdb:" + DB_FILE;
        // Create catalog instance

        catalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, "main");

    }

    @AfterEach
    public void tearDown() {
        // Delete database file
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    public void testDatabaseExists() {
        Assertions.assertTrue(catalog.databaseExists(DATABASE_NAME));
        Assertions.assertFalse(catalog.databaseExists("non_existing_db"));
    }

    @Test
    public void testTableExists() {
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        Assertions.assertTrue(catalog.tableExists(tablePath));
        Assertions.assertFalse(
                catalog.tableExists(TablePath.of(DATABASE_NAME, "non_existing_table")));
    }

    @Test
    public void testGetTable() {
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        CatalogTable table = catalog.getTable(tablePath);

        Assertions.assertEquals(
                TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME), table.getTableId());

        TableSchema schema = table.getTableSchema();
        List<Column> expectedColumns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, null),
                        PhysicalColumn.of("name", BasicType.STRING_TYPE, 1, true, null, null),
                        PhysicalColumn.of("age", BasicType.INT_TYPE, 2, true, null, null),
                        PhysicalColumn.of("salary", new DecimalType(10, 2), 3, true, null, null),
                        PhysicalColumn.of("is_active", BasicType.BOOLEAN_TYPE, 4, true, null, null),
                        PhysicalColumn.of(
                                "created_at",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                5,
                                true,
                                null,
                                null));

        List<Column> actualColumns = schema.getColumns();
        Assertions.assertEquals(expectedColumns.size(), actualColumns.size());
        for (int i = 0; i < expectedColumns.size(); i++) {
            Column expected = expectedColumns.get(i);
            Column actual = actualColumns.get(i);
            Assertions.assertEquals(expected.getName(), actual.getName());
            Assertions.assertEquals(expected.getDataType(), actual.getDataType());
            Assertions.assertEquals(expected.isNullable(), actual.isNullable());
        }
    }

    @Test
    public void testCreateAndDropDatabase() {
        String newDatabase = "new_database";
        Assertions.assertFalse(catalog.databaseExists(newDatabase));

        TablePath databasePath = TablePath.of(newDatabase);
        catalog.createDatabase(databasePath, false);
        Assertions.assertTrue(catalog.databaseExists(newDatabase));

        catalog.dropDatabase(databasePath, false);
        Assertions.assertFalse(catalog.databaseExists(newDatabase));
    }

    @Test
    public void testCreateAndDropTable() {
        TablePath tablePath = TablePath.of(DATABASE_NAME, "new_table");
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, null),
                        PhysicalColumn.of("name", BasicType.STRING_TYPE, 1, true, null, null));

        TableSchema schema = TableSchema.builder().columns(columns).build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, "new_table"),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Test table",
                        CATALOG_NAME);

        Assertions.assertFalse(catalog.tableExists(tablePath));

        catalog.createTable(tablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(tablePath));

        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));
    }
}
