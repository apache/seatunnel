/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.container;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect.log;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS)
public class SqlServerCatalogTest extends AbstractSqlServerContainerTest {

    @Test
    public void testConnection() throws Exception {
        try (Connection conn = getConnection()) {
            assertTrue(conn.isValid(5));
            System.out.println("SQL Server container is running at: " + getJdbcUrl());
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = "test_table_" + System.currentTimeMillis();
        createTestTable(tableName);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            rs.next();
            assertEquals(0, rs.getInt(1));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testSqlServerCatalog() {
        SqlServerCatalog catalog = createSqlServerCatalog();

        assertNotNull(catalog);
    }

    @Test
    public void testCatalogOperations() throws SQLException {
        SqlServerCatalog catalog = createSqlServerCatalog();
        catalog.open();

        String tableName = "test_catalog_table";
        TablePath tablePath = TablePath.of("master", "dbo", tableName);

        createTestTable(tableName);
        assertTrue(catalog.listDatabases().contains("master"));
        assertTrue(catalog.tableExists(tablePath));

        CatalogTable table = catalog.getTable(tablePath);
        assertNotNull(table);
        assertEquals(tableName, table.getTableId().getTableName());

        catalog.close();
    }

    @Test
    public void testSecondaryDatabaseOperations() {
        String schema = "dbo";
        String secondaryDatabaseName = "secondary_db";

        SqlServerCatalog catalog = createSqlServerCatalog();
        catalog.open();

        try {
            Assertions.assertFalse(
                    catalog.databaseExists(secondaryDatabaseName),
                    "Secondary database should not exist initially");

            TablePath secondaryDbPath = TablePath.of(secondaryDatabaseName, schema, "dummy");
            catalog.createDatabase(secondaryDbPath, false);

            Assertions.assertTrue(
                    catalog.databaseExists(secondaryDatabaseName),
                    "Secondary database should exist after creation");

            catalog.dropDatabase(secondaryDbPath, false);

            Assertions.assertFalse(
                    catalog.databaseExists(secondaryDatabaseName),
                    "Secondary database should not exist after deletion");

            log.info("Secondary database creation/deletion test passed");

        } finally {
            try {
                TablePath cleanupPath = TablePath.of(secondaryDatabaseName, schema, "dummy");
                if (catalog.databaseExists(secondaryDatabaseName)) {
                    catalog.dropDatabase(cleanupPath, true);
                    log.info("Cleaned up secondary database in finally block");
                }
            } catch (Exception e) {
                log.warn("Error during secondary database cleanup", e);
            }

            catalog.close();
        }
    }

    @Test
    public void testCatalogSaveMode() throws SQLException {
        String schema = "dbo";
        String databaseName = "master";
        String testTableName = "test_boundary_comments_table";

        TablePath sourceTablePath = TablePath.of(databaseName, schema, testTableName + "_source");
        TablePath sinkTablePath = TablePath.of(databaseName, schema, testTableName + "_sink");

        SqlServerCatalog catalog =
                new SqlServerCatalog(
                        "sqlserver", getUsername(), getPassword(), getJdbcUrlInfo(), schema, null);
        catalog.open();

        try {
            // 1. Create source table with complex comment
            String createSourceTableSQL =
                    "CREATE TABLE "
                            + sourceTablePath.getFullName()
                            + " (\n"
                            + "  id INT IDENTITY(1,1) PRIMARY KEY,\n"
                            + "  uniqueidentifier_col UNIQUEIDENTIFIER,\n"
                            + "  text_col TEXT,\n"
                            + "  varchar_col VARCHAR(255),\n"
                            + "  nvarchar_col NVARCHAR(255),\n"
                            + "  complex_comment_col VARCHAR(100)\n"
                            + ");\n"
                            + "EXEC sp_addextendedproperty \n"
                            + "    @name = N'MS_Description', \n"
                            + "    @value = N'\"#¥%……&*（）;;'',,.\\.``````//''@特殊注释''\\\\''\"', \n"
                            + "    @level0type = N'Schema', @level0name = 'dbo', \n"
                            + "    @level1type = N'Table',  @level1name = '"
                            + testTableName
                            + "_source', \n"
                            + "    @level2type = N'Column', @level2name = 'complex_comment_col';";

            executeSql(createSourceTableSQL);

            Assertions.assertTrue(
                    catalog.tableExists(sourceTablePath),
                    "Source table should exist after creation");

            // 2. Get source table schema
            CatalogTable sourceCatalogTable = catalog.getTable(sourceTablePath);
            Assertions.assertNotNull(sourceCatalogTable, "Source CatalogTable should not be null");

            // 3. Verify comment in source table
            String expectedComment = "\"#¥%……&*（）;;',,.\\.``````//'@特殊注释'\\\\'\"";
            String actualSourceComment =
                    sourceCatalogTable.getTableSchema().getColumns().stream()
                            .filter(col -> "complex_comment_col".equals(col.getName()))
                            .findFirst()
                            .map(Column::getComment)
                            .orElse(null);

            Assertions.assertEquals(
                    expectedComment,
                    actualSourceComment,
                    "Source table comment should be equals to inserted one");

            // 4. Create sink table from source schema
            boolean tableExistsBefore = catalog.tableExists(sinkTablePath);
            Assertions.assertFalse(tableExistsBefore, "Sink table should not exist initially");

            catalog.createTable(sinkTablePath, sourceCatalogTable, true);

            boolean tableExistsAfter = catalog.tableExists(sinkTablePath);
            Assertions.assertTrue(
                    tableExistsAfter, "Sink table should exist after creation with SaveMode");

            // 5. Verify comment was preserved in sink table
            CatalogTable sinkCatalogTable = catalog.getTable(sinkTablePath);
            Assertions.assertNotNull(sinkCatalogTable, "Sink CatalogTable should not be null");

            String actualSinkComment =
                    sinkCatalogTable.getTableSchema().getColumns().stream()
                            .filter(col -> "complex_comment_col".equals(col.getName()))
                            .findFirst()
                            .map(Column::getComment)
                            .orElse(null);

            Assertions.assertEquals(
                    expectedComment,
                    actualSinkComment,
                    "Complex comments should be preserved exactly as they are in the sink table.");

            Assertions.assertEquals(
                    actualSourceComment,
                    actualSinkComment,
                    "Comments should be the same between the source and the sink.");

            // 6. Check data exists (should be false initially)
            boolean existsDataBefore = catalog.isExistsData(sinkTablePath);
            Assertions.assertFalse(
                    existsDataBefore, "The sink table should not contain any data initially.");

            // 7. Insert data into sink table
            // Note: Need to specify all columns including the IDENTITY column
            String insertSQL =
                    "INSERT INTO "
                            + sinkTablePath.getFullName()
                            + " "
                            + "(id, uniqueidentifier_col, text_col, varchar_col, nvarchar_col, complex_comment_col) "
                            + "VALUES "
                            + "(1, NEWID(), 'Test text', 'Test varchar', N'Test nvarchar', 'dummy value');";

            catalog.executeSql(sinkTablePath, insertSQL);

            // 8. Verify data was inserted
            boolean existsDataAfter = catalog.isExistsData(sinkTablePath);
            Assertions.assertTrue(
                    existsDataAfter, "The sink table should contain data after insert");

            // 9. Truncate table
            catalog.truncateTable(sinkTablePath, true);

            boolean existsDataAfterTruncate = catalog.isExistsData(sinkTablePath);
            Assertions.assertFalse(
                    existsDataAfterTruncate,
                    "The sink table should not contain any data after truncate");

            // 10. Drop sink table
            catalog.dropTable(sinkTablePath, true);
            Assertions.assertFalse(
                    catalog.tableExists(sinkTablePath),
                    "The sink table should not exist after drop");

            // 11. Clean up source table
            executeSql("DROP TABLE IF EXISTS " + sourceTablePath.getFullName());

            System.out.println("Test SaveMode & complex data passed!!");

        } catch (Exception e) {
            // Clean up on failure
            try {
                executeSql("DROP TABLE IF EXISTS " + sourceTablePath.getFullName());
                executeSql("DROP TABLE IF EXISTS " + sinkTablePath.getFullName());
            } catch (SQLException ex) {
                // Ignore cleanup errors
            }
            throw new RuntimeException("Test Failed: " + e.getMessage(), e);
        } finally {
            catalog.close();
        }
    }

    @Test
    public void testCatalogIndexes() throws SQLException {
        String schema = "dbo";
        String databaseName = "master";

        SqlServerCatalog catalog =
                new SqlServerCatalog(
                        "sqlserver", getUsername(), getPassword(), getJdbcUrlInfo(), schema, null);
        catalog.open();

        try {
            String testTableName = "test_indexes_table";
            TablePath tablePath = TablePath.of(databaseName, schema, testTableName);

            String createTableSQL =
                    "CREATE TABLE "
                            + tablePath.getFullName()
                            + " (\n"
                            + "  id INT IDENTITY(1,1) PRIMARY KEY,\n"
                            + "  name VARCHAR(100) NOT NULL,\n"
                            + "  email VARCHAR(255),\n"
                            + "  age INTEGER,\n"
                            + "  created_at DATETIME DEFAULT GETDATE()\n"
                            + ")";

            executeSql(createTableSQL);

            Assertions.assertTrue(
                    catalog.tableExists(tablePath), "Test table should exist after creation");

            CatalogTable initialTable = catalog.getTable(tablePath);
            Assertions.assertNotNull(initialTable, "CatalogTable should not be null");

            TableSchema initialSchema = initialTable.getTableSchema();
            PrimaryKey initialPrimaryKey = initialSchema.getPrimaryKey();

            Assertions.assertNotNull(
                    initialPrimaryKey, "Primary Key should not be null after table creation");
            Assertions.assertEquals(
                    "id",
                    initialPrimaryKey.getColumnNames().get(0),
                    "Primary Key should be on column 'id'");
            System.out.println("Primary Key identified: " + initialPrimaryKey.getPrimaryKey());

            String createIndex1SQL =
                    "CREATE INDEX idx_test_name ON " + tablePath.getFullName() + "(name)";
            executeSql(createIndex1SQL);

            String createIndex2SQL =
                    "CREATE INDEX idx_test_email_age ON "
                            + tablePath.getFullName()
                            + "(email, age)";
            executeSql(createIndex2SQL);

            String createUniqueIndexSQL =
                    "CREATE UNIQUE NONCLUSTERED INDEX idx_unique_email ON "
                            + tablePath.getFullName()
                            + "(email) WHERE email IS NOT NULL";
            executeSql(createUniqueIndexSQL);

            CatalogTable tableWithIndexes = catalog.getTable(tablePath);
            TableSchema schemaWithIndexes = tableWithIndexes.getTableSchema();

            PrimaryKey primaryKeyAfter = schemaWithIndexes.getPrimaryKey();
            Assertions.assertNotNull(
                    primaryKeyAfter, "Primary Key should not be null after index creation");
            Assertions.assertEquals(
                    "id",
                    primaryKeyAfter.getColumnNames().get(0),
                    "Primary Key should remain on column 'id'");

            List<ConstraintKey> constraintKeys = schemaWithIndexes.getConstraintKeys();
            Assertions.assertFalse(
                    constraintKeys.isEmpty(),
                    "ConstraintKeys list should not be empty after index creation");

            System.out.println("Found " + constraintKeys.size() + " constraint keys");

            boolean foundNameIndex = false;
            boolean foundEmailAgeIndex = false;
            boolean foundUniqueEmailIndex = false;

            for (ConstraintKey constraintKey : constraintKeys) {
                String constraintName = constraintKey.getConstraintName();
                List<ConstraintKey.ConstraintKeyColumn> columns = constraintKey.getColumnNames();

                System.out.println(
                        "  - Constraint: "
                                + constraintName
                                + ", Type: "
                                + constraintKey.getConstraintType()
                                + ", Columns: "
                                + columns.stream()
                                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                                        .collect(Collectors.toList()));

                if ("idx_test_name".equalsIgnoreCase(constraintName)
                        && columns.size() == 1
                        && "name".equals(columns.get(0).getColumnName())) {
                    foundNameIndex = true;
                }

                if ("idx_test_email_age".equalsIgnoreCase(constraintName)
                        && columns.size() == 2
                        && "email".equals(columns.get(0).getColumnName())
                        && "age".equals(columns.get(1).getColumnName())) {
                    foundEmailAgeIndex = true;
                }

                if ("idx_unique_email".equalsIgnoreCase(constraintName)
                        && constraintKey.getConstraintType()
                                == ConstraintKey.ConstraintType.UNIQUE_KEY
                        && columns.size() == 1
                        && "email".equals(columns.get(0).getColumnName())) {
                    foundUniqueEmailIndex = true;
                }
            }

            Assertions.assertTrue(
                    foundNameIndex, "Should find index 'idx_test_name' on column 'name'");
            Assertions.assertTrue(
                    foundEmailAgeIndex,
                    "Should find index 'idx_test_email_age' on columns 'email, age'");
            Assertions.assertTrue(
                    foundUniqueEmailIndex,
                    "Should find unique index 'idx_unique_email' on column 'email'");

            System.out.println("All indexes correctly identified by catalog");

            boolean hasIndexResult = hasIndex(catalog, tablePath);
            Assertions.assertTrue(
                    hasIndexResult,
                    "hasIndex() should return true when table has Primary Key and indexes");

            System.out.println("hasIndex() correctly returns true");

            executeSql("DROP TABLE " + tablePath.getFullName());
            Assertions.assertFalse(
                    catalog.tableExists(tablePath), "Table should not exist after drop");

            System.out.println("Index test completed successfully");

        } catch (Exception e) {
            System.err.println("❌ Error in index test: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            catalog.close();
        }
    }

    private boolean hasIndex(Catalog catalog, TablePath tablePath) {
        TableSchema tableSchema = catalog.getTable(tablePath).getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();

        if (primaryKey != null
                && org.apache.commons.lang3.StringUtils.isNotBlank(primaryKey.getPrimaryKey())) {
            return true;
        }

        return !constraintKeys.isEmpty();
    }

    @Test
    public void testCatalogDataTypes() throws SQLException {
        String tableName = "test_comprehensive_data_types";
        TablePath tablePath = TablePath.of("master", "dbo", tableName);

        String createTableSQL =
                "CREATE TABLE "
                        + tablePath.getFullName()
                        + " (\n"
                        + "  id INT IDENTITY(1,1) PRIMARY KEY,\n"
                        + "  uniqueidentifier_col UNIQUEIDENTIFIER,\n"
                        + "  text_col TEXT,\n"
                        + "  varchar_col VARCHAR(255),\n"
                        + "  nvarchar_col NVARCHAR(255),\n"
                        + "  char_col CHAR(10),\n"
                        + "  nchar_col NCHAR(10),\n"
                        + "  bit_col BIT,\n"
                        + "  tinyint_col TINYINT,\n"
                        + "  smallint_col SMALLINT,\n"
                        + "  int_col INT,\n"
                        + "  bigint_col BIGINT,\n"
                        + "  decimal_col DECIMAL(10, 2),\n"
                        + "  numeric_col NUMERIC(8, 4),\n"
                        + "  float_col FLOAT,\n"
                        + "  real_col REAL,\n"
                        + "  date_col DATE,\n"
                        + "  datetime_col DATETIME,\n"
                        + "  datetime2_col DATETIME2,\n"
                        + "  datetimeoffset_col DATETIMEOFFSET,\n"
                        + "  smalldatetime_col SMALLDATETIME,\n"
                        + "  time_col TIME,\n"
                        + "  xml_col XML,\n"
                        + "  varbinary_col VARBINARY(MAX),\n"
                        + "  binary_col BINARY(50)\n"
                        + ");\n"
                        + "EXEC sp_addextendedproperty \n"
                        + "    @name = N'MS_Description', \n"
                        + "    @value = N'UNIQUEIDENTIFIER column comment', \n"
                        + "    @level0type = N'Schema', @level0name = 'dbo', \n"
                        + "    @level1type = N'Table',  @level1name = '"
                        + tableName
                        + "', \n"
                        + "    @level2type = N'Column', @level2name = 'uniqueidentifier_col';";

        executeSql(createTableSQL);

        SqlServerCatalog catalog = createSqlServerCatalog();
        catalog.open();

        try {
            CatalogTable catalogTable = catalog.getTable(tablePath);
            Assertions.assertNotNull(catalogTable, "CatalogTable should not be null");

            TableSchema tableSchema = catalogTable.getTableSchema();
            List<Column> columns = tableSchema.getColumns();

            System.out.println("Validating SQL Server to SeaTunnel data type mappings...");

            for (Column column : columns) {
                System.out.println(
                        "Column: "
                                + column.getName()
                                + ", Type: "
                                + column.getDataType()
                                + ", Comment: "
                                + column.getComment());
            }

            Map<String, Column> columnMap =
                    columns.stream()
                            .collect(Collectors.toMap(Column::getName, Function.identity()));

            Column uniqueidentifierColumn = columnMap.get("uniqueidentifier_col");
            Assertions.assertNotNull(uniqueidentifierColumn, "uniqueidentifier_col should exist");
            String uuidTypeStr = uniqueidentifierColumn.getDataType().toString();
            System.out.println("UNIQUEIDENTIFIER column type: " + uuidTypeStr);
            Assertions.assertTrue(
                    uuidTypeStr.contains("STRING")
                            || uuidTypeStr.contains("VARCHAR")
                            || uuidTypeStr.contains("TEXT"),
                    "UNIQUEIDENTIFIER should map to string-like type. Got: " + uuidTypeStr);

            Column xmlColumn = columnMap.get("xml_col");
            Assertions.assertNotNull(xmlColumn, "xml_col should exist");
            String xmlTypeStr = xmlColumn.getDataType().toString();
            System.out.println("XML type: " + xmlTypeStr);
            Assertions.assertTrue(
                    xmlTypeStr.contains("STRING")
                            || xmlTypeStr.contains("VARCHAR")
                            || xmlTypeStr.contains("TEXT"),
                    "XML should map to string-like type. Got: " + xmlTypeStr);

            Column datetimeoffsetColumn = columnMap.get("datetimeoffset_col");
            Assertions.assertNotNull(datetimeoffsetColumn, "datetimeoffset_col should exist");
            String datetimeoffsetTypeStr = datetimeoffsetColumn.getDataType().toString();
            System.out.println("DATETIMEOFFSET type: " + datetimeoffsetTypeStr);
            Assertions.assertTrue(
                    datetimeoffsetTypeStr.contains("TIMESTAMP")
                            || datetimeoffsetTypeStr.contains("DATETIME"),
                    "DATETIMEOFFSET should map to timestamp type. Got: " + datetimeoffsetTypeStr);

            Column bitColumn = columnMap.get("bit_col");
            Assertions.assertNotNull(bitColumn, "bit_col should exist");
            String bitTypeStr = bitColumn.getDataType().toString();
            System.out.println("BIT type: " + bitTypeStr);
            Assertions.assertTrue(
                    bitTypeStr.contains("BOOLEAN") || bitTypeStr.contains("BOOL"),
                    "BIT should map to boolean type. Got: " + bitTypeStr);

            Column varbinaryColumn = columnMap.get("varbinary_col");
            Assertions.assertNotNull(varbinaryColumn, "varbinary_col should exist");
            String varbinaryTypeStr = varbinaryColumn.getDataType().toString();
            System.out.println("VARBINARY type: " + varbinaryTypeStr);
            Assertions.assertTrue(
                    varbinaryTypeStr.contains("BYTES") || varbinaryTypeStr.contains("BINARY"),
                    "VARBINARY should map to binary type. Got: " + varbinaryTypeStr);

            Column nvarcharColumn = columnMap.get("nvarchar_col");
            Assertions.assertNotNull(nvarcharColumn, "nvarchar_col should exist");
            String nvarcharTypeStr = nvarcharColumn.getDataType().toString();
            System.out.println("NVARCHAR type: " + nvarcharTypeStr);

            Column decimalColumn = columnMap.get("decimal_col");
            Assertions.assertNotNull(decimalColumn, "decimal_col should exist");
            String decimalTypeStr = decimalColumn.getDataType().toString();
            System.out.println("DECIMAL(10,2) type: " + decimalTypeStr);

            Assertions.assertTrue(
                    decimalTypeStr.toUpperCase().contains("DECIMAL")
                            || decimalTypeStr.toUpperCase().contains("NUMERIC"),
                    "DECIMAL should map to decimal/numeric type. Got: " + decimalTypeStr);

            Assertions.assertEquals(25, columns.size(), "Should have 25 columns in the table");

            Assertions.assertEquals(
                    "UNIQUEIDENTIFIER column comment",
                    uniqueidentifierColumn.getComment(),
                    "Column comment should be preserved");

            PrimaryKey primaryKey = tableSchema.getPrimaryKey();
            Assertions.assertNotNull(primaryKey, "Primary Key should exist");
            Assertions.assertEquals(
                    "id",
                    primaryKey.getColumnNames().get(0),
                    "Primary Key should be on 'id' column");

            System.out.println("\nAll SQL Server data types mapped correctly to SeaTunnel types");

            testDataRoundTrip(catalog, tablePath);

        } finally {
            catalog.close();
            executeSql("DROP TABLE IF EXISTS " + tablePath.getFullName());
        }
    }

    private void testDataRoundTrip(SqlServerCatalog catalog, TablePath tablePath)
            throws SQLException {
        System.out.println("\nTesting data round-trip for critical types...");

        String enableIdentityInsert = "SET IDENTITY_INSERT " + tablePath.getFullName() + " ON;";

        String insertSQL =
                enableIdentityInsert
                        + " INSERT INTO "
                        + tablePath.getFullName()
                        + " (\n"
                        + "  id, uniqueidentifier_col, text_col, varchar_col, nvarchar_col,\n"
                        + "  char_col, nchar_col, bit_col, tinyint_col, smallint_col,\n"
                        + "  int_col, bigint_col, decimal_col, numeric_col, float_col,\n"
                        + "  real_col, date_col, datetime_col, datetime2_col, datetimeoffset_col,\n"
                        + "  smalldatetime_col, time_col, xml_col, varbinary_col, binary_col\n"
                        + ") VALUES (\n"
                        + "  1, NEWID(), 'Sample text', 'varchar value', N'unicode text',\n"
                        + "  'CHAR', N'NCHAR', 1, 255, 32767,\n"
                        + "  2147483647, 9223372036854775807, 1234.56, 12.3456, 123.456,\n"
                        + "  3.14, '2023-12-25', '2023-12-25 14:30:00', '2023-12-25 14:30:00.123', '2023-12-25 14:30:00.123 +00:00',\n"
                        + "  '2023-12-25 14:30:00', '14:30:00', '<note><to>Test</to></note>', 0x123456, 0xABCDEF\n"
                        + ");\n"
                        + "SET IDENTITY_INSERT "
                        + tablePath.getFullName()
                        + " OFF;";

        catalog.executeSql(tablePath, insertSQL);

        boolean hasData = catalog.isExistsData(tablePath);
        Assertions.assertTrue(hasData, "Table should have data after insert");

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs =
                        stmt.executeQuery("SELECT COUNT(*) FROM " + tablePath.getFullName())) {

            rs.next();
            int count = rs.getInt(1);
            Assertions.assertEquals(1, count, "Should have 1 row in table");

            ResultSet dataRs =
                    stmt.executeQuery(
                            "SELECT TOP 1 uniqueidentifier_col, datetimeoffset_col, xml_col, varbinary_col FROM "
                                    + tablePath.getFullName());

            if (dataRs.next()) {
                String uniqueidentifier = dataRs.getString(1);
                Timestamp datetimeoffset = dataRs.getTimestamp(2);
                String xml = dataRs.getString(3);
                byte[] varbinary = dataRs.getBytes(4);

                Assertions.assertNotNull(uniqueidentifier, "UNIQUEIDENTIFIER should not be null");
                Assertions.assertNotNull(datetimeoffset, "DATETIMEOFFSET should not be null");
                Assertions.assertNotNull(xml, "XML should not be null");
                Assertions.assertNotNull(varbinary, "VARBINARY should not be null");

                System.out.println("Data round-trip successful:");
                System.out.println("  UNIQUEIDENTIFIER: " + uniqueidentifier);
                System.out.println("  DATETIMEOFFSET: " + datetimeoffset);
                System.out.println(
                        "  XML: " + xml.substring(0, Math.min(50, xml.length())) + "...");
                System.out.println("  VARBINARY: " + varbinary.length + " bytes");
            }
        }

        catalog.truncateTable(tablePath, true);
        Assertions.assertFalse(
                catalog.isExistsData(tablePath), "Table should be empty after truncate");
    }
}
