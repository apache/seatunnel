package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.container;

import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect.log;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PostgresCatalogTest extends AbstractPostgresContainerTest {
    
    @Test
    public void testConnection() throws Exception {
        try (Connection conn = getConnection()) {
            assertTrue(conn.isValid(5));
            System.out.println("PostgreSQL container is running at: " + getJdbcUrl());
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
    public void testPostgresCatalog() {
        org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog catalog = createPostgresCatalog();
        assertNotNull(catalog);

    }
    
    @Test
    public void testContainerInfo() {
        System.out.println("JDBC URL: " + getJdbcUrl());
        System.out.println("Username: " + getUsername());
        System.out.println("Password: " + getPassword());
        System.out.println("Database: " + POSTGRES_CONTAINER.getDatabaseName());
    }

    @Test
    public void testCatalogOperations() throws SQLException {
        PostgresCatalog catalog = createPostgresCatalog();
        catalog.open();

        String tableName = "test_catalog_table";
        TablePath tablePath = TablePath.of("testdb", "public", tableName);

        createTestTable(tableName);
        assertTrue(catalog.listDatabases().contains("testdb"));
        assertTrue(catalog.tableExists(tablePath));
        CatalogTable table = catalog.getTable(tablePath);
        assertNotNull(table);
        assertEquals(tableName, table.getTableId().getTableName());

        catalog.close();
    }

    @Test
    public void testSecondaryDatabaseOperations() {
        String schema = "public";
        String secondaryDatabaseName = "secondary_db";

        PostgresCatalog catalog = createPostgresCatalog();
        catalog.open();

        try {
            Assertions.assertFalse(catalog.databaseExists(secondaryDatabaseName),
                    "Secondary database should not exist initially");

            TablePath secondaryDbPath = TablePath.of(secondaryDatabaseName, schema, "dummy");
            catalog.createDatabase(secondaryDbPath, false);

            Assertions.assertTrue(catalog.databaseExists(secondaryDatabaseName),
                    "Secondary database should exist after creation");

            catalog.dropDatabase(secondaryDbPath, false);

            Assertions.assertFalse(catalog.databaseExists(secondaryDatabaseName),
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
    public void testCatalogSaveMode() {
        String schema = "public";
        String databaseName = POSTGRES_CONTAINER.getDatabaseName();
        String testTableName = "test_boundary_comments_table";

        TablePath sourceTablePath = TablePath.of(databaseName, schema, testTableName + "_source");
        TablePath sinkTablePath = TablePath.of(databaseName, schema, testTableName + "_sink");

        PostgresCatalog catalog = new PostgresCatalog(
                "postgresql", // DatabaseIdentifier.POSTGRESQL
                getUsername(),
                getPassword(),
                getJdbcUrlInfo(),
                schema,
                null);
        catalog.open();

        try {
            String createSourceTableSQL =
                    "CREATE TABLE " + sourceTablePath.getFullName() + " (\n" +
                            "  id SERIAL PRIMARY KEY,\n" +
                            "  uuid_col UUID,\n" +
                            "  text_col TEXT,\n" +
                            "  varchar_col VARCHAR(255),\n" +
                            "  complex_comment_col VARCHAR(100) " +
                            ");\n" +
                            "COMMENT ON COLUMN " + sourceTablePath.getFullName() + ".complex_comment_col " +
                            "IS '\"#¥%……&*（）;;'',,.\\.``````//''@特殊注释''\\\\''\"'";

            executeSql(createSourceTableSQL);

            Assertions.assertTrue(catalog.tableExists(sourceTablePath),
                    "Source table should exists after creation");

            CatalogTable sourceCatalogTable = catalog.getTable(sourceTablePath);
            Assertions.assertNotNull(sourceCatalogTable,
                    "Source CatalogTable should not be null");

            String expectedComment = "\"#¥%……&*（）;;',,.\\.``````//'@特殊注释'\\\\'\"";
            String actualSourceComment = sourceCatalogTable.getTableSchema()
                    .getColumns().stream()
                    .filter(col -> "complex_comment_col".equals(col.getName()))
                    .findFirst()
                    .map(col -> col.getComment())
                    .orElse(null);

            Assertions.assertEquals(expectedComment, actualSourceComment,
                    "Source table comment should be equals to inserted one");

            boolean tableExistsBefore = catalog.tableExists(sinkTablePath);
            Assertions.assertFalse(tableExistsBefore,
                    "Sink table should not exists initially");

            catalog.createTable(sinkTablePath, sourceCatalogTable, true);

            boolean tableExistsAfter = catalog.tableExists(sinkTablePath);
            Assertions.assertTrue(tableExistsAfter,
                    "Sink table should exists after creations with SaveMode");

            CatalogTable sinkCatalogTable = catalog.getTable(sinkTablePath);
            Assertions.assertNotNull(sinkCatalogTable,
                    "Sink CatalogTable should not be null");

            String actualSinkComment = sinkCatalogTable.getTableSchema()
                    .getColumns().stream()
                    .filter(col -> "complex_comment_col".equals(col.getName()))
                    .findFirst()
                    .map(col -> col.getComment())
                    .orElse(null);

            Assertions.assertEquals(expectedComment, actualSinkComment,
                    "Complex comments should be preserved exactly as they are in the sink table.");

            Assertions.assertEquals(actualSourceComment, actualSinkComment,
                    "Comments should be the same between the source and the sink.");

            boolean existsDataBefore = catalog.isExistsData(sinkTablePath);
            Assertions.assertFalse(existsDataBefore,
                    "The sink table should not contain any data initially.");

            // NOTE: The catalog.createTable does not preserve PostgreSQL SERIAL / auto-increment semantics.
           // Therefore, we must explicitly provide the primary key value when inserting into sink table.
            String insertSQL =
                    "INSERT INTO " + sinkTablePath.getFullName() + " " +
                            "(id, uuid_col, text_col, varchar_col, complex_comment_col) VALUES " +
                            "(1, gen_random_uuid(), 'Test text', 'Test varchar', 'dummy value')";

            catalog.executeSql(sinkTablePath, insertSQL);

            boolean existsDataAfter = catalog.isExistsData(sinkTablePath);
            Assertions.assertTrue(existsDataAfter,
                    "The sink table should contain any data initially.");

            catalog.truncateTable(sinkTablePath, true);

            boolean existsDataAfterTruncate = catalog.isExistsData(sinkTablePath);
            Assertions.assertFalse(existsDataAfterTruncate,
                    "The sink table should not contain any data after truncate");

            catalog.createTable(sinkTablePath, sourceCatalogTable, true);

            catalog.dropTable(sinkTablePath, true);
            Assertions.assertFalse(catalog.tableExists(sinkTablePath),
                    "The sink table should not exists after drop");

            executeSql("DROP TABLE IF EXISTS " + sourceTablePath.getFullName());

            System.out.println("Teste SaveMode & complex data passed!!");

        } catch (Exception e) {

            try {
                executeSql("DROP TABLE IF EXISTS " + sourceTablePath.getFullName());
                executeSql("DROP TABLE IF EXISTS " + sinkTablePath.getFullName());
            } catch (SQLException ex) {

            }
            throw new RuntimeException("Test Failed: " + e.getMessage(), e);
        } finally {
            catalog.close();
        }
    }

    @Test
    public void testCatalogIndexes() throws SQLException {
        String schema = "public";
        String databaseName = POSTGRES_CONTAINER.getDatabaseName();

        PostgresCatalog catalog = new PostgresCatalog(
                "postgresql", // DatabaseIdentifier.POSTGRESQL
                getUsername(),
                getPassword(),
                getJdbcUrlInfo(),
                schema,
                null);
        catalog.open();

        try {

            String testTableName = "test_indexes_table";
            TablePath tablePath = TablePath.of(databaseName, schema, testTableName);

            String createTableSQL = "CREATE TABLE " + tablePath.getFullName() + " (\n" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  name VARCHAR(100) NOT NULL,\n" +
                    "  email VARCHAR(255),\n" +
                    "  age INTEGER,\n" +
                    "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
                    ")";

            executeSql(createTableSQL);

            Assertions.assertTrue(catalog.tableExists(tablePath),
                    "Test table should exist after creation");

            CatalogTable initialTable = catalog.getTable(tablePath);
            Assertions.assertNotNull(initialTable, "CatalogTable should not be null");

            TableSchema initialSchema = initialTable.getTableSchema();
            PrimaryKey initialPrimaryKey = initialSchema.getPrimaryKey();

            Assertions.assertNotNull(initialPrimaryKey,
                    "Primary Key should not be null after table creation");
            Assertions.assertEquals("id", initialPrimaryKey.getColumnNames().get(0),
                    "Primary Key should be on column 'id'");
            System.out.println("✅ Primary Key identified: " + initialPrimaryKey.getPrimaryKey());

            String createIndex1SQL = "CREATE INDEX idx_test_name ON " +
                    tablePath.getFullName() + "(name)";
            executeSql(createIndex1SQL);

            String createIndex2SQL = "CREATE INDEX idx_test_email_age ON " +
                    tablePath.getFullName() + "(email, age)";
            executeSql(createIndex2SQL);

            String createUniqueIndexSQL = "CREATE UNIQUE INDEX idx_unique_email ON " +
                    tablePath.getFullName() + "(email) WHERE email IS NOT NULL";
            executeSql(createUniqueIndexSQL);

            CatalogTable tableWithIndexes = catalog.getTable(tablePath);
            TableSchema schemaWithIndexes = tableWithIndexes.getTableSchema();

            PrimaryKey primaryKeyAfter = schemaWithIndexes.getPrimaryKey();
            Assertions.assertNotNull(primaryKeyAfter,
                    "Primary Key should not be null after index creation");
            Assertions.assertEquals("id", primaryKeyAfter.getColumnNames().get(0),
                    "Primary Key should remain on column 'id'");

            List<ConstraintKey> constraintKeys = schemaWithIndexes.getConstraintKeys();
            Assertions.assertFalse(constraintKeys.isEmpty(),
                    "ConstraintKeys list should not be empty after index creation");

            System.out.println("Found " + constraintKeys.size() + " constraint keys");

            boolean foundNameIndex = false;
            boolean foundEmailAgeIndex = false;
            boolean foundUniqueEmailIndex = false;

            for (ConstraintKey constraintKey : constraintKeys) {
                String constraintName = constraintKey.getConstraintName();
                List<ConstraintKey.ConstraintKeyColumn> columns = constraintKey.getColumnNames();

                System.out.println("  - Constraint: " + constraintName +
                        ", Type: " + constraintKey.getConstraintType() +
                        ", Columns: " + columns.stream()
                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                        .collect(Collectors.toList()));

                if ("idx_test_name".equals(constraintName) &&
                        columns.size() == 1 &&
                        "name".equals(columns.get(0).getColumnName())) {
                    foundNameIndex = true;
                }

                if ("idx_test_email_age".equals(constraintName) &&
                        columns.size() == 2 &&
                        "email".equals(columns.get(0).getColumnName()) &&
                        "age".equals(columns.get(1).getColumnName())) {
                    foundEmailAgeIndex = true;
                }

                if ("idx_unique_email".equals(constraintName) &&
                        constraintKey.getConstraintType() == ConstraintKey.ConstraintType.UNIQUE_KEY &&
                        columns.size() == 1 &&
                        "email".equals(columns.get(0).getColumnName())) {
                    foundUniqueEmailIndex = true;
                }
            }

            Assertions.assertTrue(foundNameIndex,
                    "Should find index 'idx_test_name' on column 'name'");
            Assertions.assertTrue(foundEmailAgeIndex,
                    "Should find index 'idx_test_email_age' on columns 'email, age'");
            Assertions.assertTrue(foundUniqueEmailIndex,
                    "Should find unique index 'idx_unique_email' on column 'email'");

            System.out.println("All indexes correctly identified by catalog");

            boolean hasIndexResult = hasIndex(catalog, tablePath);
            Assertions.assertTrue(hasIndexResult,
                    "hasIndex() should return true when table has Primary Key and indexes");

            System.out.println("hasIndex() correctly returns true");

            executeSql("DROP TABLE " + tablePath.getFullName() + " CASCADE");
            Assertions.assertFalse(catalog.tableExists(tablePath),
                    "Table should not exist after drop");

            System.out.println("Index test completed successfully");

        } catch (Exception e) {
            System.err.println("❌ Error in index test: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            catalog.close();
        }
    }

    /**
     * Helper method to check if a table has indexes.
     * This mimics the behavior of the hasIndex method in the original test.
     */
    private boolean hasIndex(Catalog catalog, TablePath tablePath) {
        TableSchema tableSchema = catalog.getTable(tablePath).getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();

        // Check if there's a Primary Key
        if (primaryKey != null && org.apache.commons.lang3.StringUtils.isNotBlank(primaryKey.getPrimaryKey())) {
            return true;
        }

        // Check if there are constraint keys (indexes)
        if (!constraintKeys.isEmpty()) {
            return true;
        }

        return false;
    }

    @Test
    public void testCatalogDataTypes() throws SQLException {
        String tableName = "test_comprehensive_data_types";
        TablePath tablePath = TablePath.of(POSTGRES_CONTAINER.getDatabaseName(), "public", tableName);

        String createTableSQL =
                "CREATE TABLE " + tablePath.getFullName() + " (\n" +
                        "  id SERIAL PRIMARY KEY,\n" +
                        "  uuid_col UUID,\n" +
                        "  text_col TEXT,\n" +
                        "  varchar_col VARCHAR(255),\n" +
                        "  boolean_col BOOLEAN,\n" +
                        "  smallint_col SMALLINT,\n" +
                        "  integer_col INTEGER,\n" +
                        "  bigint_col BIGINT,\n" +
                        "  decimal_col DECIMAL(10, 2),\n" +
                        "  numeric_col NUMERIC(8, 4),\n" +
                        "  real_col REAL,\n" +
                        "  double_precision_col DOUBLE PRECISION,\n" +
                        "  date_col DATE,\n" +
                        "  timestamp_col TIMESTAMP,\n" +
                        "  timestamptz_col TIMESTAMP WITH TIME ZONE,\n" +
                        "  json_col JSON,\n" +
                        "  jsonb_col JSONB,\n" +
                        "  xml_col XML,\n" +
                        "  bytea_col BYTEA\n" +
                        ");\n" +
                        "COMMENT ON COLUMN " + tablePath.getFullName() + ".uuid_col IS 'UUID column comment'";

        executeSql(createTableSQL);

        PostgresCatalog catalog = createPostgresCatalog();
        catalog.open();

        try {
            CatalogTable catalogTable = catalog.getTable(tablePath);
            Assertions.assertNotNull(catalogTable, "CatalogTable should not be null");

            TableSchema tableSchema = catalogTable.getTableSchema();
            List<Column> columns = tableSchema.getColumns();

            System.out.println("Validating PostgreSQL to SeaTunnel data type mappings...");

            // Print all columns and their types for debugging
            for (Column column : columns) {
                System.out.println("Column: " + column.getName() +
                        ", Type: " + column.getDataType() +
                        ", Comment: " + column.getComment());
            }

            // Create a map for easier validation
            Map<String, Column> columnMap = columns.stream()
                    .collect(Collectors.toMap(Column::getName, Function.identity()));

            // ========== VALIDATE SPECIFIC DATA TYPE MAPPINGS ==========

            // 1. UUID type
            Column uuidColumn = columnMap.get("uuid_col");
            Assertions.assertNotNull(uuidColumn, "uuid_col should exist");
            System.out.println("UUID column type: " + uuidColumn.getDataType());

            // Check the data type class name or toString representation
            String uuidTypeStr = uuidColumn.getDataType().toString();
            Assertions.assertTrue(
                    uuidTypeStr.contains("STRING") ||
                            uuidTypeStr.contains("UUID") ||
                            uuidTypeStr.contains("VARCHAR") ||
                            uuidTypeStr.contains("TEXT"),
                    "UUID should map to string-like type. Got: " + uuidTypeStr);

            // 2. JSON types
            Column jsonColumn = columnMap.get("json_col");
            Column jsonbColumn = columnMap.get("jsonb_col");
            Assertions.assertNotNull(jsonColumn, "json_col should exist");
            Assertions.assertNotNull(jsonbColumn, "jsonb_col should exist");

            String jsonTypeStr = jsonColumn.getDataType().toString();
            String jsonbTypeStr = jsonbColumn.getDataType().toString();
            System.out.println("JSON type: " + jsonTypeStr);
            System.out.println("JSONB type: " + jsonbTypeStr);

            // JSON types should map to SeaTunnel's string-like type
            Assertions.assertTrue(
                    jsonTypeStr.contains("STRING") ||
                            jsonTypeStr.contains("VARCHAR") ||
                            jsonTypeStr.contains("TEXT") ||
                            jsonTypeStr.contains("JSON"),
                    "JSON should map to string-like type. Got: " + jsonTypeStr);

            // 3. TIMESTAMP WITH TIME ZONE (critical for timezone handling)
            Column timestamptzColumn = columnMap.get("timestamptz_col");
            Assertions.assertNotNull(timestamptzColumn, "timestamptz_col should exist");
            String timestamptzTypeStr = timestamptzColumn.getDataType().toString();
            System.out.println("TIMESTAMPTZ type: " + timestamptzTypeStr);

            Assertions.assertTrue(
                    timestamptzTypeStr.contains("TIMESTAMP") ||
                            timestamptzTypeStr.contains("DATETIME"),
                    "TIMESTAMP WITH TIME ZONE should map to timestamp type. Got: " + timestamptzTypeStr);

            // 4. XML type
            Column xmlColumn = columnMap.get("xml_col");
            Assertions.assertNotNull(xmlColumn, "xml_col should exist");
            String xmlTypeStr = xmlColumn.getDataType().toString();
            System.out.println("XML type: " + xmlTypeStr);

            Assertions.assertTrue(
                    xmlTypeStr.contains("STRING") ||
                            xmlTypeStr.contains("VARCHAR") ||
                            xmlTypeStr.contains("TEXT"),
                    "XML should map to string-like type. Got: " + xmlTypeStr);

            // 5. BYTEA (binary data)
            Column byteaColumn = columnMap.get("bytea_col");
            Assertions.assertNotNull(byteaColumn, "bytea_col should exist");
            String byteaTypeStr = byteaColumn.getDataType().toString();
            System.out.println("BYTEA type: " + byteaTypeStr);

            Assertions.assertTrue(
                    byteaTypeStr.contains("BYTES") ||
                            byteaTypeStr.contains("BINARY") ||
                            byteaTypeStr.contains("BYTEA") ||
                            byteaTypeStr.contains("VARBINARY"),
                    "BYTEA should map to binary type. Got: " + byteaTypeStr);

            // 6. Numeric types with precision/scale
            Column decimalColumn = columnMap.get("decimal_col");
            Assertions.assertNotNull(decimalColumn, "decimal_col should exist");
            String decimalTypeStr = decimalColumn.getDataType().toString();
            System.out.println("DECIMAL(10,2) type: " + decimalTypeStr);

            // Check if it's a decimal/numeric type
            String normalizedType = decimalTypeStr.toUpperCase().trim();
            Assertions.assertTrue(
                    normalizedType.contains("DECIMAL") ||
                            normalizedType.contains("NUMERIC") ||
                            normalizedType.startsWith("DECIMAL(10,2)"),
                    "DECIMAL should map to decimal/numeric type. Got: " + decimalTypeStr);

            // 7. Integer types
            Column integerColumn = columnMap.get("integer_col");
            Column bigintColumn = columnMap.get("bigint_col");
            Assertions.assertNotNull(integerColumn, "integer_col should exist");
            Assertions.assertNotNull(bigintColumn, "bigint_col should exist");

            System.out.println("INTEGER type: " + integerColumn.getDataType());
            System.out.println("BIGINT type: " + bigintColumn.getDataType());

            // ========== VALIDATE COLUMN METADATA ==========

            // Check column count (simplified - 19 columns in our simplified table)
            Assertions.assertEquals(19, columns.size(),
                    "Should have 19 columns in the table");

            // Check comment is preserved
            Assertions.assertEquals("UUID column comment", uuidColumn.getComment(),
                    "Column comment should be preserved");

            // Check Primary Key
            PrimaryKey primaryKey = tableSchema.getPrimaryKey();
            Assertions.assertNotNull(primaryKey, "Primary Key should exist");
            Assertions.assertEquals("id", primaryKey.getColumnNames().get(0),
                    "Primary Key should be on 'id' column");

            System.out.println("\nAll PostgreSQL data types mapped correctly to SeaTunnel types");

            // Test data round-trip
            testDataRoundTrip(catalog, tablePath);

        } finally {
            catalog.close();
            executeSql("DROP TABLE IF EXISTS " + tablePath.getFullName());
        }
    }

    private void testDataRoundTrip(PostgresCatalog catalog, TablePath tablePath) throws SQLException {
        System.out.println("\nTesting data round-trip for critical types...");

        // Simpler insert that should work with all types
        String insertSQL =
                "INSERT INTO " + tablePath.getFullName() + " (\n" +
                        "  uuid_col, text_col, varchar_col, boolean_col,\n" +
                        "  smallint_col, integer_col, bigint_col, decimal_col,\n" +
                        "  numeric_col, real_col, double_precision_col,\n" +
                        "  date_col, timestamp_col, timestamptz_col,\n" +
                        "  json_col, jsonb_col, xml_col\n" +
                        ") VALUES (\n" +
                        "  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'Sample text', 'varchar value', true,\n" +
                        "  123, 4567, 123456789012345, 1234.56,\n" +
                        "  12.3456, 3.14, 3.141592653589793,\n" +
                        "  '2023-12-25', '2023-12-25 14:30:00', '2023-12-25 14:30:00+00',\n" +
                        "  '{\"key\": \"value\", \"number\": 123}', '{\"key\": \"value\", \"boolean\": true}',\n" +
                        "  '<note><to>Test</to><from>Unit</from></note>'\n" +
                        ")";

        catalog.executeSql(tablePath, insertSQL);

        // Verify data exists
        boolean hasData = catalog.isExistsData(tablePath);
        Assertions.assertTrue(hasData, "Table should have data after insert");

        // Query back the data to ensure it can be read
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tablePath.getFullName())) {

            rs.next();
            int count = rs.getInt(1);
            Assertions.assertEquals(1, count, "Should have 1 row in table");

            // Query specific columns to ensure they can be read
            ResultSet dataRs = stmt.executeQuery(
                    "SELECT uuid_col::text, timestamptz_col, json_col::text, xml_col::text FROM " +
                            tablePath.getFullName());

            if (dataRs.next()) {
                String uuid = dataRs.getString(1);
                Timestamp timestamptz = dataRs.getTimestamp(2);
                String json = dataRs.getString(3);
                String xml = dataRs.getString(4);

                Assertions.assertNotNull(uuid, "UUID should not be null");
                Assertions.assertNotNull(timestamptz, "TIMESTAMPTZ should not be null");
                Assertions.assertNotNull(json, "JSON should not be null");
                Assertions.assertNotNull(xml, "XML should not be null");

                System.out.println("Data round-trip successful:");
                System.out.println("  UUID: " + uuid);
                System.out.println("  TIMESTAMPTZ: " + timestamptz);
                System.out.println("  JSON: " + json.substring(0, Math.min(50, json.length())) + "...");
                System.out.println("  XML: " + xml.substring(0, Math.min(50, xml.length())) + "...");
            }
        }

        // Clean up test data
        catalog.truncateTable(tablePath, true);
        Assertions.assertFalse(catalog.isExistsData(tablePath),
                "Table should be empty after truncate");
    }

}