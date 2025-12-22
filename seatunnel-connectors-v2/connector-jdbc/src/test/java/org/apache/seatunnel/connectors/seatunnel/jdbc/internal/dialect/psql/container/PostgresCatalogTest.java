package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.container;

import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

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
}