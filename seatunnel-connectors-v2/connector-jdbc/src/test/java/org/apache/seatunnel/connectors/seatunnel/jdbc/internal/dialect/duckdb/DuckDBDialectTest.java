package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DuckDBDialectTest {

    @Test
    void testUpsertStatement() {
        DuckDBDialect dialect = new DuckDBDialect();
        String database = "seatunnel";
        String tableName = "test_schema.role";
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeyFields = {"id"};
        String upsertSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected upsert SQL String to be present"));
        Assertions.assertEquals(
                "MERGE INTO \"test_schema\".\"role\" AS target USING (SELECT :id AS \"id\", :name AS \"name\", :age AS \"age\") AS source ON target.\"id\" = source.\"id\" WHEN MATCHED THEN UPDATE SET target.\"name\" = source.\"name\", target.\"age\" = source.\"age\" WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"age\") VALUES (source.\"id\", source.\"name\", source.\"age\")",
                upsertSql);
    }
}
