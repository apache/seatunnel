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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqlServerDialectTest {

    @Test
    void shouldUseSchemaTableColumnForRenameWithoutDatabasePrefix() throws Exception {
        SqlServerDialect dialect = new SqlServerDialect();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        Column newColumn = mock(Column.class);
        when(newColumn.getName()).thenReturn("add_column");
        when(newColumn.getDataType()).thenReturn(null);

        AlterTableChangeColumnEvent event =
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of("SqlServer", "schema_change_test", "dbo", "products"),
                        "add_column2",
                        newColumn);

        dialect.applySchemaChange(
                connection, TablePath.of("schema_change_test", "dbo", "products_sink"), event);

        verify(statement)
                .execute(
                        "EXEC [schema_change_test].sys.sp_rename 'dbo.products_sink.add_column2', 'add_column', 'COLUMN';");
    }

    @Test
    void testAllKeyTableOmitsEmptyUpdateSet() {
        JdbcDialect dialect = new SqlServerDialect();
        String[] allFields = {"id", "name", "age"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, allFields);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertFalse(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "all-key table must NOT emit an empty 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
        Assertions.assertTrue(
                sql.contains("WHEN NOT MATCHED"), "all-key table must still insert unmatched rows");
        Assertions.assertTrue(
                sql.contains("INSERT"), "all-key table statement must contain an INSERT branch");
    }

    @Test
    void testPartialKeyTableStillUpdates() {
        JdbcDialect dialect = new SqlServerDialect();
        String[] allFields = {"id", "name", "age"};
        String[] uniqueKeys = {"id"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, uniqueKeys);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertTrue(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "partial-key table must still emit 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
    }
}
