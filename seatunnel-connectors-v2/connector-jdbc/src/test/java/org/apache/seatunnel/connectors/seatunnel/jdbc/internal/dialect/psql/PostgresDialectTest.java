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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Types;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresDialectTest {

    @Test
    void testAddColumnQuotesUserDefinedTypeWithSpaces() throws Exception {
        assertAddColumnType("order status", Types.OTHER, "order status", "\"order status\"");
    }

    @Test
    void testAddColumnQuotesMixedCaseAndReservedUserDefinedTypes() throws Exception {
        assertAddColumnType("MyRange", Types.OTHER, "MyRange", "\"MyRange\"");
        assertAddColumnType("JobStatus", Types.OTHER, "JobStatus", "\"JobStatus\"");
        assertAddColumnType("order", Types.OTHER, "order", "\"order\"");
        assertAddColumnType("lowercase_type", Types.OTHER, "lowercase_type", "\"lowercase_type\"");
    }

    @Test
    void testAddColumnLeavesBuiltinMultiWordTypeUnquoted() throws Exception {
        assertAddColumnType(
                "timestamptz",
                Types.TIMESTAMP_WITH_TIMEZONE,
                "timestamp with time zone",
                "timestamp with time zone");
    }

    private void assertAddColumnType(
            String dataType, int sqlType, String sourceType, String expectedType) throws Exception {
        PostgresDialect dialect = new PostgresDialect();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        PhysicalColumn column =
                (PhysicalColumn)
                        dialect.getTypeConverter()
                                .convert(
                                        BasicTypeDefine.builder()
                                                .name("status")
                                                .dataType(dataType)
                                                .columnType(sourceType)
                                                .sqlType(sqlType)
                                                .nullable(true)
                                                .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "db", "app", "orders"), column);
        event.setSourceDialectName(dialect.dialectName());

        dialect.applySchemaChange(connection, TablePath.of("db", "app", "orders"), event);

        verify(statement)
                .execute(
                        "ALTER TABLE \"db\".\"app\".\"orders\" ADD \"status\" "
                                + expectedType
                                + " NULL");
    }

    @Test
    void testUpsertStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "seatunnel";
        final String tableName = "role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };
        final String[] doUpdateKeyFields = {"id"};
        final String[] doNothingKeyFields = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };

        String doUpdateSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, doUpdateKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doUpdateSql String to be present"));
        Assertions.assertEquals(
                doUpdateSql,
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\") DO UPDATE SET \"type\"=EXCLUDED.\"type\", \"role_name\"=EXCLUDED.\"role_name\", \"description\"=EXCLUDED.\"description\", \"create_time\"=EXCLUDED.\"create_time\", \"update_time\"=EXCLUDED.\"update_time\"");
        String doNothingSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, doNothingKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doNothingSql String to be present"));
        Assertions.assertEquals(
                doNothingSql,
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") DO NOTHING");
    }
}
