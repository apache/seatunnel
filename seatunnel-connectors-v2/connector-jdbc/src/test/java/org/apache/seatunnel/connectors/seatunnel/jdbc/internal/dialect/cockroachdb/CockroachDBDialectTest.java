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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.cockroachdb;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CockroachDBDialectTest {
    private final CockroachDBDialect dialect = new CockroachDBDialect();

    @Test
    void usesCockroachHashForStringAndUuidKeys() {
        String expected = "(COALESCE(FNV32(CAST(\"key\" AS STRING)), 0) % 3)";
        Assertions.assertEquals(expected, dialect.hashModForField("key", 3));
        Assertions.assertEquals(expected, dialect.hashModForField("uuid", "key", 3));
    }

    @Test
    void preservesConflictTargetAndPartialColumns() {
        Assertions.assertEquals(
                "INSERT INTO \"db\".\"public\".\"target\" (\"id\", \"email\", \"value\") "
                        + "VALUES (:id, :email, :value) ON CONFLICT (\"email\") "
                        + "DO UPDATE SET \"id\"=EXCLUDED.\"id\", \"value\"=EXCLUDED.\"value\"",
                dialect.getUpsertStatement(
                                "db",
                                "public.target",
                                new String[] {"id", "email", "value"},
                                new String[] {"email"})
                        .get());
        Assertions.assertTrue(
                dialect.getUpsertStatement("db", "target", new String[] {"id"}, new String[] {"id"})
                        .get()
                        .endsWith("ON CONFLICT (\"id\") DO NOTHING"));
    }

    @Test
    void quotesIdentifiersAndRetainsConfiguredCase() {
        JdbcDialect upper = new CockroachDBDialectFactory().create("", "UPPERCASE");
        Assertions.assertEquals("\"schema\".\"TABLE\"", upper.quoteIdentifier("schema.table"));
        Assertions.assertEquals("\"a\"\"b\"", dialect.quoteIdentifier("a\"b"));
        Assertions.assertEquals(
                "\"db\"\"name\".\"public\".\"table\"\"name\"",
                dialect.tableIdentifier(TablePath.of("db\"name", "public", "table\"name")));
    }

    @Test
    void convertsStringsWithoutResultSetMetadataQueries() throws Exception {
        for (String sourceType :
                new String[] {null, "", "STRING", "UUID", "geometry", "GEOGRAPHY"}) {
            TableSchema schema =
                    TableSchema.builder()
                            .columns(
                                    Collections.singletonList(
                                            PhysicalColumn.builder()
                                                    .name("value")
                                                    .dataType(BasicType.STRING_TYPE)
                                                    .sourceType(sourceType)
                                                    .build()))
                            .build();
            ResultSet rs = mock(ResultSet.class);
            when(rs.getObject(1)).thenReturn("value");
            when(rs.getString(1)).thenReturn("value");
            Assertions.assertEquals(
                    "value", dialect.getRowConverter().toInternal(rs, schema).getField(0));
            when(rs.getObject(1)).thenReturn(null);
            Assertions.assertNull(dialect.getRowConverter().toInternal(rs, schema).getField(0));
            verify(rs, never()).getMetaData();
        }
    }

    @Test
    void rejectsPostgresTableOptionsWithoutACockroachCatalog() {
        dialect.validateTableOptions(Collections.emptyMap());
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "80")));
    }
}
