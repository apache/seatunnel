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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.cockroachdb.CockroachDBDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class JdbcSinkWriterSchemaChangeSafetyTest {
    private static final TablePath TABLE_PATH = TablePath.of("defaultdb", "public", "target");

    @Test
    void cockroachRejectsSchemaChangeBeforeMutatingOrCommitting() throws Exception {
        TableSchema schema = schema("id");
        JdbcSinkWriter writer = spy(writer(schema));
        Object originalOutputFormat = writer.outputFormat;
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("jdbc", TABLE_PATH),
                        PhysicalColumn.builder()
                                .name("extra")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> writer.applySchemaChange(event));
        Assertions.assertSame(schema, writer.tableSchema);
        Assertions.assertSame(originalOutputFormat, writer.outputFormat);
        verify(writer, never()).prepareCommit();
        verify(writer, never()).reOpenOutputFormat(any());
    }

    @Test
    void cockroachStillAcceptsRuntimeSchemaRestore() throws Exception {
        JdbcSinkWriter writer = spy(writer(schema("id")));
        TableSchema restoredSchema = schema("id", "extra");
        RestoreTableSchemaEvent event =
                new RestoreTableSchemaEvent(
                        CatalogTable.of(
                                TableIdentifier.of("jdbc", TABLE_PATH),
                                restoredSchema,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null,
                                "jdbc"));
        doNothing().when(writer).reOpenOutputFormat(event);

        writer.applySchemaChange(event);

        Assertions.assertArrayEquals(
                restoredSchema.getFieldNames(), writer.tableSchema.getFieldNames());
        verify(writer).reOpenOutputFormat(event);
    }

    private static JdbcSinkWriter writer(TableSchema schema) {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .url("jdbc:postgresql://localhost:26257/defaultdb")
                        .driverName("org.postgresql.Driver")
                        .properties(Collections.emptyMap())
                        .build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .database("defaultdb")
                        .table("public.target")
                        .build();
        return new JdbcSinkWriter(
                TABLE_PATH, new CockroachDBDialect(), sinkConfig, schema, null, null);
    }

    private static TableSchema schema(String... names) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String name : names) {
            builder.column(
                    PhysicalColumn.builder().name(name).dataType(BasicType.STRING_TYPE).build());
        }
        return builder.build();
    }
}
