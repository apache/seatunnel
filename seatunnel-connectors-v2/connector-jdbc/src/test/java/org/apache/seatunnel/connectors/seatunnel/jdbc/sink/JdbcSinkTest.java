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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class JdbcSinkTest {

    @Test
    void getPrimaryKeyIndexReturnsEmptyForPrimaryKeyWithoutColumns() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("empty_pk", Collections.emptyList()))
                        .build();

        Assertions.assertFalse(JdbcSink.getPrimaryKeyIndex(tableSchema).isPresent());
    }

    @Test
    void getPrimaryKeyIndexReturnsEmptyForPrimaryKeyWithNullColumns() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("null_pk", null))
                        .build();

        Assertions.assertFalse(JdbcSink.getPrimaryKeyIndex(tableSchema).isPresent());
    }

    @Test
    void getPrimaryKeyIndexReturnsFirstPrimaryKeyColumnIndex() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("id_pk", Collections.singletonList("id")))
                        .build();

        Assertions.assertEquals(1, JdbcSink.getPrimaryKeyIndex(tableSchema).get());
    }

    @Test
    void createWriterDoesNotDependOnTheDefaultLocale() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/test");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("query", "insert into orders(id) values(?)");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("Jdbc", "test", "orders"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
        JdbcSink sink =
                new JdbcSink(
                        config,
                        JdbcSinkConfig.of(config),
                        new MysqlDialect(),
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        catalogTable);

        Locale[] saved = {
            Locale.getDefault(),
            Locale.getDefault(Locale.Category.FORMAT),
            Locale.getDefault(Locale.Category.DISPLAY)
        };
        try {
            for (Locale locale : new Locale[] {new Locale("tr", "TR"), new Locale("az", "AZ")}) {
                Locale.setDefault(locale);
                // field_ide is not set, so createWriter resolves the table name with "original".
                Assertions.assertNotNull(
                        sink.createWriter(Mockito.mock(SinkWriter.Context.class)),
                        locale.toString());
            }
        } finally {
            // The base locale first, because setting it overwrites both category defaults.
            Locale.setDefault(saved[0]);
            Locale.setDefault(Locale.Category.FORMAT, saved[1]);
            Locale.setDefault(Locale.Category.DISPLAY, saved[2]);
        }
    }
}
