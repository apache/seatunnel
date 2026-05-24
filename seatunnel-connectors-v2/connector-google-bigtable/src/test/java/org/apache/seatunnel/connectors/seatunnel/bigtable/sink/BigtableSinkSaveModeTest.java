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

package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link BigtableSink#handleSaveMode()}.
 *
 * <p>Verifies that unsupported save modes fail fast with a {@link BigtableConnectorException}
 * instead of silently no-opping (Issue 3 fix).
 */
class BigtableSinkSaveModeTest {

    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                "row key"))
                                .column(
                                        PhysicalColumn.of(
                                                "cf:name",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
    }

    private ReadonlyConfig buildConfig(String schemaSaveMode, String dataSaveMode) {
        Map<String, Object> map = new HashMap<>();
        map.put("project_id", "p");
        map.put("instance_id", "i");
        map.put("table", "t");
        map.put("rowkey_column", Arrays.asList("id"));
        map.put("column_family", Collections.singletonMap("all_columns", "cf"));
        if (schemaSaveMode != null) {
            map.put("schema_save_mode", schemaSaveMode);
        }
        if (dataSaveMode != null) {
            map.put("data_save_mode", dataSaveMode);
        }
        return ReadonlyConfig.fromMap(map);
    }

    private BigtableSink newSink() {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        return new BigtableSink(config, catalogTable);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = BigtableSink.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    /** Invokes the private handleSaveMode guard without creating a writer or Bigtable client. */
    private static void invokeHandleSaveMode(BigtableSink sink) throws Exception {
        Method method = BigtableSink.class.getDeclaredMethod("handleSaveMode");
        method.setAccessible(true);
        try {
            method.invoke(sink);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    @Test
    void testSupportedModeDoesNotThrow() {
        BigtableSink sink = newSink();
        assertDoesNotThrow(() -> invokeHandleSaveMode(sink));
    }

    @Test
    void testDropDataThrows() throws Exception {
        BigtableSink sink = newSink();
        setField(sink, "dataSaveMode", DataSaveMode.DROP_DATA);
        assertThrows(BigtableConnectorException.class, () -> invokeHandleSaveMode(sink));
    }

    @Test
    void testErrorWhenDataExistsThrows() throws Exception {
        BigtableSink sink = newSink();
        setField(sink, "dataSaveMode", DataSaveMode.ERROR_WHEN_DATA_EXISTS);
        assertThrows(BigtableConnectorException.class, () -> invokeHandleSaveMode(sink));
    }

    @Test
    void testCreateSchemaWhenNotExistThrows() throws Exception {
        BigtableSink sink = newSink();
        setField(sink, "schemaSaveMode", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
        assertThrows(BigtableConnectorException.class, () -> invokeHandleSaveMode(sink));
    }
}
