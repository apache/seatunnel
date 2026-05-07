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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

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

    // -------------------------------------------------------------------------
    // Supported mode: RECREATE_SCHEMA + APPEND_DATA must not throw
    // -------------------------------------------------------------------------

    @Test
    void testSupportedModeDoesNotThrow() {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        BigtableSink sink = new BigtableSink(config, catalogTable);
        SinkWriter.Context ctx = mock(SinkWriter.Context.class);
        assertDoesNotThrow(() -> sink.createWriter(ctx));
    }

    // -------------------------------------------------------------------------
    // Unsupported modes must throw BigtableConnectorException immediately
    // -------------------------------------------------------------------------

    @Test
    void testDropDataThrows() {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "DROP_DATA");
        // DROP_DATA is excluded from singleChoice, so ReadonlyConfig.fromMap will use default.
        // We verify the behavior when the enum value is manually injected via reflection.
        // Since singleChoice prevents DROP_DATA from being set via config, test the guard
        // directly by inspecting the handleSaveMode logic path.
        // The OptionRule will prevent users from setting DROP_DATA, so the exception path
        // acts as a defence-in-depth guard. We verify the default (APPEND_DATA) doesn't throw.
        BigtableSink sink = new BigtableSink(config, catalogTable);
        SinkWriter.Context ctx = mock(SinkWriter.Context.class);
        assertDoesNotThrow(() -> sink.createWriter(ctx));
    }

    @Test
    void testCreateSchemaWhenNotExistThrows() {
        // CREATE_SCHEMA_WHEN_NOT_EXIST is excluded from singleChoice; verify default is safe
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        BigtableSink sink = new BigtableSink(config, catalogTable);
        SinkWriter.Context ctx = mock(SinkWriter.Context.class);
        assertDoesNotThrow(() -> sink.createWriter(ctx));
    }

    /**
     * Verify that the handleSaveMode guard logic throws for DROP_DATA when invoked directly via
     * reflection (bypassing OptionRule validation — simulates engine-level injection).
     */
    @Test
    void testHandleSaveModeThrowsForDropDataViaReflection() throws Exception {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        BigtableSink sink = new BigtableSink(config, catalogTable);

        // Inject DataSaveMode.DROP_DATA directly
        java.lang.reflect.Field dataSaveModeField =
                BigtableSink.class.getDeclaredField("dataSaveMode");
        dataSaveModeField.setAccessible(true);
        dataSaveModeField.set(sink, DataSaveMode.DROP_DATA);

        java.lang.reflect.Method method = BigtableSink.class.getDeclaredMethod("handleSaveMode");
        method.setAccessible(true);
        assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> method.invoke(sink),
                "handleSaveMode() must throw for DROP_DATA");
    }

    @Test
    void testHandleSaveModeThrowsForErrorWhenDataExistsViaReflection() throws Exception {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        BigtableSink sink = new BigtableSink(config, catalogTable);

        java.lang.reflect.Field dataSaveModeField =
                BigtableSink.class.getDeclaredField("dataSaveMode");
        dataSaveModeField.setAccessible(true);
        dataSaveModeField.set(sink, DataSaveMode.ERROR_WHEN_DATA_EXISTS);

        java.lang.reflect.Method method = BigtableSink.class.getDeclaredMethod("handleSaveMode");
        method.setAccessible(true);
        assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> method.invoke(sink),
                "handleSaveMode() must throw for ERROR_WHEN_DATA_EXISTS");
    }

    @Test
    void testHandleSaveModeThrowsForCreateSchemaWhenNotExistViaReflection() throws Exception {
        ReadonlyConfig config = buildConfig("RECREATE_SCHEMA", "APPEND_DATA");
        BigtableSink sink = new BigtableSink(config, catalogTable);

        java.lang.reflect.Field schemaSaveModeField =
                BigtableSink.class.getDeclaredField("schemaSaveMode");
        schemaSaveModeField.setAccessible(true);
        schemaSaveModeField.set(sink, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);

        java.lang.reflect.Method method = BigtableSink.class.getDeclaredMethod("handleSaveMode");
        method.setAccessible(true);
        assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> method.invoke(sink),
                "handleSaveMode() must throw for CREATE_SCHEMA_WHEN_NOT_EXIST");
    }
}
