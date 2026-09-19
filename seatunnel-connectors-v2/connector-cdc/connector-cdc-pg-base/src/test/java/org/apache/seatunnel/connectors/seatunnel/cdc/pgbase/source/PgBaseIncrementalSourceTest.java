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

package org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;

import org.apache.kafka.connect.data.Struct;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers the source behavior the PG-base extraction moved out of the concrete PostgreSQL connector:
 * table-change discovery through the already-built dialect, and the format-dependent deserializer
 * selection. A regression here silently changes the schema payload every PG-compatible CDC job
 * hands to its row deserializer.
 */
public class PgBaseIncrementalSourceTest {

    private static final TableId ORDERS = new TableId(null, "public", "orders");

    private static final TableId CUSTOMERS = new TableId(null, "public", "customers");

    @Test
    public void testLoadTableChangesSerializesEveryDiscoveredTable() {
        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(defaultConfig());

        Map<TableId, Struct> tableChanges = source.loadTableChanges();

        Assertions.assertEquals(2, tableChanges.size());
        Assertions.assertTrue(tableChanges.keySet().containsAll(Arrays.asList(ORDERS, CUSTOMERS)));
        tableChanges.values().forEach(Assertions::assertNotNull);
    }

    @Test
    public void testLoadTableChangesReusesTheDialectBuiltByTheConstructor() {
        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(defaultConfig());
        JdbcDataSourceDialect constructorDialect = source.dialect();

        clearInvocations(constructorDialect);
        source.loadTableChanges();

        InOrder connectionOrder = inOrder(constructorDialect);
        connectionOrder.verify(constructorDialect).discoverDataCollections(any());
        connectionOrder.verify(constructorDialect).openJdbcConnection(any());
        connectionOrder.verify(constructorDialect, times(2)).queryTableSchema(any(), any());
        verifyNoMoreInteractions(constructorDialect);
    }

    @Test
    public void testLoadTableChangesClosesJdbcConnection() throws Exception {
        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(defaultConfig());

        source.loadTableChanges();

        verify(source.connection(), times(2)).close();
    }

    @Test
    public void testCreateDebeziumDeserializationSchemaUsesRowSchemaByDefault() {
        ReadonlyConfig config = defaultConfig();
        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(config);

        Assertions.assertInstanceOf(
                SeaTunnelRowDebeziumDeserializeSchema.class,
                source.createDebeziumDeserializationSchema(config));
    }

    @Test
    public void testCreateDebeziumDeserializationSchemaHonorsCompatibleDebeziumJsonFormat() {
        Map<String, Object> options = new HashMap<>();
        options.put(SourceOptions.FORMAT.key(), DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.name());
        options.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), new HashMap<String, String>());
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(config);

        Assertions.assertInstanceOf(
                DebeziumJsonDeserializeSchema.class,
                source.createDebeziumDeserializationSchema(config));
    }

    @Test
    public void testDefaultConverterFactoryIsTheDebeziumDefault() {
        TestingPgBaseIncrementalSource source = new TestingPgBaseIncrementalSource(defaultConfig());

        Assertions.assertSame(
                DebeziumDeserializationConverterFactory.DEFAULT,
                source.getUserDefinedConverterFactory());
    }

    private static ReadonlyConfig defaultConfig() {
        return ReadonlyConfig.fromMap(new HashMap<>());
    }

    private static List<CatalogTable> catalogTables() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable("pg", "test_db", "public", "orders", rowType));
    }

    /** Builds a minimal real Debezium table so the change serializer produces a usable Struct. */
    private static TableChanges.TableChange tableChange(TableId tableId) {
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(
                                Column.editor()
                                        .name("id")
                                        .type("int4")
                                        .jdbcType(Types.INTEGER)
                                        .optional(false)
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();
        return new TableChanges().create(table).iterator().next();
    }

    /**
     * Test source that swaps the JDBC bootstrap for mocks while leaving the PG-base logic under
     * test untouched. The mocks are created inside the overridden factory methods because the
     * superclass constructor invokes them before any subclass field initializer runs.
     */
    private static final class TestingPgBaseIncrementalSource
            extends PgBaseIncrementalSource<SeaTunnelRow, JdbcSourceConfig> {

        private static final Option<StartupMode> STARTUP_MODE =
                Options.key(SourceOptions.STARTUP_MODE_KEY)
                        .enumType(StartupMode.class)
                        .defaultValue(StartupMode.INITIAL)
                        .withDescription("Test-only startup mode option.");

        private static final Option<StopMode> STOP_MODE =
                Options.key(SourceOptions.STOP_MODE_KEY)
                        .enumType(StopMode.class)
                        .defaultValue(StopMode.NEVER)
                        .withDescription("Test-only stop mode option.");

        private JdbcConnection jdbcConnection;

        private TestingPgBaseIncrementalSource(ReadonlyConfig options) {
            super(options, catalogTables());
        }

        @Override
        public String getPluginName() {
            return "PgBase-Test";
        }

        @Override
        public Option<StartupMode> getStartupModeOption() {
            return STARTUP_MODE;
        }

        @Override
        public Option<StopMode> getStopModeOption() {
            return STOP_MODE;
        }

        @Override
        public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(
                ReadonlyConfig config) {
            return subtask -> mock(JdbcSourceConfig.class);
        }

        @Override
        public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            jdbcConnection = mock(JdbcConnection.class);
            JdbcDataSourceDialect dialect = mock(JdbcDataSourceDialect.class);
            when(dialect.openJdbcConnection(any())).thenReturn(jdbcConnection);
            when(dialect.discoverDataCollections(any()))
                    .thenReturn(Arrays.asList(ORDERS, CUSTOMERS));
            when(dialect.queryTableSchema(any(), any()))
                    .thenAnswer(invocation -> tableChange(invocation.getArgument(1)));
            return dialect;
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new TestingOffsetFactory();
        }

        @Override
        public Optional<String> driverName() {
            return Optional.of("org.postgresql.Driver");
        }

        /** Exposes the dialect built by the superclass constructor for verification. */
        private JdbcDataSourceDialect dialect() {
            return (JdbcDataSourceDialect) dataSourceDialect;
        }

        /** Exposes the connection handed out by the mocked dialect for close verification. */
        private JdbcConnection connection() {
            return jdbcConnection;
        }
    }

    /** Minimal offset factory so the source constructor can finish without connector state. */
    private static final class TestingOffsetFactory extends OffsetFactory {

        @Override
        public Offset earliest() {
            return null;
        }

        @Override
        public Offset neverStop() {
            return null;
        }

        @Override
        public Offset latest() {
            return null;
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return null;
        }

        @Override
        public Offset specific(String filename, Long position) {
            return null;
        }

        @Override
        public Offset timestamp(long timestamp) {
            return null;
        }
    }
}
