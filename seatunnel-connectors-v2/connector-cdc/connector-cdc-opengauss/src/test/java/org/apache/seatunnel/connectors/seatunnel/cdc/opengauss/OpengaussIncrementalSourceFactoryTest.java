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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests the OpenGauss CDC source entry point introduced by the PG-base refactor so the dedicated
 * factory/source wiring stays compatible.
 */
public class OpengaussIncrementalSourceFactoryTest {

    @Test
    public void testGetSourceClassUsesDedicatedOpengaussSource() {
        OpengaussIncrementalSourceFactory factory = new OpengaussIncrementalSourceFactory();

        Assertions.assertEquals(OpengaussIncrementalSource.class, factory.getSourceClass());
    }

    /**
     * OpenGauss shares the PostgreSQL runtime but not its startup-mode surface. snapshot-only and
     * committed-offset are PostgreSQL-specific (committed-offset reads confirmed_flush_lsn and
     * active_pid from pg_replication_slots), so they must not become selectable here just because
     * the PostgreSQL option gained them.
     */
    @Test
    public void testOptionRuleExposesOnlyOpengaussStartupModes() {
        SingleChoiceOption<StartupMode> startupMode =
                (SingleChoiceOption<StartupMode>)
                        new OpengaussIncrementalSourceFactory()
                                .optionRule().getOptionalOptions().stream()
                                        .filter(
                                                option ->
                                                        SourceOptions.STARTUP_MODE_KEY.equals(
                                                                option.key()))
                                        .findFirst()
                                        .orElseThrow(
                                                () ->
                                                        new AssertionError(
                                                                "startup.mode missing from the Opengauss option rule"));

        Assertions.assertEquals(
                Arrays.asList(StartupMode.INITIAL, StartupMode.EARLIEST, StartupMode.LATEST),
                startupMode.getOptionValues());
        Assertions.assertEquals(StartupMode.INITIAL, startupMode.defaultValue());
    }

    /**
     * The source must resolve startup mode through the same narrowed option the rule advertises.
     */
    @Test
    public void testSourceResolvesStartupModeFromOpengaussOption() {
        OpengaussIncrementalSource<Object> source =
                new TestingOpengaussIncrementalSource(
                        ReadonlyConfig.fromMap(Collections.emptyMap()), createCatalogTables());

        Assertions.assertSame(OpengaussSourceOptions.STARTUP_MODE, source.getStartupModeOption());
    }

    /** Guards the other direction: narrowing OpenGauss must not narrow PostgreSQL. */
    @Test
    public void testPostgresStartupModesRemainUnchanged() {
        Assertions.assertEquals(
                Arrays.asList(
                        StartupMode.INITIAL,
                        StartupMode.SNAPSHOT_ONLY,
                        StartupMode.COMMITTED_OFFSET,
                        StartupMode.EARLIEST,
                        StartupMode.LATEST),
                PostgresSourceOptions.STARTUP_MODE.getOptionValues());
    }

    @Test
    public void testSourceKeepsOpengaussPluginName() {
        OpengaussIncrementalSource<Object> source =
                new TestingOpengaussIncrementalSource(
                        ReadonlyConfig.fromMap(Collections.emptyMap()), createCatalogTables());

        Assertions.assertEquals("Opengauss-CDC", source.getPluginName());
    }

    /**
     * OpenGauss reuses the PostgreSQL config factory through the PG base. This pins the Debezium
     * properties that assembly produces, which is what the refactor actually moved.
     */
    @Test
    public void testSourceKeepsOpengaussDebeziumPropertyAssembly() {
        JdbcSourceConfig sourceConfig =
                new TestingOpengaussConfigSource(opengaussConfig(), createCatalogTables())
                        .buildSourceConfig();
        Configuration dbzConfiguration = sourceConfig.getDbzConfiguration();

        Assertions.assertEquals(
                "io.debezium.connector.postgresql.PostgresConnector",
                dbzConfiguration.getString("connector.class"));
        Assertions.assertEquals(
                "postgres_cdc_source", dbzConfiguration.getString("database.server.name"));
        Assertions.assertEquals("opengauss-host", dbzConfiguration.getString("database.hostname"));
        Assertions.assertEquals("5432", dbzConfiguration.getString("database.port"));
        Assertions.assertEquals("inventory", dbzConfiguration.getString("database.dbname"));
        Assertions.assertEquals("pgoutput", dbzConfiguration.getString("plugin.name"));
        Assertions.assertEquals("opengauss_slot", dbzConfiguration.getString("slot.name"));
        Assertions.assertEquals("public", dbzConfiguration.getString("schema.include.list"));
        // db.schema.table must still collapse to schema.table for the PG-compatible connector.
        Assertions.assertEquals("public.orders", dbzConfiguration.getString("table.include.list"));
        Assertions.assertEquals("false", dbzConfiguration.getString("include.schema.changes"));
        Assertions.assertEquals("org.postgresql.Driver", sourceConfig.getDriverClassName());
    }

    /** Config mirroring a minimal OpenGauss CDC job so the real config factory can run. */
    private static ReadonlyConfig opengaussConfig() {
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcCommonOptions.URL.key(), "jdbc:postgresql://opengauss-host:5432/inventory");
        options.put(JdbcSourceOptions.USERNAME.key(), "og_user");
        options.put(JdbcSourceOptions.PASSWORD.key(), "og_pwd");
        options.put(JdbcSourceOptions.DATABASE_NAMES.key(), Collections.singletonList("inventory"));
        options.put(
                ConnectorCommonOptions.TABLE_NAMES.key(),
                Collections.singletonList("inventory.public.orders"));
        options.put(
                PostgresIncrementalSourceOptions.SCHEMA_NAME.key(),
                Collections.singletonList("public"));
        options.put(PostgresIncrementalSourceOptions.SLOT_NAME.key(), "opengauss_slot");
        return ReadonlyConfig.fromMap(options);
    }

    /** Builds a minimal catalog table list so the source constructor can initialize metadata. */
    private List<CatalogTable> createCatalogTables() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable(
                        "opengauss", "test_db", "public", "orders", rowType));
    }

    /**
     * Lightweight test source that bypasses the real JDBC bootstrap so the regression test can
     * focus on the entry-path contract only.
     */
    private static final class TestingOpengaussIncrementalSource
            extends OpengaussIncrementalSource<Object> {

        private TestingOpengaussIncrementalSource(
                ReadonlyConfig options, List<CatalogTable> catalogTables) {
            super(options, catalogTables);
        }

        /**
         * Returns a mock config factory because this regression test does not need real JDBC setup.
         */
        @Override
        public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(
                ReadonlyConfig config) {
            return subtask -> mock(JdbcSourceConfig.class);
        }

        /**
         * Returns a mock dialect because the constructor should not hit an external database here.
         */
        @Override
        public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return mockDataSourceDialect();
        }

        /** Returns a mock deserializer because schema loading is outside this regression scope. */
        @Override
        public DebeziumDeserializationSchema<Object> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return mockDebeziumDeserializationSchema();
        }

        /** Returns a no-op offset factory so the constructor can finish without connector state. */
        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new TestingOffsetFactory();
        }

        /** Creates a typed dialect mock for the minimal constructor-only regression scenario. */
        @SuppressWarnings("unchecked")
        private static DataSourceDialect<JdbcSourceConfig> mockDataSourceDialect() {
            return (DataSourceDialect<JdbcSourceConfig>) mock(DataSourceDialect.class);
        }

        /**
         * Creates a typed deserializer mock for the minimal constructor-only regression scenario.
         */
        @SuppressWarnings("unchecked")
        private static DebeziumDeserializationSchema<Object> mockDebeziumDeserializationSchema() {
            return (DebeziumDeserializationSchema<Object>)
                    mock(DebeziumDeserializationSchema.class);
        }
    }

    /**
     * Test source that keeps the real inherited config factory so the Debezium property assembly is
     * exercised, and mocks only the parts the superclass constructor builds afterwards.
     */
    private static final class TestingOpengaussConfigSource
            extends OpengaussIncrementalSource<Object> {

        private TestingOpengaussConfigSource(
                ReadonlyConfig options, List<CatalogTable> catalogTables) {
            super(options, catalogTables);
        }

        /**
         * Returns a mock dialect because the constructor should not hit an external database here.
         */
        @Override
        public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return TestingOpengaussIncrementalSource.mockDataSourceDialect();
        }

        /** Returns a mock deserializer because schema loading is outside this regression scope. */
        @Override
        public DebeziumDeserializationSchema<Object> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return TestingOpengaussIncrementalSource.mockDebeziumDeserializationSchema();
        }

        /** Returns a no-op offset factory so the constructor can finish without connector state. */
        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new TestingOffsetFactory();
        }

        /** Builds the source config the inherited PostgreSQL factory assembles for OpenGauss. */
        private JdbcSourceConfig buildSourceConfig() {
            return configFactory.create(0);
        }
    }

    /** Minimal offset factory used to satisfy the source constructor in this regression test. */
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
