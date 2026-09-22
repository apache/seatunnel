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

package org.apache.seatunnel.connectors.seatunnel.cassandra;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.sink.CassandraSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.cassandra.source.CassandraSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CassandraFactoryTest {

    @Test
    void dryRunSupportsDistinctTablesAndBoundsDriverWaits() throws Exception {
        Map<String, Object> options = baseConfig();
        options.put(
                "tables_configs",
                Arrays.asList(
                        Collections.singletonMap("cql", "select * from table1"),
                        Collections.singletonMap("cql", "select * from table2")));
        try (DryRunSession fixture = new DryRunSession()) {
            ColumnDefinition secondColumn = Mockito.mock(ColumnDefinition.class);
            Mockito.when(secondColumn.getName()).thenReturn(CqlIdentifier.fromInternal("Other"));
            Mockito.when(secondColumn.getTable()).thenReturn(CqlIdentifier.fromInternal("table2"));
            Mockito.when(secondColumn.getType()).thenReturn(DataTypes.INT);
            ColumnDefinitions secondColumns = Mockito.mock(ColumnDefinitions.class);
            Mockito.when(secondColumns.size()).thenReturn(1);
            Mockito.when(secondColumns.get(0)).thenReturn(secondColumn);
            PreparedStatement second = Mockito.mock(PreparedStatement.class);
            Mockito.when(second.getVariableDefinitions()).thenReturn(fixture.variables);
            Mockito.when(second.getResultSetDefinitions()).thenReturn(secondColumns);
            Mockito.when(
                            fixture.session.prepare(
                                    Mockito.argThat(
                                            (SimpleStatement query) ->
                                                    query != null
                                                            && query.getQuery()
                                                                    .contains("table2"))))
                    .thenReturn(second);
            List<CatalogTable> tables =
                    new CassandraSourceFactory().inferSchemaForDryRun(dryRunContext(options));
            Assertions.assertEquals(2, tables.size());
            Assertions.assertNotEquals(tables.get(0).getTableId(), tables.get(1).getTableId());
            Assertions.assertEquals(
                    "Other", tables.get(1).getSeaTunnelRowType().getFieldNames()[0]);
            Mockito.verify(fixture.session, Mockito.times(2))
                    .prepare(Mockito.any(SimpleStatement.class));
            Mockito.verify(fixture.session).close();
            Mockito.verifyNoMoreInteractions(fixture.session);
        }
    }

    @Test
    void dryRunPreparesSchemaAndNeverExecutesQueries() throws Exception {
        try (DryRunSession fixture = new DryRunSession()) {
            CassandraSourceFactory factory = new CassandraSourceFactory();
            List<CatalogTable> tables =
                    factory.inferSchemaForDryRun(dryRunContext(sourceConfigWithCql()));
            Assertions.assertEquals(1, tables.size());
            Assertions.assertEquals("test.table1", tables.get(0).getTablePath().toString());
            Assertions.assertArrayEquals(
                    new String[] {"Alias"}, tables.get(0).getSeaTunnelRowType().getFieldNames());
            factory.validateConnectionForDryRun(dryRunContext(sourceConfigWithCql()), tables);
            Mockito.verify(fixture.session).prepare(Mockito.any(SimpleStatement.class));
            Mockito.verify(fixture.session).close();
            Mockito.verifyNoMoreInteractions(fixture.session);
        }
    }

    @Test
    void dryRunRejectsDuplicateRuntimeTableIdentifiers() {
        Map<String, Object> config = baseConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        Collections.singletonMap("cql", "select id from table1"),
                        Collections.singletonMap("cql", "select name from table1")));
        try (DryRunSession fixture = new DryRunSession()) {
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> new CassandraSourceFactory().inferSchemaForDryRun(dryRunContext(config)));
            Mockito.verify(fixture.session).close();
        }
    }

    @Test
    void dryRunClosesAndSanitizesPrepareAndCloseFailures() {
        try (DryRunSession fixture = new DryRunSession()) {
            Mockito.when(fixture.session.prepare(Mockito.any(SimpleStatement.class)))
                    .thenThrow(new IllegalArgumentException("sensitive-query-value"));
            Mockito.doThrow(new IllegalStateException("sensitive-close-value"))
                    .when(fixture.session)
                    .close();
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new CassandraSourceFactory()
                                            .inferSchemaForDryRun(
                                                    dryRunContext(sourceConfigWithCql())));
            Assertions.assertFalse(failure.toString().contains("sensitive"));
            Assertions.assertNull(failure.getCause());
            Assertions.assertEquals(0, failure.getSuppressed().length);
            Mockito.verify(fixture.session).close();
        }
    }

    @Test
    void dryRunRejectsMissingColumnsAndUnboundParameters() {
        for (boolean missingColumns : new boolean[] {true, false}) {
            try (DryRunSession fixture = new DryRunSession()) {
                if (missingColumns) {
                    Mockito.when(fixture.columns.size()).thenReturn(0);
                } else {
                    Mockito.when(fixture.variables.size()).thenReturn(1);
                }
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new CassandraSourceFactory()
                                        .inferSchemaForDryRun(
                                                dryRunContext(sourceConfigWithCql())));
                Mockito.verify(fixture.session).close();
            }
        }
    }

    @Test
    void dryRunRejectsNonSelectBeforeConnecting() {
        try (MockedStatic<CassandraClient> client = Mockito.mockStatic(CassandraClient.class)) {
            for (String cql :
                    Arrays.asList(
                            "INSERT INTO table1 (id) VALUES (1)",
                            "UPDATE table1 SET name='x' WHERE id=1 IF EXISTS",
                            "DELETE FROM table1 WHERE id=1",
                            "TRUNCATE table1",
                            "/* comment */ SELECT * FROM table1")) {
                Map<String, Object> config = baseConfig();
                config.put("cql", cql);
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new CassandraSourceFactory()
                                        .inferSchemaForDryRun(dryRunContext(config)));
            }
            client.verifyNoInteractions();
        }
    }

    @Test
    void dryRunHonorsPreexistingInterruption() {
        Thread.currentThread().interrupt();
        try {
            Assertions.assertThrows(
                    InterruptedException.class,
                    () ->
                            new CassandraSourceFactory()
                                    .inferSchemaForDryRun(dryRunContext(sourceConfigWithCql())));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private TableSourceFactoryContext dryRunContext(Map<String, Object> config) {
        return new TableSourceFactoryContext(
                ReadonlyConfig.fromMap(config), getClass().getClassLoader());
    }

    private static final class DryRunSession implements AutoCloseable {
        final CqlSession session = Mockito.mock(CqlSession.class);
        final ColumnDefinitions columns = Mockito.mock(ColumnDefinitions.class);
        final ColumnDefinitions variables = Mockito.mock(ColumnDefinitions.class);
        final MockedStatic<CassandraClient> client = Mockito.mockStatic(CassandraClient.class);

        DryRunSession() {
            CqlSessionBuilder builder = Mockito.mock(CqlSessionBuilder.class, Mockito.RETURNS_SELF);
            client.when(
                            () ->
                                    CassandraClient.getCqlSessionBuilder(
                                            Mockito.anyString(),
                                            Mockito.anyString(),
                                            Mockito.any(),
                                            Mockito.any(),
                                            Mockito.anyString()))
                    .thenReturn(builder);
            Mockito.when(builder.build()).thenReturn(session);
            Mockito.when(builder.withConfigLoader(Mockito.any()))
                    .thenAnswer(
                            invocation -> {
                                com.datastax.oss.driver.api.core.config.DriverConfigLoader loader =
                                        invocation.getArgument(0);
                                com.datastax.oss.driver.api.core.config.DriverExecutionProfile
                                        profile = loader.getInitialConfig().getDefaultProfile();
                                Assertions.assertFalse(
                                        profile.getBoolean(
                                                com.datastax.oss.driver.api.core.config
                                                        .DefaultDriverOption.RECONNECT_ON_INIT));
                                Assertions.assertEquals(
                                        java.time.Duration.ofSeconds(10),
                                        profile.getDuration(
                                                com.datastax.oss.driver.api.core.config
                                                        .DefaultDriverOption
                                                        .CONNECTION_CONNECT_TIMEOUT));
                                Assertions.assertEquals(
                                        java.time.Duration.ofSeconds(10),
                                        profile.getDuration(
                                                com.datastax.oss.driver.api.core.config
                                                        .DefaultDriverOption.REQUEST_TIMEOUT));
                                return builder;
                            });
            PreparedStatement prepared = Mockito.mock(PreparedStatement.class);
            Mockito.when(session.prepare(Mockito.any(SimpleStatement.class))).thenReturn(prepared);
            Mockito.when(prepared.getResultSetDefinitions()).thenReturn(columns);
            Mockito.when(prepared.getVariableDefinitions()).thenReturn(variables);
            ColumnDefinition column = Mockito.mock(ColumnDefinition.class);
            Mockito.when(columns.size()).thenReturn(1);
            Mockito.when(columns.get(0)).thenReturn(column);
            Mockito.when(column.getName()).thenReturn(CqlIdentifier.fromInternal("Alias"));
            Mockito.when(column.getTable()).thenReturn(CqlIdentifier.fromInternal("table1"));
            Mockito.when(column.getType()).thenReturn(DataTypes.TEXT);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new CassandraSourceFactory()).optionRule());
        Assertions.assertNotNull((new CassandraSinkFactory()).optionRule());
    }

    @Test
    void testSourceOptionRuleWithCqlOnly() {
        OptionRule rule = new CassandraSourceFactory().optionRule();
        Map<String, Object> cfg = baseConfig();
        cfg.put(CassandraSourceOptions.CQL.key(), "select * from test.table1");
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule);
    }

    @Test
    void testSourceOptionRuleWithTablesConfigsOnly() {
        OptionRule rule = new CassandraSourceFactory().optionRule();
        Map<String, Object> cfg = baseConfig();
        List<Map<String, Object>> tablesConfigs =
                Collections.singletonList(
                        Collections.singletonMap(
                                CassandraSourceOptions.CQL.key(), "select * from test.table1"));
        cfg.put(ConnectorCommonOptions.TABLE_CONFIGS.key(), tablesConfigs);
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule);
    }

    @Test
    void testSourceOptionRuleWithBothCqlAndTablesConfigsThrows() {
        OptionRule rule = new CassandraSourceFactory().optionRule();
        Map<String, Object> cfg = baseConfig();
        cfg.put(CassandraSourceOptions.CQL.key(), "select * from test.table1");
        List<Map<String, Object>> tablesConfigs =
                Collections.singletonList(
                        Collections.singletonMap(
                                CassandraSourceOptions.CQL.key(), "select * from test.table2"));
        cfg.put(ConnectorCommonOptions.TABLE_CONFIGS.key(), tablesConfigs);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
    }

    @Test
    void testSourceOptionRuleWithNeitherCqlNorTablesConfigsThrows() {
        OptionRule rule = new CassandraSourceFactory().optionRule();
        Map<String, Object> cfg = baseConfig();
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
    }

    @Test
    void testSourceOptionRuleWithBlankHostThrows() {
        Map<String, Object> cfg = sourceConfigWithCql();
        cfg.put(CassandraSourceOptions.HOST.key(), " ");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithBlankKeyspaceThrows() {
        Map<String, Object> cfg = sourceConfigWithCql();
        cfg.put(CassandraSourceOptions.KEYSPACE.key(), "\t");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithBlankRootCqlThrows() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(CassandraSourceOptions.CQL.key(), "\n");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithEmptyTablesConfigsThrows() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(ConnectorCommonOptions.TABLE_CONFIGS.key(), Collections.emptyList());
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithTablesConfigsChildMissingCqlThrows() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(
                ConnectorCommonOptions.TABLE_CONFIGS.key(),
                Collections.singletonList(Collections.emptyMap()));
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.of(ReadonlyConfig.fromMap(cfg))
                                        .validate(sourceRule()));
        Assertions.assertTrue(exception.getMessage().contains("tables_configs[0]: 'cql'"));
    }

    @Test
    void testSourceOptionRuleWithTablesConfigsChildBlankCqlThrows() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(
                ConnectorCommonOptions.TABLE_CONFIGS.key(),
                Collections.singletonList(
                        Collections.singletonMap(CassandraSourceOptions.CQL.key(), "  ")));
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithInvalidConsistencyLevelThrows() {
        Map<String, Object> cfg = sourceConfigWithCql();
        cfg.put(CassandraSourceOptions.CONSISTENCY_LEVEL.key(), "LOCAL_ONE ");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithValidRootCqlPasses() {
        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(sourceConfigWithCql()))
                                .validate(sourceRule()));
    }

    @Test
    void testSourceOptionRuleWithValidTablesConfigsChildCqlPasses() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(
                ConnectorCommonOptions.TABLE_CONFIGS.key(),
                Collections.singletonList(
                        Collections.singletonMap(
                                CassandraSourceOptions.CQL.key(), "select * from test.table1")));
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule()));
    }

    @Test
    void testSinkOptionRuleWithBlankHostThrows() {
        Map<String, Object> cfg = sinkConfig();
        cfg.put(CassandraSinkOptions.HOST.key(), " ");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule()));
    }

    @Test
    void testSinkOptionRuleWithBlankKeyspaceThrows() {
        Map<String, Object> cfg = sinkConfig();
        cfg.put(CassandraSinkOptions.KEYSPACE.key(), "\t");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule()));
    }

    @Test
    void testSinkOptionRuleWithBlankTableThrows() {
        Map<String, Object> cfg = sinkConfig();
        cfg.put(CassandraSinkOptions.TABLE.key(), "\n");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule()));
    }

    @Test
    void testSinkOptionRuleWithInvalidConsistencyLevelThrows() {
        Map<String, Object> cfg = sinkConfig();
        cfg.put(CassandraSinkOptions.CONSISTENCY_LEVEL.key(), "invalid");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule()));
    }

    @Test
    void testSinkOptionRuleWithInvalidBatchTypeThrows() {
        Map<String, Object> cfg = sinkConfig();
        cfg.put(CassandraSinkOptions.BATCH_TYPE.key(), "BATCHED");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule()));
    }

    @Test
    void testSinkOptionRuleWithValidConfigPasses() {
        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(sinkConfig()))
                                .validate(sinkRule()));
    }

    private OptionRule sourceRule() {
        return new CassandraSourceFactory().optionRule();
    }

    private OptionRule sinkRule() {
        return new CassandraSinkFactory().optionRule();
    }

    private Map<String, Object> sourceConfigWithCql() {
        Map<String, Object> cfg = baseConfig();
        cfg.put(CassandraSourceOptions.CQL.key(), "select * from test.table1");
        return cfg;
    }

    private Map<String, Object> sinkConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(CassandraSinkOptions.HOST.key(), "localhost:9042");
        cfg.put(CassandraSinkOptions.KEYSPACE.key(), "test");
        cfg.put(CassandraSinkOptions.TABLE.key(), "table1");
        return cfg;
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("host", "localhost:9042");
        cfg.put("keyspace", "test");
        return cfg;
    }
}
