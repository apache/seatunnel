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
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.sink.CassandraSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.cassandra.source.CassandraSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CassandraFactoryTest {

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
