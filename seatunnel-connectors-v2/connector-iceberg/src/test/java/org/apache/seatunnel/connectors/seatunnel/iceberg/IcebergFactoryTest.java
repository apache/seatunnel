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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.IcebergSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IcebergFactoryTest {

    private static final OptionRule SOURCE_RULE = new IcebergSourceFactory().optionRule();
    private static final OptionRule SINK_RULE = new IcebergSinkFactory().optionRule();

    // rule structure

    @Test
    void sourceOptionalContainsQuery() {
        Assertions.assertTrue(
                SOURCE_RULE.getOptionalOptions().contains(IcebergSourceOptions.QUERY));
    }

    @Test
    void sourceCatalogNameAndNamespaceAreOptional() {
        Assertions.assertTrue(
                SOURCE_RULE.getOptionalOptions().contains(IcebergCommonOptions.KEY_CATALOG_NAME));
        Assertions.assertTrue(
                SOURCE_RULE.getOptionalOptions().contains(IcebergCommonOptions.KEY_NAMESPACE));
        List<Option<?>> required = absolutelyRequiredOptions(SOURCE_RULE);
        Assertions.assertFalse(required.contains(IcebergCommonOptions.KEY_CATALOG_NAME));
        Assertions.assertFalse(required.contains(IcebergCommonOptions.KEY_NAMESPACE));
    }

    @Test
    void sourceCatalogPropsIsRequired() {
        Assertions.assertTrue(
                absolutelyRequiredOptions(SOURCE_RULE)
                        .contains(IcebergCommonOptions.CATALOG_PROPS));
    }

    @Test
    void sourceTableAndTableListAreExclusive() {
        boolean hasExclusive =
                SOURCE_RULE.getRequiredOptions().stream()
                        .filter(o -> o instanceof RequiredOption.ExclusiveRequiredOptions)
                        .anyMatch(
                                o ->
                                        o.getOptions().contains(IcebergCommonOptions.KEY_TABLE)
                                                && o.getOptions()
                                                        .contains(
                                                                IcebergSourceOptions
                                                                        .KEY_TABLE_LIST));
        Assertions.assertTrue(hasExclusive);
    }

    @Test
    void sinkTableIsOptional() {
        Assertions.assertTrue(
                SINK_RULE.getOptionalOptions().contains(IcebergSinkOptions.KEY_TABLE));
        Assertions.assertFalse(
                absolutelyRequiredOptions(SINK_RULE).contains(IcebergSinkOptions.KEY_TABLE));
    }

    @Test
    void sinkCatalogPropsIsRequired() {
        Assertions.assertTrue(
                absolutelyRequiredOptions(SINK_RULE).contains(IcebergSinkOptions.CATALOG_PROPS));
    }

    @Test
    void sinkUpsertConditionallyRequiresPrimaryKeys() {
        boolean hasConditional =
                SINK_RULE.getRequiredOptions().stream()
                        .filter(o -> o instanceof RequiredOption.ConditionalRequiredOptions)
                        .anyMatch(
                                o ->
                                        o.getOptions()
                                                .contains(IcebergSinkOptions.TABLE_PRIMARY_KEYS));
        Assertions.assertTrue(hasConditional);
    }

    @Test
    void sourceScanIntervalHasValueConstraint() {
        boolean hasConstraint =
                SOURCE_RULE.getValueConstraints().stream()
                        .anyMatch(
                                c ->
                                        c.getOption()
                                                .equals(
                                                        IcebergSourceOptions
                                                                .KEY_INCREMENT_SCAN_INTERVAL));
        Assertions.assertTrue(hasConstraint);
    }

    // accepted configs

    @Test
    void sourceMinimalSingleTableValid() {
        Assertions.assertDoesNotThrow(() -> validateSource(sourceConfigWithTable()));
    }

    @Test
    void sourceValidWithoutCatalogNameAndNamespace() {
        Map<String, Object> config = sourceConfigWithTable();
        config.remove("catalog_name");
        config.remove("namespace");
        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    @Test
    void sourceTableListValid() {
        Map<String, Object> config = baseSourceConfig();
        Map<String, Object> t1 = new HashMap<>();
        t1.put("table", "t1");
        Map<String, Object> t2 = new HashMap<>();
        t2.put("table", "t2");
        config.put("table_list", Arrays.asList(t1, t2));
        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    @Test
    void sinkValidWithoutTable() {
        Map<String, Object> config = new HashMap<>();
        config.put("iceberg.catalog.config", catalogProps());
        Assertions.assertDoesNotThrow(() -> validateSink(config));
    }

    @Test
    void sinkUpsertWithPrimaryKeysValid() {
        Map<String, Object> config = new HashMap<>();
        config.put("iceberg.catalog.config", catalogProps());
        config.put("iceberg.table.upsert-mode-enabled", true);
        config.put("iceberg.table.primary-keys", "id");
        Assertions.assertDoesNotThrow(() -> validateSink(config));
    }

    @Test
    void sinkNoUpsertWithoutPrimaryKeysValid() {
        Map<String, Object> config = new HashMap<>();
        config.put("iceberg.catalog.config", catalogProps());
        config.put("iceberg.table.upsert-mode-enabled", false);
        Assertions.assertDoesNotThrow(() -> validateSink(config));
    }

    @Test
    void sourcePositiveScanIntervalValid() {
        Map<String, Object> config = sourceConfigWithTable();
        config.put("increment.scan-interval", 2000L);
        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    // rejected configs

    @Test
    void sourceMissingCatalogPropsRejected() {
        Map<String, Object> config = sourceConfigWithTable();
        config.remove("iceberg.catalog.config");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @Test
    void sourceMissingBothTableAndTableListRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validateSource(baseSourceConfig()));
    }

    @Test
    void sourceBothTableAndTableListRejected() {
        Map<String, Object> config = sourceConfigWithTable();
        Map<String, Object> t1 = new HashMap<>();
        t1.put("table", "t1");
        config.put("table_list", Collections.singletonList(t1));
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @Test
    void sinkMissingCatalogPropsRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("table", "t1");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    @Test
    void sinkUpsertWithoutPrimaryKeysRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("iceberg.catalog.config", catalogProps());
        config.put("iceberg.table.upsert-mode-enabled", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    @Test
    void sinkUpsertWithBlankPrimaryKeysRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("iceberg.catalog.config", catalogProps());
        config.put("iceberg.table.upsert-mode-enabled", true);
        config.put("iceberg.table.primary-keys", "  ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L})
    void sourceNonPositiveScanIntervalRejected(long interval) {
        Map<String, Object> config = sourceConfigWithTable();
        config.put("increment.scan-interval", interval);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    // helpers

    /**
     * Returns only the options that ConfigValidator treats as unconditionally required (i.e. {@link
     * RequiredOption.AbsolutelyRequiredOptions}). Exclusive-group (e.g. table/table_list) and
     * conditional requirements are intentionally excluded: they are enforced by separate validation
     * paths and are asserted separately in this test.
     */
    private static List<Option<?>> absolutelyRequiredOptions(OptionRule rule) {
        return rule.getRequiredOptions().stream()
                .filter(o -> o instanceof RequiredOption.AbsolutelyRequiredOptions)
                .flatMap(o -> o.getOptions().stream())
                .collect(Collectors.toList());
    }

    private static void validateSource(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(SOURCE_RULE);
    }

    private static void validateSink(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(SINK_RULE);
    }

    private static Map<String, Object> catalogProps() {
        Map<String, Object> props = new HashMap<>();
        props.put("type", "hadoop");
        props.put("warehouse", "file:///tmp/seatunnel/iceberg/");
        return props;
    }

    private static Map<String, Object> baseSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("catalog_name", "seatunnel");
        config.put("namespace", "database1");
        config.put("iceberg.catalog.config", catalogProps());
        return config;
    }

    private static Map<String, Object> sourceConfigWithTable() {
        Map<String, Object> config = baseSourceConfig();
        config.put("table", "source_table");
        return config;
    }
}
