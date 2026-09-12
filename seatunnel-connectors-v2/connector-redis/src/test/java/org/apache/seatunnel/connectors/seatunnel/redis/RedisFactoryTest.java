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

package org.apache.seatunnel.connectors.seatunnel.redis;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.redis.sink.RedisSink;
import org.apache.seatunnel.connectors.seatunnel.redis.sink.RedisSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.redis.source.RedisSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class RedisFactoryTest {

    private static final OptionRule SOURCE_RULE = new RedisSourceFactory().optionRule();
    private static final OptionRule SINK_RULE = new RedisSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(SOURCE_RULE);
        Assertions.assertNotNull(SINK_RULE);
    }

    // accepted configs

    @Test
    void sourceSingleModeValid() {
        Assertions.assertDoesNotThrow(() -> validateSource(singleSourceConfig()));
    }

    @Test
    void sourceClusterModeValid() {
        Map<String, Object> config = clusterSourceConfig();
        config.put("nodes", Arrays.asList("127.0.0.1:7000", "127.0.0.1:7001"));
        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    @Test
    void sourceMultiTableValid() {
        Assertions.assertDoesNotThrow(() -> validateSource(multiTableSourceConfig()));
    }

    @Test
    void sinkSingleModeValid() {
        Assertions.assertDoesNotThrow(() -> validateSink(singleSinkConfig()));
    }

    // rejected configs

    @ParameterizedTest(name = "source single-mode connection rejected: {0}")
    @MethodSource("invalidSingleConnections")
    void sourceSingleModeInvalidConnectionRejected(String name, Map<String, Object> config) {
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @ParameterizedTest(name = "source cluster-mode nodes rejected: {0}")
    @MethodSource("invalidClusterNodes")
    void sourceClusterModeInvalidNodesRejected(String name, Map<String, Object> config) {
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @ParameterizedTest(name = "source table config rejected: {0}")
    @MethodSource("invalidTableConfigs")
    void sourceTableConfigRejected(String name, Map<String, Object> config) {
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @ParameterizedTest(name = "sink config rejected: {0}")
    @MethodSource("invalidSinkConfigs")
    void sinkConfigRejected(String name, Map<String, Object> config) {
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    @Test
    void sourceSingleModeAggregatesMultipleErrors() {
        Map<String, Object> config = singleSourceConfig();
        config.put("host", "   ");
        config.put("port", 70000);
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSource(config));
        String message = ex.getMessage();
        Assertions.assertTrue(message.contains("(2 error"), message);
        Assertions.assertTrue(message.contains("host"), message);
        Assertions.assertTrue(message.contains("port"), message);
    }

    // factory-context regression

    @Test
    void sourceCreatedViaFactoryContext() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(singleSourceConfig());
        ConfigValidator.of(config).validate(SOURCE_RULE);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());
        Assertions.assertDoesNotThrow(
                () -> new RedisSourceFactory().createSource(context).createSource());
    }

    @Test
    void sinkCreatedViaFactoryContext() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(singleSinkConfig());
        ConfigValidator.of(config).validate(SINK_RULE);
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable(), config, Thread.currentThread().getContextClassLoader());
        Assertions.assertDoesNotThrow(
                () -> new RedisSinkFactory().createSink(context).createSink());
    }

    @Test
    void sinkExposesSchemaWriterStateSerializer() throws IOException {
        RedisSink sink = new RedisSink(ReadonlyConfig.fromMap(singleSinkConfig()), catalogTable());

        Assertions.assertTrue(sink.getWriterStateSerializer().isPresent());
        @SuppressWarnings("unchecked")
        Serializer<TableSchema> serializer =
                (Serializer<TableSchema>) (Serializer<?>) sink.getWriterStateSerializer().get();
        TableSchema tableSchema = catalogTable().getTableSchema();

        Assertions.assertEquals(
                tableSchema, serializer.deserialize(serializer.serialize(tableSchema)));
    }

    // parameterized-case providers

    static Stream<Arguments> invalidSingleConnections() {
        Map<String, Object> blankHost = singleSourceConfig();
        blankHost.put("host", "   ");
        Map<String, Object> portTooSmall = singleSourceConfig();
        portTooSmall.put("port", 0);
        Map<String, Object> portTooLarge = singleSourceConfig();
        portTooLarge.put("port", 70000);
        return Stream.of(
                Arguments.of("blank host", blankHost),
                Arguments.of("port below range", portTooSmall),
                Arguments.of("port above range", portTooLarge));
    }

    static Stream<Arguments> invalidClusterNodes() {
        return Stream.of(
                Arguments.of("empty nodes", clusterNodes(Collections.emptyList())),
                Arguments.of(
                        "malformed node",
                        clusterNodes(Arrays.asList("127.0.0.1:7000", "host-without-port"))),
                Arguments.of(
                        "port out of range",
                        clusterNodes(Arrays.asList("127.0.0.1:7000", "127.0.0.1:99999"))),
                Arguments.of(
                        "blank host in node",
                        clusterNodes(Arrays.asList("127.0.0.1:7000", "   :7001"))));
    }

    static Stream<Arguments> invalidTableConfigs() {
        Map<String, Object> singleBlankKeys = singleSourceConfig();
        singleBlankKeys.put("keys", "   ");
        Map<String, Object> singleMissingDataType = singleSourceConfig();
        singleMissingDataType.remove("data_type");
        Map<String, Object> multiBlankKeysInEntry = multiTableSourceConfig();
        multiBlankKeysInEntry.put(
                "tables_configs", Arrays.asList(tableEntry("key_a*"), tableEntry("")));
        Map<String, Object> entryMissingDataType = new HashMap<>();
        entryMissingDataType.put("keys", "key_a*");
        Map<String, Object> multiMissingDataType = multiTableSourceConfig();
        multiMissingDataType.put("tables_configs", Collections.singletonList(entryMissingDataType));
        Map<String, Object> multiEmptyList = multiTableSourceConfig();
        multiEmptyList.put("tables_configs", Collections.emptyList());
        return Stream.of(
                Arguments.of("single: blank keys", singleBlankKeys),
                Arguments.of("single: missing data_type", singleMissingDataType),
                Arguments.of("multi: blank keys in entry", multiBlankKeysInEntry),
                Arguments.of("multi: missing data_type in entry", multiMissingDataType),
                Arguments.of("multi: empty list", multiEmptyList));
    }

    static Stream<Arguments> invalidSinkConfigs() {
        Map<String, Object> portOutOfRange = singleSinkConfig();
        portOutOfRange.put("port", -1);
        Map<String, Object> clusterMalformedNode = singleSinkConfig();
        clusterMalformedNode.remove("host");
        clusterMalformedNode.remove("port");
        clusterMalformedNode.put("mode", "CLUSTER");
        clusterMalformedNode.put("nodes", Collections.singletonList("bad_node"));
        return Stream.of(
                Arguments.of("single: port out of range", portOutOfRange),
                Arguments.of("cluster: malformed node", clusterMalformedNode));
    }

    // helpers

    private static void validateSource(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(SOURCE_RULE);
    }

    private static void validateSink(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(SINK_RULE);
    }

    private static Map<String, Object> clusterNodes(List<String> nodes) {
        Map<String, Object> config = clusterSourceConfig();
        config.put("nodes", nodes);
        return config;
    }

    private static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "default", null, "test_table"),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                null,
                "catalog");
    }

    private static Map<String, Object> singleSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "SINGLE");
        config.put("host", "localhost");
        config.put("port", 6379);
        config.put("keys", "key_test*");
        config.put("data_type", "KEY");
        return config;
    }

    private static Map<String, Object> clusterSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "CLUSTER");
        config.put("keys", "key_test*");
        config.put("data_type", "KEY");
        return config;
    }

    private static Map<String, Object> multiTableSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 6379);
        config.put("tables_configs", Arrays.asList(tableEntry("key_a*"), tableEntry("key_b*")));
        return config;
    }

    private static Map<String, Object> tableEntry(String keys) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("keys", keys);
        entry.put("data_type", "KEY");
        return entry;
    }

    private static Map<String, Object> singleSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "SINGLE");
        config.put("host", "localhost");
        config.put("port", 6379);
        config.put("key", "my_key");
        config.put("data_type", "KEY");
        return config;
    }
}
