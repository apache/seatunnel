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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MilvusSinkFactoryTest {

    private final MilvusSinkFactory factory = new MilvusSinkFactory();

    @Test
    void testCollectionNameAliasUsesTargetCollection() {
        SeaTunnelSink<?, ?, ?, ?> sink =
                createSeaTunnelSink(
                        config(
                                "collection_name",
                                "user_data",
                                MilvusSinkOptions.DATABASE.key(),
                                "data_goverment_test"));

        CatalogTable writeCatalogTable = writeCatalogTable(sink);
        assertEquals("data_goverment_test", writeCatalogTable.getTablePath().getDatabaseName());
        assertEquals("user_data", writeCatalogTable.getTablePath().getTableName());
    }

    @Test
    void testCollectionUsesTargetCollection() {
        SeaTunnelSink<?, ?, ?, ?> sink =
                createSeaTunnelSink(config(MilvusSinkOptions.COLLECTION.key(), "user_data"));

        CatalogTable writeCatalogTable = writeCatalogTable(sink);
        assertEquals("source_database", writeCatalogTable.getTablePath().getDatabaseName());
        assertEquals("user_data", writeCatalogTable.getTablePath().getTableName());
    }

    @Test
    void testCollectionTakesPrecedenceOverCollectionNameAlias() {
        SeaTunnelSink<?, ?, ?, ?> sink =
                createSeaTunnelSink(
                        config(
                                MilvusSinkOptions.COLLECTION.key(),
                                "canonical_collection",
                                "collection_name",
                                "alias_collection"));

        assertEquals("canonical_collection", writeCatalogTable(sink).getTablePath().getTableName());
    }

    @Test
    void testCreateSinkAllowsNullContextForFallbackProbe() {
        TableSink<?, ?, ?, ?> tableSink = assertDoesNotThrow(() -> factory.createSink(null));
        assertNotNull(tableSink);
    }

    @Test
    void testOptionRuleIncludesSinkOptions() {
        List<Option<?>> declaredOptions = declaredOptions(factory.optionRule());

        assertTrue(declaredOptions.contains(MilvusSinkOptions.DATABASE));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.COLLECTION));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.BATCH_SIZE));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.PARTITION_KEY));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.COLLECTION_DESCRIPTION));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.LOAD_COLLECTION));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.CREATE_INDEX));
        assertTrue(declaredOptions.contains(MilvusSinkOptions.RATE_LIMIT));
    }

    private SeaTunnelSink<?, ?, ?, ?> createSeaTunnelSink(Map<String, Object> options) {
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable(),
                        ReadonlyConfig.fromMap(options),
                        Thread.currentThread().getContextClassLoader());
        return factory.createSink(context).createSink();
    }

    private CatalogTable writeCatalogTable(SeaTunnelSink<?, ?, ?, ?> sink) {
        return sink.getWriteCatalogTable()
                .orElseThrow(() -> new AssertionError("Expected sink write catalog table"));
    }

    private Map<String, Object> config(Object... keysAndValues) {
        Map<String, Object> options = new HashMap<>();
        options.put(MilvusSinkOptions.URL.key(), "http://localhost:19530");
        options.put(MilvusSinkOptions.TOKEN.key(), "root:Milvus");
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return options;
    }

    private CatalogTable catalogTable() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of(
                                "uniq_id", BasicType.STRING_TYPE, 0L, false, null, "Unique id"),
                        PhysicalColumn.of(
                                "profile", BasicType.STRING_TYPE, 0L, true, null, "Profile"));

        return CatalogTable.of(
                TableIdentifier.of("S3File", "source_database", "raw_data"),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Arrays.asList(),
                "Source table");
    }

    private List<Option<?>> declaredOptions(OptionRule rule) {
        return Stream.concat(
                        rule.getRequiredOptions().stream()
                                .flatMap(option -> option.getOptions().stream()),
                        rule.getOptionalOptions().stream())
                .collect(Collectors.toList());
    }
}
