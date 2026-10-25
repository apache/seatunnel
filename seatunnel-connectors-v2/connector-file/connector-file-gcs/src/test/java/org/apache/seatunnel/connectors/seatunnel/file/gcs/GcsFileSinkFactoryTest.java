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

package org.apache.seatunnel.connectors.seatunnel.file.gcs;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.catalog.GcsFileCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.config.GcsFileSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.config.GcsHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.sink.GcsFileSinkFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsFileSinkFactoryTest {

    @Test
    void shouldIdentifyGcsFileSinkAndCatalog() {
        assertEquals("GcsFile", new GcsFileSinkFactory().factoryIdentifier());
        assertEquals("GcsFile", new GcsFileCatalogFactory().factoryIdentifier());
    }

    @Test
    void shouldRegisterSinkAndCatalogFactories() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        assertEquals(
                GcsFileSinkFactory.class,
                discoverFactory(classLoader, TableSinkFactory.class, "GcsFile").getClass());
        assertEquals(
                GcsFileCatalogFactory.class,
                discoverFactory(classLoader, CatalogFactory.class, "GcsFile").getClass());
    }

    @Test
    void shouldRequirePathAndBucket() {
        OptionRule optionRule = new GcsFileSinkFactory().optionRule();
        Map<String, Object> config = sinkConfig();

        assertDoesNotThrow(() -> validate(config, optionRule));

        config.remove(FileBaseSinkOptions.FILE_PATH.key());
        assertThrows(OptionValidationException.class, () -> validate(config, optionRule));

        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/warehouse/orders");
        config.remove(GcsFileSinkOptions.BUCKET.key());
        assertThrows(OptionValidationException.class, () -> validate(config, optionRule));
    }

    @Test
    void shouldExposeAuthenticationSaveModeAndMultiTableOptions() {
        OptionRule optionRule = new GcsFileSinkFactory().optionRule();

        assertTrue(optionRuleContains(optionRule, GcsFileSinkOptions.SERVICE_ACCOUNT_KEY_FILE));
        assertTrue(optionRuleContains(optionRule, GcsFileSinkOptions.GCS_PROPERTIES));
        assertTrue(optionRuleContains(optionRule, FileBaseSinkOptions.SCHEMA_SAVE_MODE));
        assertTrue(optionRuleContains(optionRule, FileBaseSinkOptions.DATA_SAVE_MODE));
        assertTrue(
                optionRuleContains(
                        optionRule, SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA));
    }

    @Test
    void shouldRejectInvalidBucketForSinkConfiguration() {
        Map<String, Object> config = sinkConfig();
        config.put(GcsFileSinkOptions.BUCKET.key(), "test-bucket");

        assertThrows(
                IllegalArgumentException.class,
                () -> GcsHadoopConf.buildWithReadonlyConfig(ReadonlyConfig.fromMap(config)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/",
                "/./",
                "/warehouse/..",
                "/warehouse/../",
                "gs://test-bucket",
                "gs://test-bucket/",
                "gs://test-bucket/warehouse/.."
            })
    void shouldRejectNormalizedBucketRootForDropData(String path) {
        Map<String, Object> config = sinkConfig();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), path);
        config.put(FileBaseSinkOptions.DATA_SAVE_MODE.key(), "DROP_DATA");

        assertThrows(
                IllegalArgumentException.class,
                () -> new GcsFileSinkFactory().createSink(sinkContext(config)));
    }

    @Test
    void shouldRejectBucketRootForRecreateSchemaAfterPlaceholderResolution() {
        Map<String, Object> config = sinkConfig();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/${table_name}/..");
        config.put(FileBaseSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new GcsFileSinkFactory().createSink(sinkContext(config)));

        assertTrue(error.getMessage().contains("bucket root"));
    }

    @Test
    void shouldAllowBucketRootForNonDestructiveModes() {
        Map<String, Object> config = sinkConfig();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/");

        assertDoesNotThrow(() -> new GcsFileSinkFactory().createSink(sinkContext(config)));
    }

    @Test
    void shouldAllowNormalizedPrefixForDestructiveModes() {
        Map<String, Object> config = sinkConfig();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/warehouse/../orders");
        config.put(FileBaseSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");
        config.put(FileBaseSinkOptions.DATA_SAVE_MODE.key(), "DROP_DATA");

        assertDoesNotThrow(() -> new GcsFileSinkFactory().createSink(sinkContext(config)));
    }

    private static Map<String, Object> sinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/warehouse/orders");
        config.put(GcsFileSinkOptions.BUCKET.key(), "gs://test-bucket");
        config.put(FileBaseSinkOptions.FILE_FORMAT_TYPE.key(), "parquet");
        return config;
    }

    private static boolean optionRuleContains(OptionRule optionRule, Option<?> option) {
        if (optionRule.getOptionalOptions().contains(option)) {
            return true;
        }
        return optionRule.getRequiredOptions().stream()
                .anyMatch(requiredOption -> requiredOption.getOptions().contains(option));
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
    }

    private static TableSinkFactoryContext sinkContext(Map<String, Object> config) {
        return TableSinkFactoryContext.replacePlaceholderAndCreate(
                CatalogTableUtil.buildSimpleTextTable(),
                ReadonlyConfig.fromMap(config),
                Thread.currentThread().getContextClassLoader(),
                Collections.emptyList());
    }
}
