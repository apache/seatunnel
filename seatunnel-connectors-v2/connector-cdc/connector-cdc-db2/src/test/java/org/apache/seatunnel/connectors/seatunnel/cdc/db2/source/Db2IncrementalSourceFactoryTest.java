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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class Db2IncrementalSourceFactoryTest {
    private static final String VALID_URL = "jdbc:db2://localhost:50000/seatunnel";

    private final OptionRule optionRule = new Db2IncrementalSourceFactory().optionRule();

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new Db2IncrementalSourceFactory()).optionRule());
    }

    @ParameterizedTest
    @MethodSource("nonblankUrls")
    void testNonblankUrlAcceptedWithoutChangingValue(String key, String value) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        config.put(key, value);

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        // Validation does not trim a supplied URL or check whether it can connect.
        Assertions.assertEquals(value, readonlyConfig.get(Db2IncrementalSourceOptions.URL));
    }

    private static Stream<Arguments> nonblankUrls() {
        return Stream.of("url", "base-url")
                .flatMap(
                        key ->
                                Stream.of(VALID_URL, " \t" + VALID_URL + " \r\n")
                                        .map(value -> Arguments.of(key, value)));
    }

    @ParameterizedTest
    @MethodSource("blankUrls")
    void testBlankUrlRejected(String key, String value) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        config.put(key, value);

        assertInvalidUrl(config);
    }

    private static Stream<Arguments> blankUrls() {
        return Stream.of("url", "base-url")
                .flatMap(
                        key ->
                                Stream.of("", " ", "\t", "\n", " \t\r\n ")
                                        .map(value -> Arguments.of(key, value)));
    }

    @Test
    void testMissingUrlRejected() {
        Map<String, Object> config = validConfig();
        config.remove("url");

        assertInvalidUrl(config);
    }

    @Test
    void testCanonicalUrlTakesPrecedenceOverFallback() {
        Map<String, Object> config = validConfig();
        config.put("base-url", " ");

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(VALID_URL, readonlyConfig.get(Db2IncrementalSourceOptions.URL));
    }

    @Test
    void testBlankCanonicalUrlDoesNotUseValidFallback() {
        Map<String, Object> config = validConfig();
        config.put("url", " \t ");
        config.put("base-url", VALID_URL);

        assertInvalidUrl(config);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", VALID_URL);
        config.put("username", "test-user");
        config.put("password", "test-password");
        config.put("table-names", Collections.singletonList("seatunnel.DB2INST1.CUSTOMERS"));
        return config;
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "DB2-CDC");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }

    private void assertInvalidUrl(Map<String, Object> config) {
        OptionValidationException error =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(error.getMessage().contains("url"), error.getMessage());
    }

    /** Verifies Db2 checkpoint tables use the empty-catalog identifier returned by discovery. */
    @Test
    public void testToTableIdDropsCatalog() {
        Db2SourceConfigFactory configFactory = Mockito.mock(Db2SourceConfigFactory.class);
        Mockito.when(configFactory.create(0)).thenReturn(Mockito.mock(Db2SourceConfig.class));
        Db2Dialect dialect = new Db2Dialect(configFactory, Collections.emptyList());

        Assertions.assertEquals(
                new TableId("", "DB2INST1", "CUSTOMERS"),
                dialect.toTableId(TablePath.of("SAMPLE", "DB2INST1", "CUSTOMERS")));
    }
}
