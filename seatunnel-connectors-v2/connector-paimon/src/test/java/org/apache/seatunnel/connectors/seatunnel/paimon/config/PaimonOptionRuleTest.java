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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceFactory;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonOptionRuleTest {

    @Test
    void testRestOptionsDeclaredByAllFactories() {
        for (OptionRule rule :
                Arrays.asList(
                        new PaimonSourceFactory().optionRule(),
                        new PaimonSinkFactory().optionRule(),
                        new PaimonCatalogFactory().optionRule())) {
            assertDeclaredOptions(
                    rule,
                    PaimonBaseOptions.TOKEN_PROVIDER,
                    PaimonBaseOptions.TOKEN,
                    PaimonBaseOptions.DLF_ACCESS_KEY_ID,
                    PaimonBaseOptions.DLF_ACCESS_KEY_SECRET,
                    PaimonBaseOptions.DLF_SECURITY_TOKEN,
                    PaimonBaseOptions.DLF_REGION);
            Map<String, Object> options = new HashMap<>();
            options.put("catalog_type", "rest");
            options.put("warehouse", "managed-warehouse");
            options.put("database", "test_db");
            options.put("table", "test_table");
            assertThrows(
                    OptionValidationException.class,
                    () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(rule));
            options.put("catalog_uri", "https://catalog.example.com");
            assertThrows(
                    OptionValidationException.class,
                    () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(rule));
            options.put("token.provider", "bear");
            options.put("token", "test-bearer");
            assertDoesNotThrow(
                    () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(rule));
        }
    }

    @Test
    void testSinkOptionRuleDeclaresNonPrimaryKey() {
        assertDeclaredOptions(
                new PaimonSinkFactory().optionRule(), PaimonSinkOptions.NON_PRIMARY_KEY);
    }

    @Test
    void testCatalogOptionRuleDeclaresNonPrimaryKey() {
        assertDeclaredOptions(
                new PaimonCatalogFactory().optionRule(), PaimonSinkOptions.NON_PRIMARY_KEY);
    }

    @Test
    void testSinkOptionRuleDeclaresTableOptions() {
        assertDeclaredOptions(
                new PaimonSinkFactory().optionRule(), SinkConnectorCommonOptions.TABLE_OPTIONS);
    }

    @Test
    void testCatalogOptionRuleDeclaresTableOptions() {
        assertDeclaredOptions(
                new PaimonCatalogFactory().optionRule(), SinkConnectorCommonOptions.TABLE_OPTIONS);
    }

    @Test
    void testSourceOptionRuleDeclaresBaseOptionalOptions() {
        assertDeclaredOptions(
                new PaimonSourceFactory().optionRule(),
                PaimonBaseOptions.CATALOG_NAME,
                PaimonBaseOptions.USER,
                PaimonBaseOptions.PASSWORD);
    }

    @Test
    void testSinkOptionRuleDeclaresBaseOptionalOptions() {
        assertDeclaredOptions(
                new PaimonSinkFactory().optionRule(),
                PaimonBaseOptions.CATALOG_NAME,
                PaimonBaseOptions.USER,
                PaimonBaseOptions.PASSWORD);
    }

    @Test
    void testCatalogOptionRuleDeclaresBaseOptionalOptions() {
        assertDeclaredOptions(
                new PaimonCatalogFactory().optionRule(),
                PaimonBaseOptions.CATALOG_NAME,
                PaimonBaseOptions.USER,
                PaimonBaseOptions.PASSWORD);
    }

    private void assertDeclaredOptions(OptionRule rule, Option<?>... expectedOptions) {
        List<Option<?>> declaredOptions = declaredOptions(rule);
        Arrays.stream(expectedOptions)
                .forEach(
                        option ->
                                assertTrue(
                                        declaredOptions.contains(option),
                                        () -> "Expected option rule to declare " + option.key()));
    }

    private List<Option<?>> declaredOptions(OptionRule rule) {
        return Stream.concat(
                        rule.getRequiredOptions().stream()
                                .flatMap(option -> option.getOptions().stream()),
                        rule.getOptionalOptions().stream())
                .collect(toList());
    }
}
