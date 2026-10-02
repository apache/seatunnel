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

package org.apache.seatunnel.connectors.seatunnel.kudu;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.kudu.catalog.KuduCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.kudu.sink.KuduSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.kudu.source.KuduSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class KuduFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new KuduSourceFactory()).optionRule());
        Assertions.assertNotNull((new KuduSinkFactory()).optionRule());
        Assertions.assertNotNull((new KuduCatalogFactory()).optionRule());
    }

    @ParameterizedTest
    @MethodSource("factories")
    void missingMastersAreRejected(Factory factory) {
        Map<String, Object> options = validOptions(factory);
        options.remove(KuduBaseOptions.MASTER.key());

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(factory, options));
        Assertions.assertTrue(exception.getRawMessage().contains(KuduBaseOptions.MASTER.key()));
    }

    @ParameterizedTest
    @MethodSource("blankMasters")
    void blankMastersAreRejected(Factory factory, String masters) {
        Map<String, Object> options = validOptions(factory);
        options.put(KuduBaseOptions.MASTER.key(), masters);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(factory, options));
        Assertions.assertTrue(exception.getRawMessage().contains(KuduBaseOptions.MASTER.key()));
        Assertions.assertTrue(exception.getRawMessage().contains("is not blank"));
    }

    @ParameterizedTest
    @MethodSource("nonblankMasters")
    void nonblankMastersArePreserved(Factory factory, String masters) {
        Map<String, Object> options = validOptions(factory);
        options.put(KuduBaseOptions.MASTER.key(), masters);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(config).validate(factory.optionRule()));
        Assertions.assertEquals(masters, config.get(KuduBaseOptions.MASTER));
        Assertions.assertEquals(masters, new CommonConfig(config).getMasters());
    }

    @Test
    void sourceAcceptsTableListInsteadOfTableName() {
        KuduSourceFactory factory = new KuduSourceFactory();
        Map<String, Object> options = validOptions(factory);
        options.remove(KuduBaseOptions.TABLE_NAME.key());
        options.put(
                ConnectorCommonOptions.TABLE_LIST.key(),
                Collections.singletonList(
                        Collections.singletonMap(KuduBaseOptions.TABLE_NAME.key(), "test_table")));

        Assertions.assertDoesNotThrow(() -> validate(factory, options));
    }

    @Test
    void sourceStillRequiresExactlyOneTableSelection() {
        KuduSourceFactory factory = new KuduSourceFactory();
        Map<String, Object> options = validOptions(factory);
        options.remove(KuduBaseOptions.TABLE_NAME.key());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(factory, options));

        options.put(KuduBaseOptions.TABLE_NAME.key(), "test_table");
        options.put(
                ConnectorCommonOptions.TABLE_LIST.key(),
                Collections.singletonList(
                        Collections.singletonMap(KuduBaseOptions.TABLE_NAME.key(), "test_table")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(factory, options));
    }

    private static Stream<Factory> factories() {
        return Stream.of(new KuduSourceFactory(), new KuduSinkFactory(), new KuduCatalogFactory());
    }

    private static Stream<Arguments> blankMasters() {
        return masterValues("", "   ", "\t", " \t\r\n ");
    }

    private static Stream<Arguments> nonblankMasters() {
        return masterValues(
                "kudu-master:7051",
                "kudu-master-1:7051,kudu-master-2:7051",
                "  kudu-master:7051  ");
    }

    private static Stream<Arguments> masterValues(String... masters) {
        return factories()
                .flatMap(
                        factory ->
                                Arrays.stream(masters).map(value -> Arguments.of(factory, value)));
    }

    private static Map<String, Object> validOptions(Factory factory) {
        Map<String, Object> options = new HashMap<>();
        options.put(KuduBaseOptions.MASTER.key(), "kudu-master:7051");
        if (factory instanceof KuduSourceFactory) {
            options.put(KuduBaseOptions.TABLE_NAME.key(), "test_table");
        }
        return options;
    }

    private static void validate(Factory factory, Map<String, Object> options) {
        ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(factory.optionRule());
    }
}
