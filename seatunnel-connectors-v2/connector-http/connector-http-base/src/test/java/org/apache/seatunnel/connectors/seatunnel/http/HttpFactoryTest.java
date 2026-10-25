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

package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

class HttpFactoryTest {

    private final OptionRule sinkOptionRule = new HttpSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull((new HttpSourceFactory()).optionRule());
        Assertions.assertNotNull(sinkOptionRule);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://localhost/test/webhook",
                "https://example.com/webhook",
                "http://localhost/test/${database_name}_test/${table_name}_test",
                "http://localhost/test/${schema_name}_test/${table_name}_test",
                " \thttp://localhost/test/webhook\r\n "
            })
    void testNonBlankSinkUrlAcceptedWithoutModification(String url) {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(Collections.singletonMap(HttpSinkOptions.URL.key(), url));

        Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(sinkOptionRule));
        Assertions.assertEquals(url, config.get(HttpSinkOptions.URL));
    }

    @Test
    void testMissingSinkUrlRejected() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());

        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(config).validate(sinkOptionRule));
        Assertions.assertTrue(error.getMessage().contains(HttpSinkOptions.URL.key()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankSinkUrlRejected(String url) {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(Collections.singletonMap(HttpSinkOptions.URL.key(), url));

        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(config).validate(sinkOptionRule));
        Assertions.assertTrue(error.getMessage().contains(HttpSinkOptions.URL.key()));
    }
}
