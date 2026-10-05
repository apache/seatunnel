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

package org.apache.seatunnel.connectors.seatunnel;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.source.Web3jSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

class Web3jFactoryTest {

    private final OptionRule optionRule = new Web3jSourceFactory().optionRule();

    @Test
    void testNonblankUrlAccepted() {
        for (String url :
                new String[] {
                    "http://localhost:8545", "https://example.com", " custom-endpoint "
                }) {
            Assertions.assertDoesNotThrow(() -> validate(Collections.singletonMap("url", url)));
        }
    }

    @Test
    void testMissingUrlRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(Collections.emptyMap()));
    }

    @Test
    void testEmptyUrlRejected() {
        assertInvalidUrl("");
    }

    @Test
    void testWhitespaceOnlyUrlRejected() {
        assertInvalidUrl(" ");
        assertInvalidUrl("\t\r\n");
    }

    private void assertInvalidUrl(String url) {
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(Collections.singletonMap("url", url)));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "Web3j");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }
}
