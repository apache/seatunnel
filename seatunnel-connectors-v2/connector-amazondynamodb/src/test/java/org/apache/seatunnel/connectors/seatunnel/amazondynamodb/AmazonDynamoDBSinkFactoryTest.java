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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink.AmazonDynamoDBSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions.URL;

/** Tests declarative sink validation exposed by {@link AmazonDynamoDBSinkFactory}. */
public class AmazonDynamoDBSinkFactoryTest {

    private OptionRule sinkRule;

    @BeforeEach
    void setUp() {
        sinkRule = new AmazonDynamoDBSinkFactory().optionRule();
    }

    @Test
    void testOmittedMaxRetriesUsesDefault() {
        Map<String, Object> config = requiredOptions();

        Assertions.assertDoesNotThrow(() -> validate(config));
        Assertions.assertEquals(10, ReadonlyConfig.fromMap(config).get(MAX_RETRIES));
    }

    @Test
    void testNonNegativeMaxRetries() {
        Assertions.assertDoesNotThrow(() -> validateMaxRetries(0));
        Assertions.assertDoesNotThrow(() -> validateMaxRetries(3));
    }

    @Test
    void testNegativeMaxRetriesFails() {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateMaxRetries(-1));

        Assertions.assertTrue(exception.getMessage().contains(MAX_RETRIES.key()));
        Assertions.assertTrue(exception.getMessage().contains(">= 0"));
    }

    @Test
    void testMaxRetriesIsADeclaredOption() {
        Map<String, Object> config = requiredOptions();
        config.put(MAX_RETRIES.key(), 3);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), sinkRule, "AmazonDynamoDB"));
    }

    private void validateMaxRetries(int maxRetries) {
        Map<String, Object> config = requiredOptions();
        config.put(MAX_RETRIES.key(), maxRetries);
        validate(config);
    }

    private Map<String, Object> requiredOptions() {
        Map<String, Object> config = new HashMap<>();
        config.put(URL.key(), "http://localhost:8000");
        config.put(REGION.key(), "us-east-1");
        config.put(ACCESS_KEY_ID.key(), "access-key");
        config.put(SECRET_ACCESS_KEY.key(), "secret-key");
        config.put(TABLE.key(), "orders");
        return config;
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sinkRule);
    }
}
