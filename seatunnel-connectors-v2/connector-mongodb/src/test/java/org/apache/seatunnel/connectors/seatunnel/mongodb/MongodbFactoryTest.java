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

package org.apache.seatunnel.connectors.seatunnel.mongodb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.MongodbSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class MongodbFactoryTest {

    private final MongodbSourceFactory sourceFactory = new MongodbSourceFactory();

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceFactory.optionRule());
        Assertions.assertNotNull(new MongodbSinkFactory().optionRule());
    }

    @Test
    void testDefaultFetchSizePassesOptionValidation() {
        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(validSourceConfig()));
    }

    @Test
    void testPositiveFetchSizePassesOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 1);

        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(config));

        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 2048);
        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(config));
    }

    @Test
    void testZeroFetchSizeFailsOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 0);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSourceOptionRule(config));

        Assertions.assertTrue(
                exception.getMessage().contains(MongodbSourceOptions.FETCH_SIZE.key()));
    }

    @Test
    void testNegativeFetchSizeFailsOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), -1);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSourceOptionRule(config));

        Assertions.assertTrue(
                exception.getMessage().contains(MongodbSourceOptions.FETCH_SIZE.key()));
    }

    private void validateSourceOptionRule(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sourceFactory.optionRule());
    }

    private Map<String, Object> validSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSourceOptions.URI.key(), "mongodb://localhost:27017");
        config.put(MongodbSourceOptions.DATABASE.key(), "test_database");
        config.put(MongodbSourceOptions.COLLECTION.key(), "test_collection");
        config.put(
                ConnectorCommonOptions.SCHEMA.key(),
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        return config;
    }
}
