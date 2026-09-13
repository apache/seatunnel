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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.sink.RabbitmqSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RabbitmqSinkFactoryTest {

    private void validate(Map<String, Object> configMap) {
        RabbitmqSinkFactory rabbitmqSinkFactory = new RabbitmqSinkFactory();
        ConfigValidator.of(ReadonlyConfig.fromMap(configMap))
                .validate(rabbitmqSinkFactory.optionRule());
    }

    @Test
    public void testValidProtobufConfig() {
        Map<String, Object> config = createValidConfig();
        config.put(RabbitmqSinkOptions.FORMAT.key(), RabbitmqMessageFormat.PROTOBUF);
        config.put(RabbitmqSinkOptions.PROTOBUF_SCHEMA.key(), "syntax = \"proto3\";");
        config.put(RabbitmqSinkOptions.PROTOBUF_MESSAGE_NAME.key(), "TestMessage");

        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    public void testProtobufRequiresSchemaAndMessageName() {
        Map<String, Object> config = createValidConfig();
        config.put(RabbitmqSinkOptions.FORMAT.key(), RabbitmqMessageFormat.PROTOBUF);

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSinkOptions.PROTOBUF_SCHEMA.key()));
    }

    @Test
    public void testFormatIsRegisteredAsOptionalOption() {
        RabbitmqSinkFactory factory = new RabbitmqSinkFactory();

        boolean hasFormat =
                factory.optionRule().getOptionalOptions().stream()
                        .anyMatch(option -> option.key().equals(RabbitmqSinkOptions.FORMAT.key()));

        Assertions.assertTrue(hasFormat, "FORMAT should be registered as an optional option");
    }

    private Map<String, Object> createValidConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSinkOptions.HOST.key(), "localhost");
        config.put(RabbitmqSinkOptions.PORT.key(), 5672);
        config.put(RabbitmqSinkOptions.VIRTUAL_HOST.key(), "/");
        config.put(RabbitmqSinkOptions.QUEUE_NAME.key(), "test_queue");
        return config;
    }
}
