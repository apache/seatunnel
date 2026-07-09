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

package org.apache.seatunnel.connectors.seatunnel.mqtt.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class MqttSourceFactoryTest {

    @Test
    void testFactoryIdentifier() {
        MqttSourceFactory factory = new MqttSourceFactory();

        Assertions.assertEquals(MqttSourceOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        Assertions.assertEquals(MqttSource.class, factory.getSourceClass());
    }

    @Test
    void testOptionRule() {
        MqttSourceFactory factory = new MqttSourceFactory();
        OptionRule rule = factory.optionRule();

        List<Option<?>> requiredOptions =
                rule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(requiredOptions.contains(MqttSourceOptions.URL));
        Assertions.assertTrue(requiredOptions.contains(MqttSourceOptions.TOPIC));
        Assertions.assertTrue(requiredOptions.contains(ConnectorCommonOptions.SCHEMA));

        List<Option<?>> optionalOptions = rule.getOptionalOptions();
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.USERNAME));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.PASSWORD));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.QOS));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.FORMAT));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.FIELD_DELIMITER));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CLIENT_ID));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CLEAN_SESSION));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CONNECTION_TIMEOUT));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.KEEP_ALIVE_INTERVAL));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.MAX_QUEUE_SIZE));
    }
}
