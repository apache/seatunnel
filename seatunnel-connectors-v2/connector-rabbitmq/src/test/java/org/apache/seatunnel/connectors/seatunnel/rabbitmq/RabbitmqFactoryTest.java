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
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.sink.RabbitmqSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class RabbitmqFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new RabbitmqSourceFactory()).optionRule());
        Assertions.assertNotNull((new RabbitmqSinkFactory()).optionRule());
    }

    @Test
    void readsLegacyUriOption() {
        Map<String, Object> options = new HashMap<>();
        options.put(RabbitmqBaseOptions.URI.key(), "amqps://guest:guest@localhost:5671/%2F");

        RabbitmqConfig config = new RabbitmqConfig(ReadonlyConfig.fromMap(options));

        Assertions.assertEquals("amqps://guest:guest@localhost:5671/%2F", config.getUri());
    }

    @Test
    void readsSecurePassiveOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        options.put(RabbitmqBaseOptions.PORT.key(), 5671);
        options.put(RabbitmqBaseOptions.SSL.key(), true);
        options.put(RabbitmqBaseOptions.PASSIVE.key(), true);

        RabbitmqConfig config = new RabbitmqConfig(ReadonlyConfig.fromMap(options));

        Assertions.assertTrue(config.isSsl());
        Assertions.assertTrue(config.isPassive());
    }

    @Test
    void rejectsUrlAndUriSetTogether() {
        Map<String, Object> options = new HashMap<>();
        options.put(RabbitmqBaseOptions.URL.key(), "amqp://host-a:5672/%2F");
        options.put(RabbitmqBaseOptions.URI.key(), "amqps://host-b:5671/%2F");

        Assertions.assertThrows(
                RabbitmqConnectorException.class,
                () -> new RabbitmqConfig(ReadonlyConfig.fromMap(options)));
    }
}
