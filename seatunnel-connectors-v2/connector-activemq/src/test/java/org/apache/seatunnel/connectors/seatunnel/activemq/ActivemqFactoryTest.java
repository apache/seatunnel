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

package org.apache.seatunnel.connectors.seatunnel.activemq;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.activemq.sink.ActivemqSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ActivemqFactoryTest {

    private final OptionRule optionRule = new ActivemqSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(requiredConfig()));
    }

    @Test
    void testBlankRequiredOptionsRejected() {
        Map<String, Object> blankUriConfig = requiredConfig();
        blankUriConfig.put(ActivemqSinkOptions.URI.key(), " ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(blankUriConfig));

        Map<String, Object> blankQueueNameConfig = requiredConfig();
        blankQueueNameConfig.put(ActivemqSinkOptions.QUEUE_NAME.key(), "\t");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(blankQueueNameConfig));
    }

    @Test
    void testBlankClientIdRejected() {
        Map<String, Object> config = requiredConfig();
        config.put(ActivemqSinkOptions.CLIENT_ID.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testCredentialsMustBeBundled() {
        Map<String, Object> usernameOnlyConfig = requiredConfig();
        usernameOnlyConfig.put(ActivemqSinkOptions.USERNAME.key(), "user");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(usernameOnlyConfig));

        Map<String, Object> passwordOnlyConfig = requiredConfig();
        passwordOnlyConfig.put(ActivemqSinkOptions.PASSWORD.key(), "password");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(passwordOnlyConfig));
    }

    @Test
    void testSupportedOptionalOptions() {
        Map<String, Object> config = requiredConfig();
        config.put(ActivemqSinkOptions.CLIENT_ID.key(), "client-id");
        config.put(ActivemqSinkOptions.CLOSE_TIMEOUT.key(), 1000);
        config.put(ActivemqSinkOptions.CONSUMER_EXPIRY_CHECK_ENABLED.key(), true);
        config.put(ActivemqSinkOptions.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT.key(), -1);
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "ActiveMQSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    @Test
    void testSourceOptionsAndFactory() {
        ActivemqSourceFactory factory = new ActivemqSourceFactory();
        Map<String, Object> config = sourceConfig();
        ReadonlyConfig options = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(options, factory.optionRule(), "ActiveMQSource");
        SeaTunnelSource<?, ?, ?> source = createSource(config);
        Assertions.assertEquals("ActiveMQ", source.getPluginName());
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
        Assertions.assertEquals(1, source.getProducedCatalogTables().size());
    }

    @Test
    void testSourceRejectsInvalidOptions() {
        for (String key : new String[] {"uri", "queue_name", "field_delimiter"}) {
            Map<String, Object> config = sourceConfig();
            config.put(key, " ");
            Assertions.assertThrows(OptionValidationException.class, () -> createSource(config));
        }
        for (int value : new int[] {0, -1}) {
            Map<String, Object> config = sourceConfig();
            config.put("max_in_flight_messages", value);
            Assertions.assertThrows(OptionValidationException.class, () -> createSource(config));
        }
        for (String key : new String[] {"username", "password"}) {
            Map<String, Object> config = sourceConfig();
            config.put(key, "test");
            Assertions.assertThrows(OptionValidationException.class, () -> createSource(config));
        }
    }

    @Test
    void testSourceRejectsCompositeDestinationsAndTransports() {
        for (String uri :
                new String[] {
                    "failover:(tcp://localhost:61616)",
                    "vm://broker",
                    "http://localhost:8161",
                    "tcp://user:secret@localhost:61616",
                    "tcp://localhost:61616?jms.prefetchPolicy.all=0",
                    "tcp://localhost",
                    "tcp://localhost:65536",
                    "tcp://localhost:61616/path"
                }) {
            Map<String, Object> config = sourceConfig();
            config.put("uri", uri);
            IllegalArgumentException error =
                    Assertions.assertThrows(
                            IllegalArgumentException.class, () -> createSource(config));
            Assertions.assertFalse(error.getMessage().contains("secret"));
        }
        for (String queue :
                new String[] {"a,b", "a?consumer.prefetchSize=0", "a.>", "a.*", "topic://a"}) {
            Map<String, Object> config = sourceConfig();
            config.put("queue_name", queue);
            Assertions.assertThrows(IllegalArgumentException.class, () -> createSource(config));
        }
    }

    @Test
    void testSourceRequiresStreamingCheckpoints() {
        SeaTunnelSource<?, ?, ?> source = createSource(sourceConfig());
        JobContext context = new JobContext();
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(true);
        source.setJobContext(context);
        Assertions.assertThrows(IllegalArgumentException.class, source::getBoundedness);
        context.setJobMode(JobMode.STREAMING);
        context.setEnableCheckpoint(false);
        Assertions.assertThrows(IllegalArgumentException.class, source::getBoundedness);
        context.setEnableCheckpoint(true);
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    private SeaTunnelSource<?, ?, ?> createSource(Map<String, Object> config) {
        return new ActivemqSourceFactory()
                .createSource(
                        new TableSourceFactoryContext(
                                ReadonlyConfig.fromMap(config), getClass().getClassLoader()))
                .createSource();
    }

    private Map<String, Object> sourceConfig() {
        Map<String, Object> config = requiredConfig();
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        return config;
    }

    private Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ActivemqSinkOptions.URI.key(), "tcp://localhost:61616");
        config.put(ActivemqSinkOptions.QUEUE_NAME.key(), "test-queue");
        return config;
    }
}
