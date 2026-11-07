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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JmsSourceFactoryTest {

    @Test
    void factoryIdentifierIsJms() {
        assertEquals("Jms", new JmsSourceFactory().factoryIdentifier());
    }

    @Test
    void classAndJndiConfigurationsAreValid() {
        assertDoesNotThrow(() -> validate(classConfig()));
        assertDoesNotThrow(() -> validate(jndiConfig()));
    }

    @Test
    void exactlyOneConnectionFactoryModeIsRequired() {
        Map<String, Object> neither = classConfig();
        neither.remove("connection_factory_class");
        assertThrows(OptionValidationException.class, () -> validate(neither));

        Map<String, Object> both = classConfig();
        both.put("jndi_connection_factory_name", "ConnectionFactory");
        assertThrows(OptionValidationException.class, () -> validate(both));
    }

    @Test
    void optionsOfTheUnusedModeAreRejected() {
        Map<String, Object> jndiWithClassProperties = jndiConfig();
        jndiWithClassProperties.put(
                "connection_factory_properties", Collections.singletonMap("brokerURL", "x"));
        assertThrows(IllegalArgumentException.class, () -> validate(jndiWithClassProperties));

        Map<String, Object> classWithJndiUrl = classConfig();
        classWithJndiUrl.put("jndi_provider_url", "tcp://127.0.0.1:61616");
        assertThrows(IllegalArgumentException.class, () -> validate(classWithJndiUrl));
    }

    @Test
    void jndiNameRequiresAnInitialContextFactory() {
        Map<String, Object> config = jndiConfig();
        config.remove("jndi_initial_context_factory");
        assertThrows(IllegalArgumentException.class, () -> validate(config));
    }

    @Test
    void requiredAndBoundedOptionsAreChecked() {
        Map<String, Object> noDestination = classConfig();
        noDestination.remove("destination");
        assertThrows(OptionValidationException.class, () -> validate(noDestination));

        Map<String, Object> blankDestination = classConfig();
        blankDestination.put("destination", " ");
        assertThrows(OptionValidationException.class, () -> validate(blankDestination));

        Map<String, Object> noSchema = classConfig();
        noSchema.remove("schema");
        assertThrows(OptionValidationException.class, () -> validate(noSchema));

        Map<String, Object> zeroInFlight = classConfig();
        zeroInFlight.put("max_in_flight_messages", 0);
        assertThrows(OptionValidationException.class, () -> validate(zeroInFlight));
    }

    @Test
    void usernameAndPasswordGoTogether() {
        Map<String, Object> usernameOnly = classConfig();
        usernameOnly.put("username", "reader");
        assertThrows(OptionValidationException.class, () -> validate(usernameOnly));

        Map<String, Object> both = classConfig();
        both.put("username", "reader");
        both.put("password", "secret");
        assertDoesNotThrow(() -> validate(both));
    }

    @Test
    void everyOptionIsDeclaredInTheRule() {
        Map<String, Object> config = classConfig();
        config.put("username", "reader");
        config.put("password", "secret");
        config.put("message_selector", "kind = 'keep'");
        config.put("format", "text");
        config.put("field_delimiter", "|");
        config.put("max_in_flight_messages", 10);
        assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config),
                                new JmsSourceFactory().optionRule(),
                                "Jms"));
    }

    /** The map options keep dotted keys as they are written in a job config. */
    @Test
    void dottedPropertyKeysSurviveTheJobConfigFormat() {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "connection_factory_properties {\n"
                                        + "  brokerURL = \"tcp://127.0.0.1:61616\"\n"
                                        + "  prefetchPolicy.queuePrefetch = 1\n"
                                        + "}"));

        Map<String, String> expected = new HashMap<>();
        expected.put("brokerURL", "tcp://127.0.0.1:61616");
        expected.put("prefetchPolicy.queuePrefetch", "1");
        assertEquals(expected, config.get(JmsSourceOptions.CONNECTION_FACTORY_PROPERTIES));
    }

    private static void validate(Map<String, Object> config) {
        JmsSourceFactory.validate(ReadonlyConfig.fromMap(config));
    }

    private static Map<String, Object> classConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("connection_factory_class", "org.apache.activemq.ActiveMQConnectionFactory");
        config.put(
                "connection_factory_properties",
                Collections.singletonMap("brokerURL", "tcp://127.0.0.1:61616"));
        config.put("destination", "orders");
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        return config;
    }

    private static Map<String, Object> jndiConfig() {
        Map<String, Object> config = classConfig();
        config.remove("connection_factory_class");
        config.remove("connection_factory_properties");
        config.put(
                "jndi_initial_context_factory",
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        config.put("jndi_provider_url", "tcp://127.0.0.1:61616");
        config.put("jndi_connection_factory_name", "ConnectionFactory");
        return config;
    }
}
