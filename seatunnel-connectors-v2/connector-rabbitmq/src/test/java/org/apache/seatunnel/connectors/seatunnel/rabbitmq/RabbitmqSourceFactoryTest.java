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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitmqSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        RabbitmqSourceFactory factory = new RabbitmqSourceFactory();
        Assertions.assertEquals("RabbitMQ", factory.factoryIdentifier());
    }

    /** Test Basic Required Options. Checks if HOST and PORT are mandatory. */
    @Test
    public void testRequiredOptions() {
        RabbitmqSourceFactory factory = new RabbitmqSourceFactory();
        OptionRule rule = factory.optionRule();

        List<RequiredOption> requiredOptions = rule.getRequiredOptions();

        boolean hasHost =
                requiredOptions.stream()
                        .anyMatch(req -> req.toString().contains(RabbitmqSourceOptions.HOST.key()));
        Assertions.assertTrue(hasHost, "HOST should be required");

        boolean hasPort =
                requiredOptions.stream()
                        .anyMatch(req -> req.toString().contains(RabbitmqSourceOptions.PORT.key()));
        Assertions.assertTrue(hasPort, "PORT should be required");
    }

    /**
     * Test Exclusive Options (Legacy vs Multi-table). Since we cannot access 'getExclusiveOptions'
     * directly in this API version, we check 'getRequiredOptions' because exclusive rules are
     * stored there as complex required rules (Condition: A OR B is required).
     */
    @Test
    public void testExclusiveOptionsLogic() {
        RabbitmqSourceFactory factory = new RabbitmqSourceFactory();
        OptionRule rule = factory.optionRule();

        List<RequiredOption> requiredOptions = rule.getRequiredOptions();

        boolean hasExclusiveRule =
                requiredOptions.stream()
                        .anyMatch(
                                req -> {
                                    String ruleString = req.toString();
                                    return ruleString.contains(
                                                    TableSchemaOptions.TABLE_CONFIGS.key())
                                            && ruleString.contains(
                                                    RabbitmqSourceOptions.QUEUE_NAME.key());
                                });

        Assertions.assertTrue(
                hasExclusiveRule,
                "Factory must have a rule linking 'table_configs' and 'queue_name' (Exclusive Logic)");
    }

    /**
     * Explicitly verifies that the 'table_configs' option key is present in the factory rules. This
     * confirms that the Multi-table feature is discoverable by the SeaTunnel engine.
     */
    @Test
    public void testMultiTableKeyPresence() {
        RabbitmqSourceFactory factory = new RabbitmqSourceFactory();
        OptionRule rule = factory.optionRule();

        boolean keyExists =
                rule.getRequiredOptions().stream()
                        .anyMatch(
                                opt ->
                                        opt.toString()
                                                .contains(TableSchemaOptions.TABLE_CONFIGS.key()));

        Assertions.assertTrue(
                keyExists,
                "The Factory must explicitly register the '"
                        + TableSchemaOptions.TABLE_CONFIGS.key()
                        + "' option.");
    }

    private void validate(Map<String, Object> configMap) {
        RabbitmqSourceFactory rabbitmqSourceFactory = new RabbitmqSourceFactory();
        ConfigValidator.of(ReadonlyConfig.fromMap(configMap))
                .validate(rabbitmqSourceFactory.optionRule());
    }

    @Test
    public void testValidSingleTableConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);
        config.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "test_queue");

        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        config.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    public void testSingleTableMissingSchema() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);
        config.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "queue_1");

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.SCHEMA.key()));
    }

    @Test
    public void testSingleTableBlankQueueName() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);
        config.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "   ");

        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        config.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private Map<String, Object> createValidMultiTableConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        Map<String, Object> table = new HashMap<>();
        table.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "queue_1");
        table.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        config.put(RabbitmqSourceOptions.TABLE_CONFIGS.key(), Collections.singletonList(table));
        return config;
    }

    @Test
    public void testValidMultiTableConfig() {
        Map<String, Object> config = createValidMultiTableConfig();
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    public void testSchemaAndTableConfigsAreExclusive() {
        Map<String, Object> config = createValidMultiTableConfig();
        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        config.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.SCHEMA.key()));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.TABLE_CONFIGS.key()));
    }

    @Test
    public void testQueueNameAndTableConfigsAreExclusive() {
        Map<String, Object> config = createValidMultiTableConfig();
        config.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "root_queue");

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    public void testMissingSingleAndMultiTableConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    public void testTableConfigsMissingQueueName() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        Map<String, Object> table = new HashMap<>();
        table.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        config.put(RabbitmqSourceOptions.TABLE_CONFIGS.key(), Collections.singletonList(table));

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.QUEUE_NAME.key()));
    }

    @Test
    public void testTableConfigsBlankQueueName() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);

        Map<String, Object> table = new HashMap<>();
        table.put(RabbitmqSourceOptions.QUEUE_NAME.key(), " ");
        table.put(RabbitmqSourceOptions.SCHEMA.key(), schema);

        config.put(RabbitmqSourceOptions.TABLE_CONFIGS.key(), Collections.singletonList(table));

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.QUEUE_NAME.key()));
    }

    @Test
    public void testTableConfigsMissingSchema() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        Map<String, Object> table = new HashMap<>();
        table.put(RabbitmqSourceOptions.QUEUE_NAME.key(), "queue_1");

        config.put(RabbitmqSourceOptions.TABLE_CONFIGS.key(), Collections.singletonList(table));

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.SCHEMA.key()));
    }

    @Test
    public void testEmptyTableConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(RabbitmqSourceOptions.HOST.key(), "localhost");
        config.put(RabbitmqSourceOptions.PORT.key(), 5672);

        config.put(RabbitmqSourceOptions.TABLE_CONFIGS.key(), Collections.emptyList());

        OptionValidationException optionValidationException =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(
                optionValidationException
                        .getMessage()
                        .contains(RabbitmqSourceOptions.TABLE_CONFIGS.key()));
    }

    @Test
    public void testSchemaIsRegisteredAsOptionalOption() {
        RabbitmqSourceFactory factory = new RabbitmqSourceFactory();

        boolean hasSchema =
                factory.optionRule().getOptionalOptions().stream()
                        .anyMatch(
                                option -> option.key().equals(RabbitmqSourceOptions.SCHEMA.key()));

        Assertions.assertTrue(hasSchema, "SCHEMA should be registered as an optional option");
    }
}
