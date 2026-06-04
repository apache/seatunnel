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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
