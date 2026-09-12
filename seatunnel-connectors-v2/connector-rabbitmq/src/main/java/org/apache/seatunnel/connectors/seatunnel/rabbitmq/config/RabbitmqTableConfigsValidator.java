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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import java.util.List;
import java.util.Map;

/**
 * Validates the RabbitMQ source multi-table configuration.
 *
 * <p>This validator is invoked by {@code ConfigValidator} through {@code
 * Conditions.extension(TABLE_CONFIGS, ...)} before {@code RabbitmqSource} is constructed. It
 * validates each {@code tables_configs} entry and rejects a root-level {@code schema}.
 */
public class RabbitmqTableConfigsValidator
        implements ConditionExtension<List<Map<String, Object>>> {

    private static final String TABLE_CONFIGS_KEY = RabbitmqBaseOptions.TABLE_CONFIGS.key();
    private static final String QUEUE_NAME_KEY = RabbitmqBaseOptions.QUEUE_NAME.key();
    private static final String SCHEMA_KEY = RabbitmqBaseOptions.SCHEMA.key();

    @Override
    public String description() {
        return "requires each entry to declare a non-blank '"
                + QUEUE_NAME_KEY
                + "' and a '"
                + SCHEMA_KEY
                + "', and forbids a root-level '"
                + SCHEMA_KEY
                + "'";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
            throws OptionValidationException {
        if (config.getOptional(RabbitmqBaseOptions.SCHEMA).isPresent()) {
            throw new OptionValidationException(
                    "Cannot specify both '%s' and root-level '%s'.", TABLE_CONFIGS_KEY, SCHEMA_KEY);
        }
        for (int i = 0; i < entries.size(); i++) {
            ReadonlyConfig entry = ReadonlyConfig.fromMap(entries.get(i));
            String queueName = entry.getOptional(RabbitmqBaseOptions.QUEUE_NAME).orElse(null);
            if (queueName == null || queueName.trim().isEmpty()) {
                throw new OptionValidationException(
                        "%s[%d]: '%s' must be configured and non-blank",
                        TABLE_CONFIGS_KEY, i, QUEUE_NAME_KEY);
            }

            if (!entry.getOptional(RabbitmqBaseOptions.SCHEMA).isPresent()) {
                throw new OptionValidationException(
                        "%s[%d]: '%s' must be configured", TABLE_CONFIGS_KEY, i, SCHEMA_KEY);
            }
        }

        return true;
    }
}
