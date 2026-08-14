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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.util.List;
import java.util.Map;

public class RabbitmqTableConfigsValidator
        implements ConditionExtension<List<Map<String, Object>>> {

    @Override
    public String description() {
        return "when 'table_configs' is configured, root-level 'schema' must not be configured "
                + "and each 'table_configs' entry must declare a non-blank 'queue_name' and a 'schema'";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
            throws OptionValidationException {
        if (config.getOptional(RabbitmqBaseOptions.SCHEMA).isPresent()) {
            throw new OptionValidationException(
                    "Cannot specify both 'table_configs' and root-level 'schema'.");
        }
        for (int i = 0; i < entries.size(); i++) {
            ReadonlyConfig entry = ReadonlyConfig.fromMap(entries.get(i));
            String queueName = entry.getOptional(RabbitmqBaseOptions.QUEUE_NAME).orElse(null);
            if (queueName == null || queueName.trim().isEmpty()) {
                throw new OptionValidationException(
                        "table_configs[%d]: 'queue_name' must be configured and non-blank", i);
            }

            if (!entry.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                throw new OptionValidationException(
                        "table_configs[%d]: 'schema' must be configured", i);
            }
        }

        return true;
    }
}
