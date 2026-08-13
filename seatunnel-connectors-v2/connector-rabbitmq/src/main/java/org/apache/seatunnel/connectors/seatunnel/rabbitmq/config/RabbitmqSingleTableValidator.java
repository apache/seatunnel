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

public class RabbitmqSingleTableValidator implements ConditionExtension<String> {
    @Override
    public String description() {
        return "'schema' must be configured when 'queue_name' is used.";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, String queueName)
            throws OptionValidationException {

        if (!config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            throw new OptionValidationException(
                    "'schema' must be configured when 'queue_name' is used");
        }

        return true;
    }
}
