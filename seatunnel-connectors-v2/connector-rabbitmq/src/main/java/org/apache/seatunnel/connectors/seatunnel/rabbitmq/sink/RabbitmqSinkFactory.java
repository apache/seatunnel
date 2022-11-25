/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.sink;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.AUTOMATIC_RECOVERY_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.EXCHANGE;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.NETWORK_RECOVERY_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.PORT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.ROUTING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.TOPOLOGY_RECOVERY_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig.VIRTUAL_HOST;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class RabbitmqSinkFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "RabbitMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(
                HOST,
                PORT,
                VIRTUAL_HOST,
                QUEUE_NAME
            )
            .bundled(USERNAME, PASSWORD)
            .optional(
                URL,
                ROUTING_KEY,
                EXCHANGE,
                NETWORK_RECOVERY_INTERVAL,
                TOPOLOGY_RECOVERY_ENABLED,
                AUTOMATIC_RECOVERY_ENABLED,
                CONNECTION_TIMEOUT
            )
            .build();
    }
}
