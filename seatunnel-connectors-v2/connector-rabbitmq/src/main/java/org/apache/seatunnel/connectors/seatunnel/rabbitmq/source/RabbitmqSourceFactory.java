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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.AUTOMATIC_RECOVERY_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.DELIVERY_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.EXCHANGE;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.NETWORK_RECOVERY_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.PORT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.PREFETCH_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.REQUESTED_CHANNEL_MAX;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.REQUESTED_FRAME_MAX;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.REQUESTED_HEARTBEAT;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.ROUTING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.TOPOLOGY_RECOVERY_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqOptions.VIRTUAL_HOST;

@AutoService(Factory.class)
public class RabbitmqSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "RabbitMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, PORT, VIRTUAL_HOST, QUEUE_NAME, TableSchemaOptions.SCHEMA)
                .bundled(USERNAME, PASSWORD)
                .optional(
                        URL,
                        ROUTING_KEY,
                        EXCHANGE,
                        NETWORK_RECOVERY_INTERVAL,
                        TOPOLOGY_RECOVERY_ENABLED,
                        AUTOMATIC_RECOVERY_ENABLED,
                        CONNECTION_TIMEOUT,
                        REQUESTED_CHANNEL_MAX,
                        REQUESTED_FRAME_MAX,
                        REQUESTED_HEARTBEAT,
                        PREFETCH_COUNT,
                        DELIVERY_TIMEOUT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return RabbitmqSource.class;
    }
}
