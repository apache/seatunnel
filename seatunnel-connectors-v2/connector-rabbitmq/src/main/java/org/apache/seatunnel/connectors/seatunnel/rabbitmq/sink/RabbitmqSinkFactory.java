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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class RabbitmqSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "RabbitMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RabbitmqSinkOptions.HOST,
                        RabbitmqSinkOptions.PORT,
                        RabbitmqSinkOptions.VIRTUAL_HOST,
                        RabbitmqSinkOptions.QUEUE_NAME)
                .bundled(RabbitmqSinkOptions.USERNAME, RabbitmqSinkOptions.PASSWORD)
                .optional(
                        RabbitmqSinkOptions.URL,
                        RabbitmqSinkOptions.ROUTING_KEY,
                        RabbitmqSinkOptions.EXCHANGE,
                        RabbitmqSinkOptions.NETWORK_RECOVERY_INTERVAL,
                        RabbitmqSinkOptions.TOPOLOGY_RECOVERY_ENABLED,
                        RabbitmqSinkOptions.AUTOMATIC_RECOVERY_ENABLED,
                        RabbitmqSinkOptions.CONNECTION_TIMEOUT,
                        RabbitmqSinkOptions.FOR_E2E_TESTING,
                        RabbitmqSinkOptions.DURABLE,
                        RabbitmqSinkOptions.EXCLUSIVE,
                        RabbitmqSinkOptions.AUTO_DELETE,
                        RabbitmqSinkOptions.RABBITMQ_CONFIG)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new RabbitmqSink(
                        new RabbitmqConfig(context.getOptions()), context.getCatalogTable());
    }
}
