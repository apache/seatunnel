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
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class RabbitmqSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "RabbitMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RabbitmqSourceOptions.HOST,
                        RabbitmqSourceOptions.PORT,
                        RabbitmqSourceOptions.VIRTUAL_HOST,
                        RabbitmqSourceOptions.QUEUE_NAME,
                        RabbitmqSourceOptions.SCHEMA)
                .bundled(RabbitmqSourceOptions.USERNAME, RabbitmqSourceOptions.PASSWORD)
                .optional(
                        RabbitmqSourceOptions.URL,
                        RabbitmqSourceOptions.ROUTING_KEY,
                        RabbitmqSourceOptions.EXCHANGE,
                        RabbitmqSourceOptions.NETWORK_RECOVERY_INTERVAL,
                        RabbitmqSourceOptions.TOPOLOGY_RECOVERY_ENABLED,
                        RabbitmqSourceOptions.AUTOMATIC_RECOVERY_ENABLED,
                        RabbitmqSourceOptions.CONNECTION_TIMEOUT,
                        RabbitmqSinkOptions.FOR_E2E_TESTING,
                        RabbitmqSinkOptions.DURABLE,
                        RabbitmqSinkOptions.EXCLUSIVE,
                        RabbitmqSinkOptions.AUTO_DELETE,
                        RabbitmqSourceOptions.REQUESTED_CHANNEL_MAX,
                        RabbitmqSourceOptions.REQUESTED_FRAME_MAX,
                        RabbitmqSourceOptions.REQUESTED_HEARTBEAT,
                        RabbitmqSourceOptions.PREFETCH_COUNT,
                        RabbitmqSourceOptions.DELIVERY_TIMEOUT)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new RabbitmqSource(
                                new RabbitmqConfig(context.getOptions()),
                                CatalogTableUtil.buildWithConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return RabbitmqSource.class;
    }
}
