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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class RocketMqSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return RocketMqSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RocketMqSourceOptions.TOPICS, RocketMqSourceOptions.NAME_SRV_ADDR)
                .optional(
                        RocketMqSourceOptions.FORMAT,
                        RocketMqSourceOptions.TAGS,
                        RocketMqSourceOptions.START_MODE,
                        RocketMqSourceOptions.CONSUMER_GROUP,
                        RocketMqSourceOptions.COMMIT_ON_CHECKPOINT,
                        RocketMqSourceOptions.SCHEMA,
                        RocketMqSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        RocketMqSourceOptions.POLL_TIMEOUT_MILLIS,
                        RocketMqSourceOptions.BATCH_SIZE)
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_TIMESTAMP,
                        RocketMqSourceOptions.START_MODE_TIMESTAMP)
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_SPECIFIC_OFFSETS,
                        RocketMqSourceOptions.START_MODE_OFFSETS,
                        RocketMqSourceOptions.IGNORE_PARSE_ERRORS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return RocketMqSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new RocketMqSource(context.getOptions());
    }
}
