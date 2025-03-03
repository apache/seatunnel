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
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.ConsumerConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class RocketMqSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return Config.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ConsumerConfig.TOPICS, Config.NAME_SRV_ADDR)
                .optional(
                        Config.FORMAT,
                        ConsumerConfig.START_MODE,
                        ConsumerConfig.CONSUMER_GROUP,
                        ConsumerConfig.COMMIT_ON_CHECKPOINT,
                        ConsumerConfig.SCHEMA,
                        ConsumerConfig.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        ConsumerConfig.POLL_TIMEOUT_MILLIS,
                        ConsumerConfig.BATCH_SIZE)
                .conditional(
                        ConsumerConfig.START_MODE,
                        StartMode.CONSUME_FROM_TIMESTAMP,
                        ConsumerConfig.START_MODE_TIMESTAMP)
                .conditional(
                        ConsumerConfig.START_MODE,
                        StartMode.CONSUME_FROM_SPECIFIC_OFFSETS,
                        ConsumerConfig.START_MODE_OFFSETS,
                        ConsumerConfig.IGNORE_PARSE_ERRORS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return RocketMqSource.class;
    }
}
