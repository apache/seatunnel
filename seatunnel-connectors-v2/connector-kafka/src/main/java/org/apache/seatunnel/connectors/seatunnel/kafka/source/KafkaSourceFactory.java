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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class KafkaSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Kafka";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KafkaSourceOptions.BOOTSTRAP_SERVERS)
                .exclusive(
                        KafkaSourceOptions.TOPIC,
                        KafkaSourceOptions.TABLE_CONFIGS,
                        KafkaSourceOptions.TABLE_LIST)
                .optional(
                        KafkaSourceOptions.START_MODE,
                        KafkaSourceOptions.PATTERN,
                        KafkaSourceOptions.CONSUMER_GROUP,
                        KafkaSourceOptions.COMMIT_ON_CHECKPOINT,
                        KafkaSourceOptions.KAFKA_CONFIG,
                        KafkaSourceOptions.SCHEMA,
                        KafkaSourceOptions.FORMAT,
                        KafkaSourceOptions.DEBEZIUM_RECORD_INCLUDE_SCHEMA,
                        KafkaSourceOptions.DEBEZIUM_RECORD_TABLE_FILTER,
                        KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS)
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.TIMESTAMP,
                        KafkaSourceOptions.START_MODE_TIMESTAMP)
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.SPECIFIC_OFFSETS,
                        KafkaSourceOptions.START_MODE_OFFSETS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new KafkaSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return KafkaSource.class;
    }
}
