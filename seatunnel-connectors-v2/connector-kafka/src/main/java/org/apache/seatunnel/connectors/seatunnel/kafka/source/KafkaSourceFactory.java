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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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
                        KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        KafkaSourceOptions.READER_CACHE_QUEUE_SIZE,
                        KafkaSourceOptions.IGNORE_NO_LEADER_PARTITION)
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.TIMESTAMP,
                        KafkaSourceOptions.START_MODE_TIMESTAMP)
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.TIMESTAMP,
                        Conditions.greaterOrEqual(KafkaSourceOptions.START_MODE_TIMESTAMP, 0L))
                .optional(
                        KafkaSourceOptions.START_MODE_END_TIMESTAMP,
                        Conditions.greaterOrEqual(KafkaSourceOptions.START_MODE_END_TIMESTAMP, 0L))
                .conditional(
                        KafkaSourceOptions.IGNORE_NO_LEADER_PARTITION,
                        Boolean.TRUE,
                        Conditions.greaterThan(
                                KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, 0L))
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.SPECIFIC_OFFSETS,
                        KafkaSourceOptions.START_MODE_OFFSETS)
                .conditional(
                        KafkaSourceOptions.START_MODE,
                        StartMode.SPECIFIC_OFFSETS,
                        Conditions.mapNotEmpty(KafkaSourceOptions.START_MODE_OFFSETS))
                .optional(
                        KafkaSourceOptions.TABLE_CONFIGS,
                        Conditions.extension(
                                KafkaSourceOptions.TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(
                        KafkaSourceOptions.TABLE_LIST,
                        Conditions.extension(
                                KafkaSourceOptions.TABLE_LIST, new TableConfigsValidator()))
                .conditional(
                        KafkaSourceOptions.FORMAT,
                        MessageFormat.PROTOBUF,
                        KafkaSourceOptions.STRIP_SCHEMA_REGISTRY_HEADER)
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

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each tables_configs entry: start_mode.timestamp >= 0 when TIMESTAMP, "
                    + "non-empty start_mode.offsets when SPECIFIC_OFFSETS";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
                throws OptionValidationException {
            if (entries == null || entries.isEmpty()) {
                return true;
            }
            for (int i = 0; i < entries.size(); i++) {
                ReadonlyConfig entryConfig = ReadonlyConfig.fromMap(entries.get(i));
                StartMode startMode =
                        entryConfig.getOptional(KafkaSourceOptions.START_MODE).orElse(null);
                if (startMode == StartMode.TIMESTAMP) {
                    Long ts =
                            entryConfig
                                    .getOptional(KafkaSourceOptions.START_MODE_TIMESTAMP)
                                    .orElse(null);
                    if (ts != null && ts < 0) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start_mode.timestamp' must be >= 0, got: %d",
                                i, ts);
                    }
                    Long endTs =
                            entryConfig
                                    .getOptional(KafkaSourceOptions.START_MODE_END_TIMESTAMP)
                                    .orElse(null);
                    if (endTs != null && endTs < 0) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start_mode.end_timestamp' must be >= 0, got: %d",
                                i, endTs);
                    }
                } else if (startMode == StartMode.SPECIFIC_OFFSETS) {
                    Map<String, Long> offsets =
                            entryConfig
                                    .getOptional(KafkaSourceOptions.START_MODE_OFFSETS)
                                    .orElse(null);
                    if (offsets != null && offsets.isEmpty()) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start_mode.offsets' must not be empty "
                                        + "when start_mode=SPECIFIC_OFFSETS",
                                i);
                    }
                }
            }
            return true;
        }
    }
}
