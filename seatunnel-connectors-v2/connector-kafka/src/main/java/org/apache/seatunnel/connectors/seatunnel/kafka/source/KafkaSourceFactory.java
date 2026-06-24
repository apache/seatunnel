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
                        KafkaSourceOptions.PATTERN,
                        KafkaSourceOptions.CONSUMER_GROUP,
                        KafkaSourceOptions.COMMIT_ON_CHECKPOINT,
                        KafkaSourceOptions.KAFKA_CONFIG,
                        KafkaSourceOptions.SCHEMA,
                        KafkaSourceOptions.FORMAT,
                        KafkaSourceOptions.DEBEZIUM_RECORD_INCLUDE_SCHEMA,
                        KafkaSourceOptions.DEBEZIUM_RECORD_TABLE_FILTER,
                        KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        KafkaSourceOptions.READER_CACHE_QUEUE_SIZE)
                .optional(
                        KafkaSourceOptions.START_MODE_END_TIMESTAMP,
                        Conditions.greaterOrEqual(KafkaSourceOptions.START_MODE_END_TIMESTAMP, 0L))
                .optional(
                        KafkaSourceOptions.TABLE_CONFIGS,
                        Conditions.extension(
                                KafkaSourceOptions.TABLE_CONFIGS, new KafkaTableConfigsValidator()))
                .optional(
                        KafkaSourceOptions.TABLE_LIST,
                        Conditions.extension(
                                KafkaSourceOptions.TABLE_LIST, new KafkaTableConfigsValidator()))
                .conditional(
                        KafkaSourceOptions.FORMAT,
                        MessageFormat.PROTOBUF,
                        KafkaSourceOptions.STRIP_SCHEMA_REGISTRY_HEADER)
                .optional(
                        KafkaSourceOptions.START_MODE,
                        Conditions.extension(
                                KafkaSourceOptions.START_MODE, new KafkaStartModeValidator()))
                .optional(
                        KafkaSourceOptions.IGNORE_NO_LEADER_PARTITION,
                        Conditions.extension(
                                KafkaSourceOptions.IGNORE_NO_LEADER_PARTITION,
                                new KafkaPartitionDiscoveryValidator()))
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

    private static class KafkaPartitionDiscoveryValidator implements ConditionExtension<Boolean> {
        @Override
        public String description() {
            return "If [ignore_no_leader_partition] is true, "
                    + "then [partition-discovery.interval-millis] must not be null and greater than 0";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Boolean value)
                throws OptionValidationException {
            if (!value) {
                return true;
            }
            Long partitionDiscoveryIntervalMillis =
                    config.get(KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);
            return partitionDiscoveryIntervalMillis != null && partitionDiscoveryIntervalMillis > 0;
        }
    }

    private static class KafkaStartModeValidator implements ConditionExtension<StartMode> {
        @Override
        public String description() {
            return "if [start_mode] is timestamp than [start_mode.timestamp] must not be null and greater than 0, "
                    + "if [start_mode] is specific_offsets than [start_mode.offsets] must not be null and map is not empty";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, StartMode value)
                throws OptionValidationException {
            if (StartMode.TIMESTAMP == value) {
                Long startModeTimestamp = config.get(KafkaSourceOptions.START_MODE_TIMESTAMP);
                return startModeTimestamp != null && startModeTimestamp >= 0L;

            } else if (StartMode.SPECIFIC_OFFSETS == value) {
                Map<String, Long> startModeOffsets =
                        config.get(KafkaSourceOptions.START_MODE_OFFSETS);
                return startModeOffsets != null && !startModeOffsets.isEmpty();
            }
            return true;
        }
    }

    private static class KafkaTableConfigsValidator
            implements ConditionExtension<List<Map<String, Object>>> {

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
                    Long ts = entryConfig.get(KafkaSourceOptions.START_MODE_TIMESTAMP);
                    if (ts == null || ts < 0) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start_mode.timestamp' must be >= 0, got: %d",
                                i, ts);
                    }
                    Long endTs = entryConfig.get(KafkaSourceOptions.START_MODE_END_TIMESTAMP);
                    if (endTs != null && endTs < 0) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start_mode.end_timestamp' must be >= 0, got: %d",
                                i, endTs);
                    }
                } else if (startMode == StartMode.SPECIFIC_OFFSETS) {
                    Map<String, Long> offsets =
                            entryConfig.get(KafkaSourceOptions.START_MODE_OFFSETS);
                    if (offsets == null || offsets.isEmpty()) {
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
