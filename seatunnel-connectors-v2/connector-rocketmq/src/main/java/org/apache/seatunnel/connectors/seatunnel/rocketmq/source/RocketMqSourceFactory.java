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
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AutoService(Factory.class)
public class RocketMqSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return RocketMqSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RocketMqSourceOptions.NAME_SRV_ADDR)
                .exclusive(
                        RocketMqSourceOptions.TOPICS,
                        RocketMqSourceOptions.TABLE_CONFIGS,
                        RocketMqSourceOptions.TABLE_LIST)
                .optional(
                        RocketMqSourceOptions.FORMAT,
                        RocketMqBaseOptions.FIELD_DELIMITER,
                        RocketMqSourceOptions.TAGS,
                        RocketMqSourceOptions.START_MODE,
                        RocketMqSourceOptions.CONSUMER_GROUP,
                        RocketMqSourceOptions.COMMIT_ON_CHECKPOINT,
                        RocketMqSourceOptions.SCHEMA,
                        RocketMqSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        RocketMqSourceOptions.POLL_TIMEOUT_MILLIS,
                        RocketMqSourceOptions.BATCH_SIZE,
                        RocketMqSourceOptions.IGNORE_PARSE_ERRORS,
                        RocketMqBaseOptions.ACL_ENABLED)
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_TIMESTAMP,
                        RocketMqSourceOptions.START_MODE_TIMESTAMP)
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_TIMESTAMP,
                        Conditions.greaterOrEqual(RocketMqSourceOptions.START_MODE_TIMESTAMP, 0L))
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_SPECIFIC_OFFSETS,
                        RocketMqSourceOptions.START_MODE_OFFSETS)
                .conditional(
                        RocketMqSourceOptions.START_MODE,
                        StartMode.CONSUME_FROM_SPECIFIC_OFFSETS,
                        Conditions.mapNotEmpty(RocketMqSourceOptions.START_MODE_OFFSETS))
                .optional(
                        RocketMqSourceOptions.TABLE_CONFIGS,
                        Conditions.extension(
                                RocketMqSourceOptions.TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(
                        RocketMqSourceOptions.TABLE_LIST,
                        Conditions.extension(
                                RocketMqSourceOptions.TABLE_LIST, new TableConfigsValidator()))
                .conditional(
                        RocketMqBaseOptions.ACL_ENABLED,
                        true,
                        RocketMqBaseOptions.ACCESS_KEY,
                        RocketMqBaseOptions.SECRET_KEY)
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

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each tables_configs entry must have valid topics, "
                    + "start.mode.timestamp (>= 0) when CONSUME_FROM_TIMESTAMP, "
                    + "and non-empty start.mode.offsets when CONSUME_FROM_SPECIFIC_OFFSETS";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
                throws OptionValidationException {
            if (entries == null || entries.isEmpty()) {
                return true;
            }
            for (int i = 0; i < entries.size(); i++) {
                ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(entries.get(i));
                String topics = tableConfig.get(RocketMqSourceOptions.TOPICS);
                if (topics == null || topics.trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'topics' must not be empty", i);
                }
                StartMode startMode =
                        tableConfig.getOptional(RocketMqSourceOptions.START_MODE).orElse(null);
                if (startMode == StartMode.CONSUME_FROM_TIMESTAMP) {
                    Long ts =
                            tableConfig
                                    .getOptional(RocketMqSourceOptions.START_MODE_TIMESTAMP)
                                    .orElse(null);
                    if (ts == null) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start.mode.timestamp' required "
                                        + "when start.mode=CONSUME_FROM_TIMESTAMP",
                                i);
                    }
                    if (ts < 0) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start.mode.timestamp' must be >= 0, got: %d",
                                i, ts);
                    }
                } else if (startMode == StartMode.CONSUME_FROM_SPECIFIC_OFFSETS) {
                    Map<String, Long> offsets =
                            tableConfig
                                    .getOptional(RocketMqSourceOptions.START_MODE_OFFSETS)
                                    .orElse(null);
                    if (offsets == null || offsets.isEmpty()) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: 'start.mode.offsets' must not be empty "
                                        + "when start.mode=CONSUME_FROM_SPECIFIC_OFFSETS",
                                i);
                    }
                }
            }
            return true;
        }
    }
}
