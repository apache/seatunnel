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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqSourceOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** RocketMq source */
public class RocketMqSource
        implements SeaTunnelSource<SeaTunnelRow, RocketMqSourceSplit, RocketMqSourceState>,
                SupportParallelism {

    private final ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;
    private final ConsumerMetadata metadata;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;

    public RocketMqSource(ReadonlyConfig pluginConfig) {
        this.pluginConfig = pluginConfig;
        // check config
        this.metadata = new ConsumerMetadata();
        this.metadata.setTopics(
                Arrays.asList(
                        pluginConfig
                                .get(RocketMqSourceOptions.TOPICS)
                                .split(RocketMqSourceOptions.DEFAULT_FIELD_DELIMITER)));

        String tags = pluginConfig.get(RocketMqSourceOptions.TAGS);
        if (tags != null && !tags.trim().isEmpty()) {
            this.metadata.setTags(
                    Arrays.stream(tags.split(RocketMqSourceOptions.DEFAULT_FIELD_DELIMITER))
                            .map(String::trim)
                            .filter(tag -> !tag.isEmpty())
                            .distinct()
                            .collect(Collectors.toList()));
        } else {
            this.metadata.setTags(Collections.emptyList());
        }

        RocketMqBaseConfiguration.Builder baseConfigBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .consumer()
                        .namesrvAddr(pluginConfig.get(RocketMqSourceOptions.NAME_SRV_ADDR));
        if (pluginConfig.getOptional(RocketMqSourceOptions.ACCESS_KEY).isPresent()) {
            baseConfigBuilder.accessKey(pluginConfig.get(RocketMqSourceOptions.ACCESS_KEY));
        }
        if (pluginConfig.getOptional(RocketMqSourceOptions.SECRET_KEY).isPresent()) {
            baseConfigBuilder.secretKey(pluginConfig.get(RocketMqSourceOptions.SECRET_KEY));
        }
        baseConfigBuilder.aclEnable(pluginConfig.get(RocketMqSourceOptions.ACL_ENABLED));
        baseConfigBuilder.groupId(pluginConfig.get(RocketMqSourceOptions.CONSUMER_GROUP));
        baseConfigBuilder.batchSize(pluginConfig.get(RocketMqSourceOptions.BATCH_SIZE));

        baseConfigBuilder.pollTimeoutMillis(
                pluginConfig.get(RocketMqSourceOptions.POLL_TIMEOUT_MILLIS));

        this.metadata.setBaseConfig(baseConfigBuilder.build());

        this.metadata.setEnabledCommitCheckpoint(
                pluginConfig.get(RocketMqSourceOptions.COMMIT_ON_CHECKPOINT));

        StartMode startMode = pluginConfig.get(RocketMqSourceOptions.START_MODE);
        switch (startMode) {
            case CONSUME_FROM_TIMESTAMP:
                long startOffsetsTimestamp =
                        pluginConfig.get(RocketMqSourceOptions.START_MODE_TIMESTAMP);
                long currentTimestamp = System.currentTimeMillis();
                if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                    throw new IllegalArgumentException(
                            "The offsets timestamp value is smaller than 0 or smaller"
                                    + " than the current time");
                }
                this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                break;
            case CONSUME_FROM_SPECIFIC_OFFSETS:
                Map<String, Long> offsetConfigMap =
                        pluginConfig.get(RocketMqSourceOptions.START_MODE_OFFSETS);
                Map<MessageQueue, Long> specificStartOffsets = new HashMap<>();
                offsetConfigMap.forEach(
                        (k, v) -> {
                            int splitIndex = k.lastIndexOf("-");
                            String topic = k.substring(0, splitIndex);
                            String partition = k.substring(splitIndex + 1);
                            MessageQueue messageQueue =
                                    new MessageQueue(topic, null, Integer.parseInt(partition));
                            specificStartOffsets.put(messageQueue, v);
                        });
                this.metadata.setSpecificStartOffsets(specificStartOffsets);
                break;
            default:
                break;
        }
        this.metadata.setStartMode(startMode);
        this.catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        // set deserialization
        setDeserialization(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Rocketmq";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SourceReader<SeaTunnelRow, RocketMqSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new RocketMqSourceReader(this.metadata, deserializationSchema, readerContext);
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> createEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context) throws Exception {
        return new RocketMqSourceSplitEnumerator(
                this.metadata,
                context,
                pluginConfig.get(RocketMqSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS));
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context,
            RocketMqSourceState sourceState)
            throws Exception {
        return new RocketMqSourceSplitEnumerator(
                this.metadata,
                context,
                pluginConfig.get(RocketMqSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS));
    }

    private void setDeserialization(ReadonlyConfig config) {
        if (config.getOptional(RocketMqSourceOptions.SCHEMA).isPresent()) {
            SchemaFormat format = config.get(RocketMqSourceOptions.FORMAT);
            boolean ignoreParseErrors = config.get(RocketMqSourceOptions.IGNORE_PARSE_ERRORS);
            switch (format) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
                    break;
                case TEXT:
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                                    .delimiter(config.get(RocketMqSourceOptions.FIELD_DELIMITER))
                                    .build();
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported format: " + format);
            }
        } else {
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                            .delimiter(String.valueOf('\002'))
                            .build();
        }
    }
}
