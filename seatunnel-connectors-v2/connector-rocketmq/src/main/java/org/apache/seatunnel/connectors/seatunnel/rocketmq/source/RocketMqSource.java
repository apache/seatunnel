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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.ConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.rocketmq.common.message.MessageQueue;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACL_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.NAME_SRV_ADDR;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.SECRET_KEY;
import static org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions.concise;

/** RocketMq source */
@AutoService(SeaTunnelSource.class)
public class RocketMqSource
        implements SeaTunnelSource<SeaTunnelRow, RocketMqSourceSplit, RocketMqSourceState>,
                SupportParallelism {

    private static final String DEFAULT_CONSUMER_GROUP = "SeaTunnel-Consumer-Group";
    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private JobContext jobContext;
    private SeaTunnelRowType typeInfo;
    private CatalogTable catalogTable;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private long discoveryIntervalMillis =
            ConsumerConfig.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.defaultValue();

    @Override
    public String getPluginName() {
        return "Rocketmq";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        // check config
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        config, ConsumerConfig.TOPICS.key(), NAME_SRV_ADDR.key());
        if (!result.isSuccess()) {
            throw new RocketMqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.metadata.setTopics(
                Arrays.asList(
                        config.getString(ConsumerConfig.TOPICS.key())
                                .split(DEFAULT_FIELD_DELIMITER)));

        RocketMqBaseConfiguration.Builder baseConfigBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .consumer()
                        .namesrvAddr(config.getString(NAME_SRV_ADDR.key()));
        boolean aclEnabled = ACL_ENABLED.defaultValue();
        if (config.hasPath(ACL_ENABLED.key())) {
            aclEnabled = config.getBoolean(ACL_ENABLED.key());
            if (aclEnabled
                    && (!config.hasPath(ACCESS_KEY.key()) || !config.hasPath(SECRET_KEY.key()))) {
                throw new RocketMqConnectorException(
                        SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                        "When ACL_ENABLED "
                                + "true , ACCESS_KEY and SECRET_KEY must be configured");
            }
            if (config.hasPath(ACCESS_KEY.key())) {
                baseConfigBuilder.accessKey(config.getString(ACCESS_KEY.key()));
            }
            if (config.hasPath(SECRET_KEY.key())) {
                baseConfigBuilder.secretKey(config.getString(SECRET_KEY.key()));
            }
        }
        baseConfigBuilder.aclEnable(aclEnabled);
        // config consumer group
        if (config.hasPath(ConsumerConfig.CONSUMER_GROUP.key())) {
            baseConfigBuilder.groupId(config.getString(ConsumerConfig.CONSUMER_GROUP.key()));
        } else {
            baseConfigBuilder.groupId(DEFAULT_CONSUMER_GROUP);
        }
        if (config.hasPath(ConsumerConfig.BATCH_SIZE.key())) {
            baseConfigBuilder.batchSize(config.getInt(ConsumerConfig.BATCH_SIZE.key()));
        } else {
            baseConfigBuilder.batchSize(ConsumerConfig.BATCH_SIZE.defaultValue());
        }
        if (config.hasPath(ConsumerConfig.POLL_TIMEOUT_MILLIS.key())) {
            baseConfigBuilder.pollTimeoutMillis(
                    config.getInt(ConsumerConfig.POLL_TIMEOUT_MILLIS.key()));
        } else {
            baseConfigBuilder.pollTimeoutMillis(ConsumerConfig.POLL_TIMEOUT_MILLIS.defaultValue());
        }
        this.metadata.setBaseConfig(baseConfigBuilder.build());

        // auto commit
        if (config.hasPath(ConsumerConfig.COMMIT_ON_CHECKPOINT.key())) {
            this.metadata.setEnabledCommitCheckpoint(
                    config.getBoolean(ConsumerConfig.COMMIT_ON_CHECKPOINT.key()));
        } else {
            this.metadata.setEnabledCommitCheckpoint(
                    ConsumerConfig.COMMIT_ON_CHECKPOINT.defaultValue());
        }

        StartMode startMode = ConsumerConfig.START_MODE.defaultValue();
        if (config.hasPath(ConsumerConfig.START_MODE.key())) {
            startMode =
                    StartMode.valueOf(
                            config.getString(ConsumerConfig.START_MODE.key()).toUpperCase());
            switch (startMode) {
                case CONSUME_FROM_TIMESTAMP:
                    long startOffsetsTimestamp =
                            config.getLong(ConsumerConfig.START_MODE_TIMESTAMP.key());
                    long currentTimestamp = System.currentTimeMillis();
                    if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                        throw new IllegalArgumentException(
                                "The offsets timestamp value is smaller than 0 or smaller"
                                        + " than the current time");
                    }
                    this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                    break;
                case CONSUME_FROM_SPECIFIC_OFFSETS:
                    Config offsets = config.getConfig(ConsumerConfig.START_MODE_OFFSETS.key());
                    ConfigRenderOptions options = concise();
                    String offsetsJson = offsets.root().render(options);
                    if (offsetsJson == null) {
                        throw new IllegalArgumentException(
                                "start mode is "
                                        + StartMode.CONSUME_FROM_SPECIFIC_OFFSETS
                                        + "but no specific"
                                        + " offsets were specified.");
                    }
                    Map<MessageQueue, Long> specificStartOffsets = new HashMap<>();
                    ObjectNode jsonNodes = JsonUtils.parseObject(offsetsJson);
                    jsonNodes
                            .fieldNames()
                            .forEachRemaining(
                                    key -> {
                                        int splitIndex = key.lastIndexOf("-");
                                        String topic = key.substring(0, splitIndex);
                                        String partition = key.substring(splitIndex + 1);
                                        long offset = jsonNodes.get(key).asLong();
                                        MessageQueue messageQueue =
                                                new MessageQueue(
                                                        topic, null, Integer.valueOf(partition));
                                        specificStartOffsets.put(messageQueue, offset);
                                    });
                    this.metadata.setSpecificStartOffsets(specificStartOffsets);
                    break;
                default:
                    break;
            }
        }
        this.metadata.setStartMode(startMode);

        if (config.hasPath(ConsumerConfig.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key())) {
            this.discoveryIntervalMillis =
                    config.getLong(ConsumerConfig.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key());
        }
        this.catalogTable = CatalogTableUtil.buildWithConfig(config);
        this.typeInfo = catalogTable.getSeaTunnelRowType();

        // set deserialization
        setDeserialization(config);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, RocketMqSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new RocketMqSourceReader(this.metadata, deserializationSchema, readerContext);
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> createEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context) throws Exception {
        return new RocketMqSourceSplitEnumerator(this.metadata, context, discoveryIntervalMillis);
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context,
            RocketMqSourceState sourceState)
            throws Exception {
        return new RocketMqSourceSplitEnumerator(this.metadata, context, discoveryIntervalMillis);
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(ConsumerConfig.SCHEMA.key())) {
            SchemaFormat format = SchemaFormat.JSON;
            if (config.hasPath(FORMAT.key())) {
                format = SchemaFormat.find(config.getString(FORMAT.key()));
            }
            boolean ignoreParseErrors = false;
            if (config.hasPath(ConsumerConfig.IGNORE_PARSE_ERRORS.key())) {
                ignoreParseErrors = config.getBoolean(ConsumerConfig.IGNORE_PARSE_ERRORS.key());
            }
            switch (format) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
                    break;
                case TEXT:
                    String delimiter = DEFAULT_FIELD_DELIMITER;
                    if (config.hasPath(FIELD_DELIMITER.key())) {
                        delimiter = config.getString(FIELD_DELIMITER.key());
                    }
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(typeInfo)
                                    .delimiter(delimiter)
                                    .build();
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported format: " + format);
            }
        } else {
            typeInfo = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(typeInfo)
                            .delimiter(String.valueOf('\002'))
                            .build();
        }
    }
}
