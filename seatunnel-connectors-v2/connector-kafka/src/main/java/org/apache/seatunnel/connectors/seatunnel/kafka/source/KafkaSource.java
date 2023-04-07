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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import org.apache.kafka.common.TopicPartition;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_OFFSETS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;

@AutoService(SeaTunnelSource.class)
public class KafkaSource
        implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
                SupportParallelism {

    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;
    private long discoveryIntervalMillis = KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.defaultValue();
    private MessageFormatErrorHandleWay messageFormatErrorHandleWay =
            MessageFormatErrorHandleWay.FAIL;

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(config, TOPIC.key(), BOOTSTRAP_SERVERS.key());
        if (!result.isSuccess()) {
            throw new KafkaConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.metadata.setTopic(config.getString(TOPIC.key()));
        if (config.hasPath(PATTERN.key())) {
            this.metadata.setPattern(config.getBoolean(PATTERN.key()));
        } else {
            this.metadata.setPattern(PATTERN.defaultValue());
        }
        this.metadata.setBootstrapServers(config.getString(BOOTSTRAP_SERVERS.key()));
        this.metadata.setProperties(new Properties());

        if (config.hasPath(CONSUMER_GROUP.key())) {
            this.metadata.setConsumerGroup(config.getString(CONSUMER_GROUP.key()));
        } else {
            this.metadata.setConsumerGroup(CONSUMER_GROUP.defaultValue());
        }

        if (config.hasPath(COMMIT_ON_CHECKPOINT.key())) {
            this.metadata.setCommitOnCheckpoint(config.getBoolean(COMMIT_ON_CHECKPOINT.key()));
        } else {
            this.metadata.setCommitOnCheckpoint(COMMIT_ON_CHECKPOINT.defaultValue());
        }

        if (config.hasPath(START_MODE.key())) {
            StartMode startMode =
                    StartMode.valueOf(config.getString(START_MODE.key()).toUpperCase());
            this.metadata.setStartMode(startMode);
            switch (startMode) {
                case TIMESTAMP:
                    long startOffsetsTimestamp = config.getLong(START_MODE_TIMESTAMP.key());
                    long currentTimestamp = System.currentTimeMillis();
                    if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                        throw new IllegalArgumentException(
                                "start_mode.timestamp The value is smaller than 0 or smaller than the current time");
                    }
                    this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                    break;
                case SPECIFIC_OFFSETS:
                    Config offsets = config.getConfig(START_MODE_OFFSETS.key());
                    ConfigRenderOptions options = ConfigRenderOptions.concise();
                    String offsetsJson = offsets.root().render(options);
                    if (offsetsJson == null) {
                        throw new IllegalArgumentException(
                                "start mode is "
                                        + StartMode.SPECIFIC_OFFSETS
                                        + "but no specific offsets were specified.");
                    }
                    Map<TopicPartition, Long> specificStartOffsets = new HashMap<>();
                    ObjectNode jsonNodes = JsonUtils.parseObject(offsetsJson);
                    jsonNodes
                            .fieldNames()
                            .forEachRemaining(
                                    key -> {
                                        int splitIndex = key.lastIndexOf("-");
                                        String topic = key.substring(0, splitIndex);
                                        String partition = key.substring(splitIndex + 1);
                                        long offset = jsonNodes.get(key).asLong();
                                        TopicPartition topicPartition =
                                                new TopicPartition(
                                                        topic, Integer.valueOf(partition));
                                        specificStartOffsets.put(topicPartition, offset);
                                    });
                    this.metadata.setSpecificStartOffsets(specificStartOffsets);
                    break;
                default:
                    break;
            }
        }

        if (config.hasPath(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key())) {
            this.discoveryIntervalMillis =
                    config.getLong(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key());
        }

        if (CheckConfigUtil.isValidParam(config, KAFKA_CONFIG.key())) {
            config.getObject(KAFKA_CONFIG.key())
                    .forEach(
                            (key, value) ->
                                    this.metadata.getProperties().put(key, value.unwrapped()));
        }

        if (config.hasPath(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION.key())) {
            MessageFormatErrorHandleWay formatErrorWayOption =
                    ReadonlyConfig.fromConfig(config).get(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
            switch (formatErrorWayOption) {
                case FAIL:
                case SKIP:
                    this.messageFormatErrorHandleWay = formatErrorWayOption;
                    break;
                default:
                    break;
            }
        }

        setDeserialization(config);
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new KafkaSourceReader(
                this.metadata, deserializationSchema, readerContext, messageFormatErrorHandleWay);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) throws Exception {
        return new KafkaSourceSplitEnumerator(
                this.metadata, enumeratorContext, discoveryIntervalMillis);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext,
            KafkaSourceState checkpointState)
            throws Exception {
        return new KafkaSourceSplitEnumerator(
                this.metadata, enumeratorContext, checkpointState, discoveryIntervalMillis);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(SCHEMA.key())) {
            Config schema = config.getConfig(SCHEMA.key());
            // todo: use KafkaDataTypeConvertor here?
            typeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
            MessageFormat format = ReadonlyConfig.fromConfig(config).get(FORMAT);
            switch (format) {
                case JSON:
                    deserializationSchema = new JsonDeserializationSchema(false, false, typeInfo);
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
                case CANAL_JSON:
                    deserializationSchema =
                            CanalJsonDeserializationSchema.builder(typeInfo)
                                    .setIgnoreParseErrors(true)
                                    .build();
                    break;
                case COMPATIBLE_KAFKA_CONNECT_JSON:
                    deserializationSchema =
                            new CompatibleKafkaConnectDeserializationSchema(
                                    typeInfo, config, false, false);
                    break;
                case DEBEZIUM_JSON:
                    boolean includeSchema = DEBEZIUM_RECORD_INCLUDE_SCHEMA.defaultValue();
                    if (config.hasPath(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key())) {
                        includeSchema = config.getBoolean(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key());
                    }
                    deserializationSchema =
                            new DebeziumJsonDeserializationSchema(typeInfo, true, includeSchema);
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
            }
        } else {
            typeInfo = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(typeInfo)
                            .delimiter(TextFormatConstant.PLACEHOLDER)
                            .build();
        }
    }
}
