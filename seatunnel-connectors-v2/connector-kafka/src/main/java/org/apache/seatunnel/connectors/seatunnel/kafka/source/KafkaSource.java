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

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_OFFSETS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@AutoService(SeaTunnelSource.class)
@NoArgsConstructor
public class KafkaSource
    implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
    SupportParallelism {

    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private SeaTunnelDataType<SeaTunnelRow> typeInfo;
    private JobContext jobContext;
    private long discoveryIntervalMillis;
    private MessageFormatErrorHandleWay messageFormatErrorHandleWay =
        MessageFormatErrorHandleWay.FAIL;


    public KafkaSource(ReadonlyConfig option, SeaTunnelDataType<SeaTunnelRow> dataType) {

        this.metadata.setTopic(option.get(TOPIC));
        this.metadata.setPattern(option.get(PATTERN));

        this.metadata.setBootstrapServers(option.get(BOOTSTRAP_SERVERS));
        this.metadata.setProperties(new Properties());
        this.metadata.setConsumerGroup(option.get(CONSUMER_GROUP));
        this.metadata.setCommitOnCheckpoint(option.get(COMMIT_ON_CHECKPOINT));

        StartMode startMode = option.get(START_MODE);
        this.metadata.setStartMode(startMode);
        switch (startMode) {
            case TIMESTAMP:
                long startOffsetsTimestamp = option.get(START_MODE_TIMESTAMP);
                long currentTimestamp = System.currentTimeMillis();
                if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                    throw new IllegalArgumentException(
                        "start_mode.timestamp The value is smaller than 0 or smaller than the current time");
                }
                this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                break;
            case SPECIFIC_OFFSETS:
                Map<String, String> offsets = option.get(START_MODE_OFFSETS);
                if (offsets.isEmpty()) {
                    throw new IllegalArgumentException(
                        "start mode is "
                            + StartMode.SPECIFIC_OFFSETS
                            + "but no specific offsets were specified.");
                }
                Map<TopicPartition, Long> specificStartOffsets = new HashMap<>();
                offsets.keySet()
                    .forEach(
                        key -> {
                            int splitIndex = key.lastIndexOf("-");
                            String topic = key.substring(0, splitIndex);
                            String partition = key.substring(splitIndex + 1);
                            long offset = Long.parseLong(offsets.get(key));
                            TopicPartition topicPartition =
                                new TopicPartition(
                                    topic, Integer.parseInt(partition));
                            specificStartOffsets.put(topicPartition, offset);
                        });
                this.metadata.setSpecificStartOffsets(specificStartOffsets);
                break;
            default:
                break;
        }

        this.discoveryIntervalMillis = option.get(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);

        if (option.get(KAFKA_CONFIG) != null) {
            option.get(KAFKA_CONFIG).forEach(
                (key, value) ->
                    this.metadata.getProperties().put(key, value));
        }

        MessageFormatErrorHandleWay formatErrorWayOption =
            option.get(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
        switch (formatErrorWayOption) {
            case FAIL:
            case SKIP:
                this.messageFormatErrorHandleWay = formatErrorWayOption;
                break;
            default:
                break;
        }

        this.typeInfo = dataType;
        setDeserialization(option, dataType);

    }

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
        // TODO remove
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
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

    private void setDeserialization(ReadonlyConfig option, SeaTunnelDataType<SeaTunnelRow> dataType) {
        deserializationSchema = getDeserializationSchema(option, dataType);
    }


    private DeserializationSchema<SeaTunnelRow> getDeserializationSchema(ReadonlyConfig option, SeaTunnelDataType<SeaTunnelRow> typeInfo) {
        MessageFormat format = option.get(FORMAT);
        switch (format) {
            case JSON:
                if (typeInfo instanceof SeaTunnelRowType) {
                    return new JsonDeserializationSchema(false, false, (SeaTunnelRowType) typeInfo);
                } else {
                    throw new KafkaConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported multi-table format: " + format);
                }
            case TEXT:
                if (typeInfo instanceof SeaTunnelRowType) {
                    return
                        TextDeserializationSchema.builder()
                            .seaTunnelRowType((SeaTunnelRowType) typeInfo)
                            .delimiter(option.get(FIELD_DELIMITER))
                            .build();
                } else {
                    throw new KafkaConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported multi-table format: " + format);
                }
            case CANAL_JSON:
                if (typeInfo instanceof SeaTunnelRowType) {
                    return
                        CanalJsonDeserializationSchema.builder((SeaTunnelRowType) typeInfo)
                            .setIgnoreParseErrors(true)
                            .build();
                } else {
                    throw new KafkaConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported multi-table format: " + format);
                }
            default:
                throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }

}
