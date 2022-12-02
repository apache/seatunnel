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
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_OFFSETS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@AutoService(SeaTunnelSource.class)
public class KafkaSource implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState> {

    private static final String DEFAULT_CONSUMER_GROUP = "SeaTunnel-Consumer-Group";

    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;
    private long discoveryIntervalMillis = KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.defaultValue();

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "Kafka";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, TOPIC.key(), BOOTSTRAP_SERVERS.key());
        if (!result.isSuccess()) {
            throw new KafkaConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s", getPluginName(), PluginType.SOURCE, result.getMsg())
            );
        }
        this.metadata.setTopic(config.getString(TOPIC.key()));
        if (config.hasPath(PATTERN.key())) {
            this.metadata.setPattern(config.getBoolean(PATTERN.key()));
        }
        this.metadata.setBootstrapServers(config.getString(BOOTSTRAP_SERVERS.key()));
        this.metadata.setProperties(new Properties());

        if (config.hasPath(CONSUMER_GROUP.key())) {
            this.metadata.setConsumerGroup(config.getString(CONSUMER_GROUP.key()));
        } else {
            this.metadata.setConsumerGroup(DEFAULT_CONSUMER_GROUP);
        }

        if (config.hasPath(COMMIT_ON_CHECKPOINT.key())) {
            this.metadata.setCommitOnCheckpoint(config.getBoolean(COMMIT_ON_CHECKPOINT.key()));
        }

        if (config.hasPath(START_MODE.key())) {
            StartMode startMode = StartMode.valueOf(config.getString(START_MODE.key()).toUpperCase());
            this.metadata.setStartMode(startMode);
            switch (startMode) {
                case TIMESTAMP:
                    long startOffsetsTimestamp = config.getLong(START_MODE_TIMESTAMP.key());
                    long currentTimestamp = System.currentTimeMillis();
                    if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                        throw new IllegalArgumentException("start_mode.timestamp The value is smaller than 0 or smaller than the current time");
                    }
                    this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                    break;
                case SPECIFIC_OFFSETS:
                    Config offsets = config.getConfig(START_MODE_OFFSETS.key());
                    ConfigRenderOptions options = ConfigRenderOptions.concise();
                    String offsetsJson = offsets.root().render(options);
                    if (offsetsJson == null) {
                        throw new IllegalArgumentException("start mode is " + StartMode.SPECIFIC_OFFSETS + "but no specific offsets were specified.");
                    }
                    Map<TopicPartition, Long> specificStartOffsets = new HashMap<>();
                    ObjectNode jsonNodes = JsonUtils.parseObject(offsetsJson);
                    jsonNodes.fieldNames().forEachRemaining(key -> {
                        String[] topicAndPartition = key.split("-");
                        long offset = jsonNodes.get(key).asLong();
                        TopicPartition topicPartition = new TopicPartition(topicAndPartition[0], Integer.valueOf(topicAndPartition[1]));
                        specificStartOffsets.put(topicPartition, offset);
                    });
                    this.metadata.setSpecificStartOffsets(specificStartOffsets);
                    break;
                default:
                    break;
            }
        }

        if (config.hasPath(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key())) {
            this.discoveryIntervalMillis = config.getLong(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS.key());
        }

        TypesafeConfigUtils.extractSubConfig(config, "kafka.", false).entrySet().forEach(e -> {
            this.metadata.getProperties().put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
        });

        setDeserialization(config);
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new KafkaSourceReader(this.metadata, deserializationSchema, readerContext);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) throws Exception {
        return new KafkaSourceSplitEnumerator(this.metadata, enumeratorContext, discoveryIntervalMillis);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> restoreEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext, KafkaSourceState checkpointState) throws Exception {
        return new KafkaSourceSplitEnumerator(this.metadata, enumeratorContext, checkpointState, discoveryIntervalMillis);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(SCHEMA.key())) {
            Config schema = config.getConfig(SCHEMA.key());
            typeInfo = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
            String format = DEFAULT_FORMAT;
            if (config.hasPath(FORMAT.key())) {
                format = config.getString(FORMAT.key());
            }
            if (DEFAULT_FORMAT.equals(format)) {
                deserializationSchema = new JsonDeserializationSchema(false, false, typeInfo);
            } else if ("text".equals(format)) {
                String delimiter = DEFAULT_FIELD_DELIMITER;
                if (config.hasPath(FIELD_DELIMITER.key())) {
                    delimiter = config.getString(FIELD_DELIMITER.key());
                }
                deserializationSchema = TextDeserializationSchema.builder()
                    .seaTunnelRowType(typeInfo)
                    .delimiter(delimiter)
                    .build();
            } else {
                // TODO: use format SPI
                throw new UnsupportedOperationException("Unsupported format: " + format);
            }
        } else {
            typeInfo = SeaTunnelSchema.buildSimpleTextSchema();
            this.deserializationSchema = TextDeserializationSchema.builder()
                .seaTunnelRowType(typeInfo)
                .delimiter(String.valueOf('\002'))
                .build();
        }
    }
}
