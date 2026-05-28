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
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqSourceOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.rocketmq.common.message.MessageQueue;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** RocketMQ source configuration, supports both single-table and multi-table modes. */
public class RocketMqSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private final ConsumerMetadata metadata;
    @Getter private final Map<String, TopicTableConfig> topicConfigs;
    @Getter private final long discoveryIntervalMillis;

    public RocketMqSourceConfig(ReadonlyConfig readonlyConfig) {
        this.topicConfigs = new LinkedHashMap<>();
        this.discoveryIntervalMillis =
                readonlyConfig.get(RocketMqSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);

        List<Map<String, Object>> tableConfigList = null;
        if (readonlyConfig.getOptional(RocketMqSourceOptions.TABLE_CONFIGS).isPresent()) {
            tableConfigList = readonlyConfig.get(RocketMqSourceOptions.TABLE_CONFIGS);
        } else if (readonlyConfig.getOptional(RocketMqSourceOptions.TABLE_LIST).isPresent()) {
            tableConfigList = readonlyConfig.get(RocketMqSourceOptions.TABLE_LIST);
        }

        if (tableConfigList != null) {
            // Multi-table mode
            this.metadata = buildConsumerMetadata(readonlyConfig, Collections.emptyList());
            List<String> allTopics = new ArrayList<>();
            for (Map<String, Object> tableConfig : tableConfigList) {
                parseTableConfig(ReadonlyConfig.fromMap(tableConfig), allTopics);
            }
            this.metadata.setTopics(allTopics);
        } else {
            // Single-table mode (backward compatible)
            List<String> topics =
                    Arrays.stream(
                                    readonlyConfig
                                            .get(RocketMqSourceOptions.TOPICS)
                                            .split(RocketMqSourceOptions.DEFAULT_FIELD_DELIMITER))
                            .map(String::trim)
                            .filter(t -> !t.isEmpty())
                            .collect(Collectors.toList());
            this.metadata = buildConsumerMetadata(readonlyConfig, topics);
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            DeserializationSchema<SeaTunnelRow> deserializationSchema =
                    buildDeserialization(readonlyConfig, catalogTable);
            List<String> tags = parseTags(readonlyConfig);
            for (String topic : topics) {
                TopicTableConfig config = new TopicTableConfig();
                config.setCatalogTable(catalogTable);
                config.setDeserializationSchema(deserializationSchema);
                config.setTags(tags);
                topicConfigs.put(topic, config);
            }
        }
    }

    private void parseTableConfig(ReadonlyConfig tableConfig, List<String> allTopics) {
        String topicsStr = tableConfig.get(RocketMqSourceOptions.TOPICS);
        if (topicsStr == null || topicsStr.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "'topics' must be configured in each tables_configs entry, but got: "
                            + tableConfig);
        }
        List<String> topics =
                Arrays.stream(topicsStr.split(RocketMqSourceOptions.DEFAULT_FIELD_DELIMITER))
                        .map(String::trim)
                        .filter(t -> !t.isEmpty())
                        .collect(Collectors.toList());

        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(tableConfig);
        if (TablePath.DEFAULT.equals(catalogTable.getTablePath())) {
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("", TablePath.of(null, null, topics.get(0))),
                            catalogTable.getTableSchema(),
                            catalogTable.getOptions(),
                            catalogTable.getPartitionKeys(),
                            catalogTable.getComment());
        }
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                buildDeserialization(tableConfig, catalogTable);
        List<String> tags = parseTags(tableConfig);
        StartMode startMode =
                tableConfig.getOptional(RocketMqSourceOptions.START_MODE).orElse(null);
        Long startTimestamp = null;

        if (startMode != null) {
            switch (startMode) {
                case CONSUME_FROM_TIMESTAMP:
                    startTimestamp = tableConfig.get(RocketMqSourceOptions.START_MODE_TIMESTAMP);
                    if (startTimestamp == null) {
                        throw new IllegalArgumentException(
                                "When 'start.mode' is set to 'CONSUME_FROM_TIMESTAMP' in tables_configs, "
                                        + "'start.mode.timestamp' must also be specified in the same table config entry. "
                                        + "Topics: "
                                        + topicsStr);
                    }
                    long currentTimestamp = System.currentTimeMillis();
                    if (startTimestamp < 0 || startTimestamp > currentTimestamp) {
                        throw new IllegalArgumentException(
                                "The offsets timestamp value is smaller than 0 or larger"
                                        + " than the current time");
                    }
                    break;
                case CONSUME_FROM_SPECIFIC_OFFSETS:
                    Map<String, Long> offsetConfigMap =
                            tableConfig.get(RocketMqSourceOptions.START_MODE_OFFSETS);
                    if (offsetConfigMap == null || offsetConfigMap.isEmpty()) {
                        throw new IllegalArgumentException(
                                "When 'start.mode' is set to 'CONSUME_FROM_SPECIFIC_OFFSETS' in tables_configs, "
                                        + "'start.mode.offsets' must also be specified in the same table config entry. "
                                        + "Topics: "
                                        + topicsStr);
                    }
                    Map<MessageQueue, Long> specificOffsets = metadata.getSpecificStartOffsets();
                    if (specificOffsets == null) {
                        specificOffsets = new HashMap<>();
                        metadata.setSpecificStartOffsets(specificOffsets);
                    }
                    for (Map.Entry<String, Long> entry : offsetConfigMap.entrySet()) {
                        int splitIndex = entry.getKey().lastIndexOf("-");
                        String topicName = entry.getKey().substring(0, splitIndex);
                        int queueId = Integer.parseInt(entry.getKey().substring(splitIndex + 1));
                        specificOffsets.put(
                                new MessageQueue(topicName, null, queueId), entry.getValue());
                    }
                    break;
                default:
                    break;
            }
        }

        for (String topic : topics) {
            TopicTableConfig config = new TopicTableConfig();
            config.setCatalogTable(catalogTable);
            config.setDeserializationSchema(deserializationSchema);
            config.setTags(tags);
            config.setStartMode(startMode);
            config.setStartTimestamp(startTimestamp);
            topicConfigs.put(topic, config);
            allTopics.add(topic);
        }
    }

    private List<String> parseTags(ReadonlyConfig config) {
        String tags = config.get(RocketMqSourceOptions.TAGS);
        if (tags != null && !tags.trim().isEmpty()) {
            return Arrays.stream(tags.split(RocketMqSourceOptions.DEFAULT_FIELD_DELIMITER))
                    .map(String::trim)
                    .filter(tag -> !tag.isEmpty())
                    .distinct()
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private ConsumerMetadata buildConsumerMetadata(
            ReadonlyConfig readonlyConfig, List<String> topics) {
        ConsumerMetadata consumerMetadata = new ConsumerMetadata();
        consumerMetadata.setTopics(topics);
        consumerMetadata.setTags(Collections.emptyList());

        RocketMqBaseConfiguration.Builder baseConfigBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .consumer()
                        .namesrvAddr(readonlyConfig.get(RocketMqSourceOptions.NAME_SRV_ADDR));
        if (readonlyConfig.getOptional(RocketMqSourceOptions.ACCESS_KEY).isPresent()) {
            baseConfigBuilder.accessKey(readonlyConfig.get(RocketMqSourceOptions.ACCESS_KEY));
        }
        if (readonlyConfig.getOptional(RocketMqSourceOptions.SECRET_KEY).isPresent()) {
            baseConfigBuilder.secretKey(readonlyConfig.get(RocketMqSourceOptions.SECRET_KEY));
        }
        baseConfigBuilder.aclEnable(readonlyConfig.get(RocketMqSourceOptions.ACL_ENABLED));
        baseConfigBuilder.groupId(readonlyConfig.get(RocketMqSourceOptions.CONSUMER_GROUP));
        baseConfigBuilder.batchSize(readonlyConfig.get(RocketMqSourceOptions.BATCH_SIZE));
        baseConfigBuilder.pollTimeoutMillis(
                readonlyConfig.get(RocketMqSourceOptions.POLL_TIMEOUT_MILLIS));

        consumerMetadata.setBaseConfig(baseConfigBuilder.build());
        consumerMetadata.setEnabledCommitCheckpoint(
                readonlyConfig.get(RocketMqSourceOptions.COMMIT_ON_CHECKPOINT));

        StartMode startMode = readonlyConfig.get(RocketMqSourceOptions.START_MODE);
        switch (startMode) {
            case CONSUME_FROM_TIMESTAMP:
                long startOffsetsTimestamp =
                        readonlyConfig.get(RocketMqSourceOptions.START_MODE_TIMESTAMP);
                long currentTimestamp = System.currentTimeMillis();
                if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                    throw new IllegalArgumentException(
                            "The offsets timestamp value is smaller than 0 or larger"
                                    + " than the current time");
                }
                consumerMetadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                break;
            case CONSUME_FROM_SPECIFIC_OFFSETS:
                Map<String, Long> offsetConfigMap =
                        readonlyConfig.get(RocketMqSourceOptions.START_MODE_OFFSETS);
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
                consumerMetadata.setSpecificStartOffsets(specificStartOffsets);
                break;
            default:
                break;
        }
        consumerMetadata.setStartMode(startMode);

        return consumerMetadata;
    }

    private DeserializationSchema<SeaTunnelRow> buildDeserialization(
            ReadonlyConfig config, CatalogTable catalogTable) {
        DeserializationSchema<SeaTunnelRow> schema;
        if (config.getOptional(RocketMqSourceOptions.SCHEMA).isPresent()) {
            SchemaFormat format = config.get(RocketMqSourceOptions.FORMAT);
            boolean ignoreParseErrors = config.get(RocketMqSourceOptions.IGNORE_PARSE_ERRORS);
            switch (format) {
                case JSON:
                    schema = new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
                    break;
                case TEXT:
                    schema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                                    .delimiter(config.get(RocketMqSourceOptions.FIELD_DELIMITER))
                                    .setCatalogTable(catalogTable)
                                    .build();
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported format: " + format);
            }
        } else {
            schema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                            .delimiter(String.valueOf('\002'))
                            .setCatalogTable(catalogTable)
                            .build();
        }
        String tableId = catalogTable.getTablePath().toString();
        return new RocketMqTableIdDeserializationSchema(schema, tableId);
    }

    public List<CatalogTable> getCatalogTables() {
        return topicConfigs.values().stream()
                .map(TopicTableConfig::getCatalogTable)
                .distinct()
                .collect(Collectors.toList());
    }
}
