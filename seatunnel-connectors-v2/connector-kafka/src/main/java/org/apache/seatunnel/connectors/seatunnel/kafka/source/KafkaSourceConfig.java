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
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;
import org.apache.seatunnel.format.compatible.kafka.connect.json.KafkaConnectJsonFormatOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.TopicPartition;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONNECTOR_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
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

public class KafkaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private final ConsumerMetadata metadata;

    @Getter private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Getter private final CatalogTable catalogTable;

    @Getter private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    @Getter private final long discoveryIntervalMillis;

    public KafkaSourceConfig(ReadonlyConfig readonlyConfig) {
        this.metadata = createConsumerMetadata(readonlyConfig);
        this.discoveryIntervalMillis = readonlyConfig.get(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);
        this.messageFormatErrorHandleWay =
                readonlyConfig.get(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
        this.catalogTable = createCatalogTable(readonlyConfig);
        this.deserializationSchema = createDeserializationSchema(catalogTable, readonlyConfig);
    }

    private ConsumerMetadata createConsumerMetadata(ReadonlyConfig readonlyConfig) {
        ConsumerMetadata consumerMetadata = new ConsumerMetadata();
        consumerMetadata.setTopic(readonlyConfig.get(TOPIC));
        consumerMetadata.setBootstrapServers(readonlyConfig.get(BOOTSTRAP_SERVERS));
        consumerMetadata.setPattern(readonlyConfig.get(PATTERN));
        consumerMetadata.setProperties(new Properties());
        consumerMetadata.setConsumerGroup(readonlyConfig.get(CONSUMER_GROUP));
        consumerMetadata.setCommitOnCheckpoint(readonlyConfig.get(COMMIT_ON_CHECKPOINT));
        // parse start mode
        readonlyConfig
                .getOptional(START_MODE)
                .ifPresent(
                        startMode -> {
                            consumerMetadata.setStartMode(startMode);
                            switch (startMode) {
                                case TIMESTAMP:
                                    long startOffsetsTimestamp =
                                            readonlyConfig.get(START_MODE_TIMESTAMP);
                                    long currentTimestamp = System.currentTimeMillis();
                                    if (startOffsetsTimestamp < 0
                                            || startOffsetsTimestamp > currentTimestamp) {
                                        throw new IllegalArgumentException(
                                                "start_mode.timestamp The value is smaller than 0 or smaller than the current time");
                                    }
                                    consumerMetadata.setStartOffsetsTimestamp(
                                            startOffsetsTimestamp);
                                    break;
                                case SPECIFIC_OFFSETS:
                                    // Key is topic-partition, value is offset
                                    Map<String, Long> offsetMap =
                                            readonlyConfig.get(START_MODE_OFFSETS);
                                    if (MapUtils.isEmpty(offsetMap)) {
                                        throw new IllegalArgumentException(
                                                "start mode is "
                                                        + StartMode.SPECIFIC_OFFSETS
                                                        + "but no specific offsets were specified.");
                                    }
                                    Map<TopicPartition, Long> specificStartOffsets =
                                            new HashMap<>();
                                    offsetMap.forEach(
                                            (topicPartitionKey, offset) -> {
                                                int splitIndex = topicPartitionKey.lastIndexOf("-");
                                                String topic =
                                                        topicPartitionKey.substring(0, splitIndex);
                                                String partition =
                                                        topicPartitionKey.substring(splitIndex + 1);
                                                TopicPartition topicPartition =
                                                        new TopicPartition(
                                                                topic, Integer.parseInt(partition));
                                                specificStartOffsets.put(topicPartition, offset);
                                            });
                                    consumerMetadata.setSpecificStartOffsets(specificStartOffsets);
                                    break;
                                default:
                                    break;
                            }
                        });

        readonlyConfig
                .getOptional(KAFKA_CONFIG)
                .ifPresent(
                        kafkaConfig ->
                                kafkaConfig.forEach(
                                        (key, value) ->
                                                consumerMetadata.getProperties().put(key, value)));

        return consumerMetadata;
    }

    private CatalogTable createCatalogTable(ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMA);
        if (schemaOptions.isPresent()) {
            return CatalogTableUtil.buildWithConfig(readonlyConfig);
        } else {
            TableIdentifier tableIdentifier = TableIdentifier.of(CONNECTOR_IDENTITY, null, null);
            TableSchema tableSchema =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "content",
                                            new SeaTunnelRowType(
                                                    new String[] {"content"},
                                                    new SeaTunnelDataType<?>[] {
                                                        BasicType.STRING_TYPE
                                                    }),
                                            0,
                                            false,
                                            null,
                                            null))
                            .build();
            return CatalogTable.of(
                    tableIdentifier,
                    tableSchema,
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null);
        }
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();

        if (!readonlyConfig.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            return TextDeserializationSchema.builder()
                    .seaTunnelRowType(seaTunnelRowType)
                    .delimiter(TextFormatConstant.PLACEHOLDER)
                    .build();
        }

        MessageFormat format = readonlyConfig.get(FORMAT);
        switch (format) {
            case JSON:
                return new JsonDeserializationSchema(false, false, seaTunnelRowType);
            case TEXT:
                String delimiter = readonlyConfig.get(FIELD_DELIMITER);
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter(delimiter)
                        .build();
            case CANAL_JSON:
                return CanalJsonDeserializationSchema.builder(seaTunnelRowType)
                        .setIgnoreParseErrors(true)
                        .build();
            case COMPATIBLE_KAFKA_CONNECT_JSON:
                Boolean keySchemaEnable =
                        readonlyConfig.get(
                                KafkaConnectJsonFormatOptions.KEY_CONVERTER_SCHEMA_ENABLED);
                Boolean valueSchemaEnable =
                        readonlyConfig.get(
                                KafkaConnectJsonFormatOptions.VALUE_CONVERTER_SCHEMA_ENABLED);
                return new CompatibleKafkaConnectDeserializationSchema(
                        seaTunnelRowType, keySchemaEnable, valueSchemaEnable, false, false);
            case DEBEZIUM_JSON:
                boolean includeSchema = readonlyConfig.get(DEBEZIUM_RECORD_INCLUDE_SCHEMA);
                return new DebeziumJsonDeserializationSchema(seaTunnelRowType, true, includeSchema);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }
}
