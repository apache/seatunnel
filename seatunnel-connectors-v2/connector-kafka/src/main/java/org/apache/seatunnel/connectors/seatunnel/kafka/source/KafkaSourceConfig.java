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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.TableIdentifierConfig;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;
import org.apache.seatunnel.format.compatible.kafka.connect.json.KafkaConnectJsonFormatOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchemaDispatcher;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.json.maxwell.MaxWellJsonDeserializationSchema;
import org.apache.seatunnel.format.json.ogg.OggJsonDeserializationSchema;
import org.apache.seatunnel.format.protobuf.ProtobufDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.DEBEZIUM_RECORD_TABLE_FILTER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.KEY_POLL_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.PROTOBUF_MESSAGE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.PROTOBUF_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.START_MODE_OFFSETS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.START_MODE_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.TOPIC;

public class KafkaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private final String bootstrap;
    @Getter private final Map<TablePath, ConsumerMetadata> mapMetadata;
    @Getter private final boolean commitOnCheckpoint;
    @Getter private final Properties properties;
    @Getter private final long discoveryIntervalMillis;
    @Getter private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;
    @Getter private final String consumerGroup;
    @Getter private final long pollTimeout;

    public KafkaSourceConfig(ReadonlyConfig readonlyConfig) {
        this.bootstrap = readonlyConfig.get(BOOTSTRAP_SERVERS);
        this.mapMetadata = createMapConsumerMetadata(readonlyConfig);
        this.commitOnCheckpoint = readonlyConfig.get(COMMIT_ON_CHECKPOINT);
        this.properties = createKafkaProperties(readonlyConfig);
        this.discoveryIntervalMillis = readonlyConfig.get(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);
        this.messageFormatErrorHandleWay =
                readonlyConfig.get(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
        this.pollTimeout = readonlyConfig.get(KEY_POLL_TIMEOUT);
        this.consumerGroup = readonlyConfig.get(CONSUMER_GROUP);
    }

    private Properties createKafkaProperties(ReadonlyConfig readonlyConfig) {
        Properties resultProperties = new Properties();
        readonlyConfig.getOptional(KAFKA_CONFIG).ifPresent(resultProperties::putAll);
        return resultProperties;
    }

    private Map<TablePath, ConsumerMetadata> createMapConsumerMetadata(
            ReadonlyConfig readonlyConfig) {
        List<ConsumerMetadata> consumerMetadataList;
        if (readonlyConfig.getOptional(KafkaSourceOptions.TABLE_CONFIGS).isPresent()) {
            consumerMetadataList =
                    readonlyConfig.get(KafkaSourceOptions.TABLE_CONFIGS).stream()
                            .map(ReadonlyConfig::fromMap)
                            .map(this::createConsumerMetadata)
                            .collect(Collectors.toList());
        } else if (readonlyConfig.getOptional(KafkaSourceOptions.TABLE_LIST).isPresent()) {
            consumerMetadataList =
                    readonlyConfig.get(KafkaSourceOptions.TABLE_LIST).stream()
                            .map(ReadonlyConfig::fromMap)
                            .map(this::createConsumerMetadata)
                            .collect(Collectors.toList());
        } else {
            consumerMetadataList =
                    Collections.singletonList(createConsumerMetadata(readonlyConfig));
        }

        return consumerMetadataList.stream()
                .collect(
                        Collectors.toMap(
                                consumerMetadata -> TablePath.of(null, consumerMetadata.getTopic()),
                                consumerMetadata -> consumerMetadata));
    }

    private ConsumerMetadata createConsumerMetadata(ReadonlyConfig readonlyConfig) {
        ConsumerMetadata consumerMetadata = new ConsumerMetadata();
        consumerMetadata.setTopic(readonlyConfig.get(TOPIC));
        consumerMetadata.setPattern(readonlyConfig.get(PATTERN));
        consumerMetadata.setProperties(new Properties());
        // Create a catalog
        CatalogTable catalogTable = createCatalogTable(readonlyConfig);
        consumerMetadata.setCatalogTable(catalogTable);
        consumerMetadata.setDeserializationSchema(
                createDeserializationSchema(catalogTable, readonlyConfig));

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

        return consumerMetadata;
    }

    private CatalogTable createCatalogTable(ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(KafkaSourceOptions.SCHEMA);
        TablePath tablePath = TablePath.of(null, readonlyConfig.get(TOPIC));
        TableSchema tableSchema;
        if (schemaOptions.isPresent()) {
            tableSchema = new ReadonlyConfigParser().parse(readonlyConfig);
        } else {
            tableSchema =
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
        }
        return CatalogTable.of(
                TableIdentifier.of("", tablePath),
                tableSchema,
                new HashMap<String, String>() {
                    {
                        Optional.ofNullable(readonlyConfig.get(PROTOBUF_MESSAGE_NAME))
                                .ifPresent(value -> put(PROTOBUF_MESSAGE_NAME.key(), value));

                        Optional.ofNullable(readonlyConfig.get(PROTOBUF_SCHEMA))
                                .ifPresent(value -> put(PROTOBUF_SCHEMA.key(), value));
                    }
                },
                Collections.emptyList(),
                null);
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        MessageFormat format = readonlyConfig.get(FORMAT);

        if (!readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            return TextDeserializationSchema.builder()
                    .seaTunnelRowType(seaTunnelRowType)
                    .delimiter(TextFormatConstant.PLACEHOLDER)
                    .setCatalogTable(catalogTable)
                    .build();
        }

        switch (format) {
            case JSON:
                return new JsonDeserializationSchema(catalogTable, false, false);
            case TEXT:
                String delimiter = readonlyConfig.get(FIELD_DELIMITER);
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter(delimiter)
                        .build();
            case CANAL_JSON:
                return CanalJsonDeserializationSchema.builder(catalogTable)
                        .setIgnoreParseErrors(true)
                        .build();
            case OGG_JSON:
                return OggJsonDeserializationSchema.builder(catalogTable)
                        .setIgnoreParseErrors(true)
                        .build();
            case MAXWELL_JSON:
                return MaxWellJsonDeserializationSchema.builder(catalogTable)
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
                        catalogTable, keySchemaEnable, valueSchemaEnable, false, false);
            case DEBEZIUM_JSON:
                boolean includeSchema = readonlyConfig.get(DEBEZIUM_RECORD_INCLUDE_SCHEMA);
                TableIdentifierConfig tableFilter =
                        readonlyConfig.get(DEBEZIUM_RECORD_TABLE_FILTER);
                if (tableFilter != null) {
                    TablePath tablePath =
                            TablePath.of(
                                    StringUtils.isNotEmpty(tableFilter.getDatabaseName())
                                            ? tableFilter.getDatabaseName()
                                            : null,
                                    StringUtils.isNotEmpty(tableFilter.getSchemaName())
                                            ? tableFilter.getSchemaName()
                                            : null,
                                    StringUtils.isNotEmpty(tableFilter.getTableName())
                                            ? tableFilter.getTableName()
                                            : null);
                    Map<TablePath, DebeziumJsonDeserializationSchema> tableDeserializationMap =
                            Collections.singletonMap(
                                    tablePath,
                                    new DebeziumJsonDeserializationSchema(
                                            catalogTable, true, includeSchema));
                    return new DebeziumJsonDeserializationSchemaDispatcher(
                            tableDeserializationMap, true, includeSchema);
                } else {
                    return new DebeziumJsonDeserializationSchema(catalogTable, true, includeSchema);
                }
            case AVRO:
                return new AvroDeserializationSchema(catalogTable);
            case PROTOBUF:
                return new ProtobufDeserializationSchema(catalogTable);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported format: " + format);
        }
    }
}
