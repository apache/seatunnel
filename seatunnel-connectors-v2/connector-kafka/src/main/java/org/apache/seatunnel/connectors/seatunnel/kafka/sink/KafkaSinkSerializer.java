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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseConstants;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseConstants.HEADERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseConstants.KEY;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseConstants.TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaBaseConstants.VALUE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_HEADERS_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_MESSAGE_VALUE_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.PARTITION_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TOPIC;

/** Shared local schema validation and serializer construction; never opens a Kafka client. */
final class KafkaSinkSerializer {

    private static final Pattern TOPIC_FIELD = Pattern.compile("\\$\\{(.*?)\\}", Pattern.DOTALL);

    private KafkaSinkSerializer() {}

    static SeaTunnelRowSerializer<byte[], byte[]> create(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        MessageFormat messageFormat = pluginConfig.get(FORMAT);
        String topic = pluginConfig.get(TOPIC);
        if (topic != null) {
            // Match topicExtractor: only the first template field supplies the runtime topic.
            Matcher matcher = TOPIC_FIELD.matcher(topic);
            if (matcher.find()
                    && !Arrays.asList(seaTunnelRowType.getFieldNames())
                            .contains(matcher.group(1))) {
                throw new LocalValidationException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "topic references a field absent from the upstream schema",
                        String.format("Field name { %s } is not found!", topic));
            }
        }

        if (pluginConfig.get(KAFKA_MESSAGE_VALUE_FIELDS) != null) {
            if (MessageFormat.NATIVE.equals(messageFormat)
                    || MessageFormat.COMPATIBLE_DEBEZIUM_JSON.equals(messageFormat)
                    || MessageFormat.COMPATIBLE_KAFKA_CONNECT_JSON.equals(messageFormat)) {
                throw new LocalValidationException(
                        CommonErrorCode.OPERATION_NOT_SUPPORTED,
                        "kafka_message_value_fields is incompatible with format",
                        String.format(
                                "kafka_message_value_fields is not supported for %s format",
                                messageFormat));
            }
        }

        if (MessageFormat.NATIVE.equals(messageFormat)) {
            // Validate that kafka_headers_fields is not configured for NATIVE format
            if (pluginConfig.get(KAFKA_HEADERS_FIELDS) != null) {
                throw new LocalValidationException(
                        CommonErrorCode.OPERATION_NOT_SUPPORTED,
                        "kafka_headers_fields is incompatible with NATIVE format",
                        "kafka_headers_fields is not supported with NATIVE format. Please use JSON, TEXT, or other formats.");
            }
            checkNativeSeaTunnelType(seaTunnelRowType);
            return DefaultSeaTunnelRowSerializer.create(topic, messageFormat, seaTunnelRowType);
        }

        String delimiter = DEFAULT_FIELD_DELIMITER;

        if (pluginConfig.get(FIELD_DELIMITER) != null) {
            delimiter = pluginConfig.get(FIELD_DELIMITER);
        }
        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null && pluginConfig.get(PARTITION) != null) {
            throw new LocalValidationException(
                    KafkaConnectorErrorCode.GET_TRANSACTIONMANAGER_FAILED,
                    "partition and partition_key_fields cannot both be configured",
                    "Cannot select both `partiton` and `partition_key_fields`. You can configure only one of them");
        }

        // Validate that partition_key_fields and kafka_headers_fields don't overlap
        List<String> partitionKeyFields = getPartitionKeyFields(pluginConfig, seaTunnelRowType);
        List<String> headerFields = getHeaderFields(pluginConfig, seaTunnelRowType);
        List<String> messageValueFields = getMessageValueFields(pluginConfig, seaTunnelRowType);
        if (!partitionKeyFields.isEmpty() && !headerFields.isEmpty()) {
            for (String headerField : headerFields) {
                if (partitionKeyFields.contains(headerField)) {
                    throw new LocalValidationException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            "partition_key_fields and kafka_headers_fields must not overlap",
                            String.format(
                                    "Field '%s' cannot be in both partition_key_fields and kafka_headers_fields",
                                    headerField));
                }
            }
        }
        // Validate that kafka_message_value_fields and kafka_headers_fields don't overlap
        if (!messageValueFields.isEmpty() && !headerFields.isEmpty()) {
            for (String headerField : headerFields) {
                if (messageValueFields.contains(headerField)) {
                    throw new LocalValidationException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            "kafka_message_value_fields and kafka_headers_fields must not overlap",
                            String.format(
                                    "Field '%s' cannot be in both kafka_message_value_fields and kafka_headers_fields",
                                    headerField));
                }
            }
        }

        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
            return DefaultSeaTunnelRowSerializer.create(
                    topic,
                    partitionKeyFields,
                    headerFields,
                    messageValueFields,
                    seaTunnelRowType,
                    messageFormat,
                    delimiter,
                    pluginConfig);
        }
        if (pluginConfig.get(PARTITION) != null) {
            return DefaultSeaTunnelRowSerializer.create(
                    topic,
                    pluginConfig.get(PARTITION),
                    headerFields,
                    messageValueFields,
                    seaTunnelRowType,
                    messageFormat,
                    delimiter,
                    pluginConfig);
        }
        // By default, all partitions are sent randomly
        return DefaultSeaTunnelRowSerializer.create(
                topic,
                Collections.<String>emptyList(),
                headerFields,
                messageValueFields,
                seaTunnelRowType,
                messageFormat,
                delimiter,
                pluginConfig);
    }

    private static List<String> getPartitionKeyFields(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {

        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
            List<String> partitionKeyFields = pluginConfig.get(PARTITION_KEY_FIELDS);
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String partitionKeyField : partitionKeyFields) {
                if (!rowTypeFieldNames.contains(partitionKeyField)) {
                    throw new LocalValidationException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "partition_key_fields contains a field absent from the upstream schema",
                            String.format(
                                    "Partition key field not found: %s, rowType: %s",
                                    partitionKeyField, rowTypeFieldNames));
                }
            }
            return partitionKeyFields;
        }
        return Collections.emptyList();
    }

    private static List<String> getHeaderFields(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {

        if (pluginConfig.get(KAFKA_HEADERS_FIELDS) != null) {
            List<String> headerFields = pluginConfig.get(KAFKA_HEADERS_FIELDS);
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String headerField : headerFields) {
                if (!rowTypeFieldNames.contains(headerField)) {
                    throw new LocalValidationException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            "kafka_headers_fields contains a field absent from the upstream schema",
                            String.format(
                                    "Header field not found: %s, rowType: %s",
                                    headerField, rowTypeFieldNames));
                }
            }
            return headerFields;
        }
        return Collections.emptyList();
    }

    private static List<String> getMessageValueFields(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        if (pluginConfig.get(KAFKA_MESSAGE_VALUE_FIELDS) != null) {
            List<String> messageValueFields = pluginConfig.get(KAFKA_MESSAGE_VALUE_FIELDS);
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String messageValueField : messageValueFields) {
                if (!rowTypeFieldNames.contains(messageValueField)) {
                    throw new LocalValidationException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            "kafka_message_value_fields contains a field absent from the upstream schema",
                            String.format(
                                    "Message value field not found: %s, rowType: %s",
                                    messageValueField, rowTypeFieldNames));
                }
            }
            return messageValueFields;
        }
        return Collections.emptyList();
    }

    private static void checkNativeSeaTunnelType(SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelRowType exceptRowType = nativeTableSchema().toPhysicalRowDataType();
        for (int i = 0; i < exceptRowType.getFieldTypes().length; i++) {
            String exceptField = exceptRowType.getFieldNames()[i];
            SeaTunnelDataType<?> exceptFieldType = exceptRowType.getFieldTypes()[i];
            int fieldIndex = seaTunnelRowType.indexOf(exceptField, false);
            if (fieldIndex < 0) {
                throw new LocalValidationException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "NATIVE format requires its standard fields in the upstream schema",
                        String.format("Field name { %s } is not found!", exceptField));
            }
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(fieldIndex);
            if (exceptFieldType.getSqlType() != fieldType.getSqlType()) {
                throw new LocalValidationException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "NATIVE format field types do not match the upstream schema",
                        String.format(
                                "Field name { %s } unsupported sql type { %s } !",
                                exceptField, fieldType.getSqlType()));
            }
        }
    }

    /** Keeps runtime diagnostics intact while exposing only connector-owned text to dry-run. */
    static final class LocalValidationException extends KafkaConnectorException {
        private final String dryRunReason;
        private final String runtimeMessage;

        private LocalValidationException(
                SeaTunnelErrorCode errorCode, String dryRunReason, String runtimeMessage) {
            super(errorCode, errorParameters(errorCode, runtimeMessage));
            this.dryRunReason = dryRunReason;
            this.runtimeMessage = runtimeMessage;
        }

        String getDryRunReason() {
            return dryRunReason;
        }

        @Override
        public String getMessage() {
            return super.getMessage() + " - " + runtimeMessage;
        }

        private static Map<String, String> errorParameters(
                SeaTunnelErrorCode code, String message) {
            Map<String, String> params = new HashMap<>();
            if (code == CommonErrorCode.ILLEGAL_ARGUMENT) {
                params.put("argument", message);
                params.put("operation", "Kafka sink serialization");
            } else if (code == CommonErrorCode.OPERATION_NOT_SUPPORTED) {
                params.put("identifier", "Kafka");
                params.put("operation", message);
            } else if (code == CommonErrorCode.UNSUPPORTED_DATA_TYPE) {
                params.put("identifier", "Kafka");
                params.put("dataType", "upstream schema");
                params.put("field", message);
            }
            return params;
        }
    }

    private static TableSchema nativeTableSchema() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                HEADERS,
                                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                                0,
                                false,
                                null,
                                null))
                .column(
                        PhysicalColumn.of(
                                KEY, PrimitiveByteArrayType.INSTANCE, 0, false, null, null))
                .column(
                        PhysicalColumn.of(
                                KafkaBaseConstants.PARTITION,
                                BasicType.INT_TYPE,
                                0,
                                false,
                                null,
                                null))
                .column(PhysicalColumn.of(TIMESTAMP, BasicType.LONG_TYPE, 0, false, null, null))
                .column(
                        PhysicalColumn.of(
                                VALUE, PrimitiveByteArrayType.INSTANCE, 0, false, null, null))
                .build();
    }
}
