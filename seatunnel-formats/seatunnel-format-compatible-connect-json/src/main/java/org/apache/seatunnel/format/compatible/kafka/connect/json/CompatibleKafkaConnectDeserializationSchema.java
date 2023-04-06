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

package org.apache.seatunnel.format.compatible.kafka.connect.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.ConnectUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.data.Envelope;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/** Compatible kafka connect deserialization schema */
@RequiredArgsConstructor
public class CompatibleKafkaConnectDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {

    private static final String INCLUDE_SCHEMA_METHOD = "convertToJsonWithEnvelope";
    private static final String EXCLUDE_SCHEMA_METHOD = "convertToJsonWithoutEnvelope";
    private static final String KAFKA_CONNECT_SINK_RECORD_PAYLOAD = "payload";
    private static final String DBZ_OP_FIELD = "__op";
    private static final String DBZ_SOURCE_CONNECTOR = "__source_connector";
    private static final String DBZ_SOURCE_CONNECTOR_MONGODB_VALUE = "mongodb";
    private transient JsonConverter keyConverter;
    private transient JsonConverter valueConverter;
    private transient ExtractNewRecordState<SinkRecord> extractNewRecordState;
    private transient ExtractNewDocumentState<SinkRecord> extractNewDocumentState;
    private transient Method keyConverterMethod;
    private transient Method valueConverterMethod;
    private final SeaTunnelRowType seaTunnelRowType;
    private final JsonToRowConverters.JsonToRowConverter runtimeConverter;
    private final Boolean fromDebeziumRecord;
    private final boolean keySchemaEnable;
    private final boolean valueSchemaEnable;
    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CompatibleKafkaConnectDeserializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            @NonNull Config config,
            boolean failOnMissingField,
            boolean ignoreParseErrors) {

        Map<String, String> configMap = ReadonlyConfig.fromConfig(config).toMap();
        this.seaTunnelRowType = seaTunnelRowType;
        this.fromDebeziumRecord = KafkaConnectJsonFormatOptions.getDebeziumRecordEnabled(configMap);
        this.keySchemaEnable =
                KafkaConnectJsonFormatOptions.getKeyConverterSchemaEnabled(configMap);
        this.valueSchemaEnable =
                KafkaConnectJsonFormatOptions.getValueConverterSchemaEnabled(configMap);

        // Runtime converter
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createConverter(checkNotNull(seaTunnelRowType));
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedEncodingException();
    }

    /**
     * Deserialize kafka consumer record
     *
     * @param msg
     * @param out
     * @throws Exception
     */
    public void deserialize(ConsumerRecord<byte[], byte[]> msg, Collector<SeaTunnelRow> out)
            throws InvocationTargetException, IllegalAccessException {
        tryInitConverter();
        SinkRecord record = convertToSinkRecord(msg);
        RowKind rowKind = RowKind.INSERT;
        if (fromDebeziumRecord) {
            // Rewrite capture data
            record = convertByTransforms(record);
            rowKind = extractDebeziumRecordRowKind(record);
        }
        JsonNode jsonNode =
                (JsonNode)
                        valueConverterMethod.invoke(
                                valueConverter, record.valueSchema(), record.value());
        JsonNode payload = jsonNode.get(KAFKA_CONNECT_SINK_RECORD_PAYLOAD);
        if (payload.isArray()) {
            ArrayNode arrayNode = (ArrayNode) payload;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow row = convertJsonNode(arrayNode.get(i));
                row.setRowKind(rowKind);
                out.collect(row);
            }
        } else {
            SeaTunnelRow row = convertJsonNode(payload);
            row.setRowKind(rowKind);
            out.collect(row);
        }
    }

    private RowKind extractDebeziumRecordRowKind(SinkRecord record) {
        Header header = record.headers().lastWithName(DBZ_OP_FIELD);
        if (header != null) {
            String op = header.value().toString();
            Envelope.Operation operation = Envelope.Operation.forCode(op);
            switch (operation) {
                case READ:
                case CREATE:
                    return RowKind.INSERT;
                case UPDATE:
                    return RowKind.UPDATE_AFTER;
                case TRUNCATE:
                case DELETE:
                    return RowKind.DELETE;
            }
        }
        return RowKind.INSERT;
    }

    private SeaTunnelRow convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return null;
        }
        try {
            org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode jsonData =
                    objectMapper.readTree(jsonNode.toString());
            return (SeaTunnelRow) runtimeConverter.convert(jsonData);
        } catch (Throwable t) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", jsonNode),
                    t);
        }
    }

    private SinkRecord convertToSinkRecord(ConsumerRecord<byte[], byte[]> msg) {
        SchemaAndValue keyAndSchema =
                keyConverter.toConnectData(msg.topic(), msg.headers(), msg.key());
        SchemaAndValue valueAndSchema =
                valueConverter.toConnectData(msg.topic(), msg.headers(), msg.value());
        Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
        return new SinkRecord(
                msg.topic(),
                msg.partition(),
                keyAndSchema.schema(),
                keyAndSchema.value(),
                valueAndSchema.schema(),
                valueAndSchema.value(),
                msg.offset(),
                timestamp,
                msg.timestampType(),
                null);
    }

    private SinkRecord convertByTransforms(SinkRecord record) {
        Header header = record.headers().lastWithName(DBZ_SOURCE_CONNECTOR);
        if (header != null) {
            String connector = header.value().toString();
            if (DBZ_SOURCE_CONNECTOR_MONGODB_VALUE.equals(connector)) {
                return extractNewDocumentState.apply(record);
            } else {
                return this.extractNewRecordState.apply(record);
            }
        }
        return record;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    private void tryInitConverter() {
        if (keyConverter == null) {
            synchronized (this) {
                if (keyConverter == null) {
                    keyConverter = new JsonConverter();
                    keyConverter.configure(
                            Collections.singletonMap(
                                    JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, keySchemaEnable),
                            true);
                    keyConverterMethod =
                            ReflectionUtils.getDeclaredMethod(
                                            JsonConverter.class,
                                            keySchemaEnable
                                                    ? INCLUDE_SCHEMA_METHOD
                                                    : EXCLUDE_SCHEMA_METHOD,
                                            Schema.class,
                                            Object.class)
                                    .get();
                }
            }
        }
        if (valueConverter == null) {
            synchronized (this) {
                if (valueConverter == null) {
                    valueConverter = new JsonConverter();
                    valueConverter.configure(
                            Collections.singletonMap(
                                    JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, valueSchemaEnable),
                            false);
                    valueConverterMethod =
                            ReflectionUtils.getDeclaredMethod(
                                            JsonConverter.class,
                                            valueSchemaEnable
                                                    ? INCLUDE_SCHEMA_METHOD
                                                    : EXCLUDE_SCHEMA_METHOD,
                                            Schema.class,
                                            Object.class)
                                    .get();
                }
            }
        }
    }

    private void tryInitTransforms() {
        Map<String, Object> transformsConfiguration = new HashMap<>();
        transformsConfiguration.put(
                ExtractNewRecordStateConfigDefinition.ADD_HEADERS.name(), "op,source.connector");
        transformsConfiguration.put(
                ExtractNewRecordStateConfigDefinition.HANDLE_DELETES.name(),
                ExtractNewRecordStateConfigDefinition.DeleteHandling.REWRITE);
        if (extractNewRecordState == null) {
            synchronized (this) {
                this.extractNewRecordState = new ExtractNewRecordState<>();
                this.extractNewRecordState.configure(transformsConfiguration);
            }
        }

        if (extractNewDocumentState == null) {
            synchronized (this) {
                this.extractNewDocumentState = new ExtractNewDocumentState<>();
                this.extractNewDocumentState.configure(transformsConfiguration);
            }
        }
    }
}
