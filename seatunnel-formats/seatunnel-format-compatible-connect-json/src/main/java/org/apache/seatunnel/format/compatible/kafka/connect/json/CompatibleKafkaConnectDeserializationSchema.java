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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** Compatible kafka connect deserialization schema */
@RequiredArgsConstructor
public class CompatibleKafkaConnectDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {

    private static final String INCLUDE_SCHEMA_METHOD = "convertToJsonWithEnvelope";
    private static final String EXCLUDE_SCHEMA_METHOD = "convertToJsonWithoutEnvelope";
    private static final String KAFKA_CONNECT_SINK_RECORD_PAYLOAD = "payload";
    public static final String FORMAT = "Kafka.Connect";
    private transient JsonConverter keyConverter;
    private transient JsonConverter valueConverter;
    private transient Method keyConverterMethod;
    private transient Method valueConverterMethod;
    private final SeaTunnelRowType seaTunnelRowType;
    private final JsonToRowConverters.JsonToObjectConverter runtimeConverter;
    private final boolean keySchemaEnable;
    private final boolean valueSchemaEnable;
    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CatalogTable catalogTable;

    public CompatibleKafkaConnectDeserializationSchema(
            @NonNull CatalogTable catalogTable,
            boolean keySchemaEnable,
            boolean valueSchemaEnable,
            boolean failOnMissingField,
            boolean ignoreParseErrors) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.keySchemaEnable = keySchemaEnable;
        this.valueSchemaEnable = valueSchemaEnable;
        // Runtime converter
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(seaTunnelRowType));
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
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
        if (msg == null) {
            return;
        }
        SinkRecord record = convertToSinkRecord(msg);
        RowKind rowKind = RowKind.INSERT;
        JsonNode jsonNode =
                (JsonNode)
                        valueConverterMethod.invoke(
                                valueConverter, record.valueSchema(), record.value());
        JsonNode payload = jsonNode.get(KAFKA_CONNECT_SINK_RECORD_PAYLOAD);
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (payload.isArray()) {
            ArrayNode arrayNode = (ArrayNode) payload;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow row = convertJsonNode(arrayNode.get(i));
                row.setRowKind(rowKind);
                if (tablePath.isPresent()) {
                    row.setTableId(tablePath.toString());
                }
                out.collect(row);
            }
        } else {
            SeaTunnelRow row = convertJsonNode(payload);
            row.setRowKind(rowKind);
            if (tablePath.isPresent()) {
                row.setTableId(tablePath.toString());
            }
            out.collect(row);
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return null;
        }

        try {
            org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode jsonData =
                    JsonUtils.stringToJsonNode(jsonNode.toString());
            return (SeaTunnelRow) runtimeConverter.convert(jsonData, null);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), t);
        }
    }

    private SinkRecord convertToSinkRecord(ConsumerRecord<byte[], byte[]> msg) {
        SchemaAndValue keyAndSchema =
                (msg.key() == null)
                        ? SchemaAndValue.NULL
                        : keyConverter.toConnectData(msg.topic(), msg.headers(), msg.key());
        SchemaAndValue valueAndSchema =
                valueConverter.toConnectData(msg.topic(), msg.headers(), msg.value());
        return new SinkRecord(
                msg.topic(),
                msg.partition(),
                keyAndSchema.schema(),
                keyAndSchema.value(),
                valueAndSchema.schema(),
                valueAndSchema.value(),
                msg.offset(),
                msg.timestamp(),
                msg.timestampType(),
                null);
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
}
