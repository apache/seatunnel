/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeDataType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.DataFormatType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AerospikeSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ReadonlyConfig config;
    private final SerializationSchema serializationSchema;
    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final AerospikeTypeConverter typeConverter;

    public AerospikeSinkWriter(SeaTunnelRowType seaTunnelRowType, ReadonlyConfig config) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.config = config;
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.aerospikeClient = buildClient();

        this.writePolicy = new WritePolicy();
        this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        this.writePolicy.totalTimeout = config.get(AerospikeSinkOptions.WRITE_TIMEOUT);
        this.writePolicy.socketTimeout = config.get(AerospikeSinkOptions.WRITE_TIMEOUT);
        this.writePolicy.sleepBetweenRetries = 0;
        this.writePolicy.maxRetries = 0;
        this.typeConverter = new AerospikeTypeConverter(seaTunnelRowType, config);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            String data = new String(serializationSchema.serialize(element));
            String keyField = config.get(AerospikeSinkOptions.KEY_FIELD);
            String key = element.getField(seaTunnelRowType.indexOf(keyField)).toString();

            Key aerospikeKey =
                    new Key(
                            config.get(AerospikeSinkOptions.NAMESPACE),
                            config.get(AerospikeSinkOptions.SET),
                            key);

            String formatValue = config.get(AerospikeSinkOptions.DATA_FORMAT).toLowerCase();
            DataFormatType formatType = DataFormatType.fromString(formatValue);

            switch (formatType) {
                case MAP:
                    Map<String, Object> dataMap =
                            JSON.parseObject(data, new TypeReference<Map<String, Object>>() {});
                    Map<String, Object> filteredMap = new HashMap<>();
                    for (String fieldName : typeConverter.getFieldNames()) {
                        filteredMap.put(fieldName, dataMap.get(fieldName));
                    }
                    Map<String, Object> convertedMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : filteredMap.entrySet()) {
                        String fieldName = entry.getKey();
                        Object value = entry.getValue();
                        AerospikeDataType dataType = typeConverter.getFieldType(fieldName);
                        convertedMap.put(fieldName, convertValue(value, dataType));
                    }
                    Bin dataBin = new Bin(config.get(AerospikeSinkOptions.BIN_NAME), convertedMap);
                    aerospikeClient.put(writePolicy, aerospikeKey, dataBin);
                    break;

                case STRING:
                    Map<String, Object> filteredDataMap = new HashMap<>();
                    for (String fieldName : typeConverter.getFieldNames()) {
                        int index = seaTunnelRowType.indexOf(fieldName);
                        filteredDataMap.put(fieldName, element.getField(index));
                    }
                    String filteredData = JSON.toJSONString(filteredDataMap);
                    Bin stringBin =
                            new Bin(config.get(AerospikeSinkOptions.BIN_NAME), filteredData);
                    aerospikeClient.put(writePolicy, aerospikeKey, stringBin);
                    break;

                case KV:
                    Map<String, Object> fieldsMap =
                            JSON.parseObject(data, new TypeReference<Map<String, Object>>() {});
                    List<Bin> bins = new ArrayList<>();
                    Map<String, String> configFieldTypes =
                            config.get(AerospikeSinkOptions.FIELD_TYPES);
                    for (String fieldName : configFieldTypes.keySet()) {
                        Object value = fieldsMap.get(fieldName);
                        AerospikeDataType dataType = typeConverter.getFieldType(fieldName);
                        Object convertedValue = convertValue(value, dataType);
                        bins.add(new Bin(fieldName, convertedValue));
                    }
                    aerospikeClient.put(writePolicy, aerospikeKey, bins.toArray(new Bin[0]));
                    break;

                default:
                    throw new IllegalArgumentException(
                            "Unsupported data format type: " + formatType);
            }
        } catch (Exception e) {
            throw new AerospikeConnectorException(
                    AerospikeErrorCode.WRITER_OPERATION_FAILED, "Failed to write record", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (Objects.nonNull(aerospikeClient)) {
                aerospikeClient.close();
            }
        } catch (Exception e) {
            throw new AerospikeConnectorException(
                    AerospikeErrorCode.WRITER_CLOSE_FAILED, "Failed to close writer", e);
        }
    }

    private AerospikeClient buildClient() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = config.get(AerospikeSinkOptions.USERNAME);
        clientPolicy.password = config.get(AerospikeSinkOptions.PASSWORD);
        clientPolicy.timeout = config.get(AerospikeSinkOptions.WRITE_TIMEOUT);
        clientPolicy.maxConnsPerNode = 300;

        return new AerospikeClient(
                clientPolicy,
                config.get(AerospikeSinkOptions.HOST),
                config.get(AerospikeSinkOptions.PORT));
    }

    private Object convertValue(Object value, AerospikeDataType dataType) {
        if (value == null) {
            return null;
        }

        switch (dataType) {
            case STRING:
                return value.toString();
            case INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof TemporalAccessor) {
                    return convertTimestampToLong(value);
                } else if (value instanceof String) {
                    Optional<Long> timestamp = tryParseDateTime((String) value);
                    return timestamp.orElseGet(() -> Long.parseLong((String) value));
                } else {
                    return Long.parseLong(value.toString());
                }
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            case BYTEARRAY:
                if (value.getClass().isArray()) {
                    return value;
                }
                throw new IllegalArgumentException(
                        "Expected Array type but got: " + value.getClass());
            case LIST:
                if (value instanceof Iterable) {
                    return value;
                }
                throw new IllegalArgumentException(
                        "Expected List type but got: " + value.getClass());
            default:
                throw new IllegalArgumentException("Unsupported AEROSPIKE data type: " + dataType);
        }
    }

    private long parseDateTimeString(String datetime) {
        try {
            return LocalDateTime.parse(datetime)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            try {
                return Instant.parse(datetime).toEpochMilli();
            } catch (DateTimeParseException ex) {
                throw new IllegalArgumentException("Unsupported datetime format: " + datetime);
            }
        }
    }

    private Optional<Long> tryParseDateTime(String datetime) {
        try {
            return Optional.of(parseDateTimeString(datetime));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private long convertTimestampToLong(Object timestamp) {
        if (timestamp instanceof TemporalAccessor) {
            Instant instant = Instant.from((TemporalAccessor) timestamp);
            return instant.toEpochMilli();
        }
        throw new IllegalArgumentException("Unsupported timestamp type: " + timestamp.getClass());
    }
}
