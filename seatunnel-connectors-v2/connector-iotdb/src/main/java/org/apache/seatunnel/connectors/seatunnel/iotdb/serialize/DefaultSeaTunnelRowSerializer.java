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

package org.apache.seatunnel.connectors.seatunnel.iotdb.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;

import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private final Function<SeaTunnelRow, Long> timestampExtractor;
    private final Function<SeaTunnelRow, String> deviceExtractor;
    private final Function<SeaTunnelRow, List<Object>> valuesExtractor;
    private final List<String> measurements;
    private final List<TSDataType> measurementsType;

    public DefaultSeaTunnelRowSerializer(@NonNull SeaTunnelRowType seaTunnelRowType,
                                         String storageGroup,
                                         String timestampKey,
                                         @NonNull String deviceKey,
                                         List<String> measurementKeys) {
        this.timestampExtractor = createTimestampExtractor(seaTunnelRowType, timestampKey);
        this.deviceExtractor = createDeviceExtractor(seaTunnelRowType, deviceKey, storageGroup);
        this.measurements = createMeasurements(seaTunnelRowType, timestampKey, deviceKey, measurementKeys);
        this.measurementsType = createMeasurementTypes(seaTunnelRowType, measurements);
        this.valuesExtractor = createValuesExtractor(seaTunnelRowType, measurements, measurementsType);
    }

    @Override
    public IoTDBRecord serialize(SeaTunnelRow seaTunnelRow) {
        Long timestamp = timestampExtractor.apply(seaTunnelRow);
        String device = deviceExtractor.apply(seaTunnelRow);
        List<Object> values = valuesExtractor.apply(seaTunnelRow);
        return new IoTDBRecord(device, timestamp, measurements, measurementsType, values);
    }

    private Function<SeaTunnelRow, Long> createTimestampExtractor(SeaTunnelRowType seaTunnelRowType,
                                                                  String timestampKey) {
        if (Strings.isNullOrEmpty(timestampKey)) {
            return row -> System.currentTimeMillis();
        }

        int timestampFieldIndex = seaTunnelRowType.indexOf(timestampKey);
        return row -> {
            Object timestamp = row.getField(timestampFieldIndex);
            if (timestamp == null) {
                return System.currentTimeMillis();
            }
            SeaTunnelDataType<?> timestampFieldType = seaTunnelRowType.getFieldType(timestampFieldIndex);
            switch (timestampFieldType.getSqlType()) {
                case STRING:
                    return Long.parseLong((String) timestamp);
                case TIMESTAMP:
                    return ((LocalDateTime) timestamp)
                        .atZone(ZoneOffset.UTC)
                        .toInstant()
                        .toEpochMilli();
                case BIGINT:
                    return (Long) timestamp;
                default:
                    throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + timestampFieldType);
            }
        };
    }

    private Function<SeaTunnelRow, String> createDeviceExtractor(SeaTunnelRowType seaTunnelRowType,
                                                                 String deviceKey,
                                                                 String storageGroup) {
        int deviceIndex = seaTunnelRowType.indexOf(deviceKey);
        return seaTunnelRow -> {
            String device = seaTunnelRow.getField(deviceIndex).toString();
            if (Strings.isNullOrEmpty(storageGroup)) {
                return device;
            }
            if (storageGroup.endsWith(".") || device.startsWith(".")) {
                return storageGroup + device;
            }
            return storageGroup + "." + device;
        };
    }

    private List<String> createMeasurements(SeaTunnelRowType seaTunnelRowType,
                                            String timestampKey,
                                            String deviceKey,
                                            List<String> measurementKeys) {
        if (measurementKeys == null || measurementKeys.isEmpty()) {
            return Stream.of(seaTunnelRowType.getFieldNames())
                .filter(name -> !name.equals(deviceKey))
                .filter(name -> !name.equals(timestampKey))
                .collect(Collectors.toList());
        }
        return measurementKeys;
    }

    private List<TSDataType> createMeasurementTypes(SeaTunnelRowType seaTunnelRowType,
                                                    List<String> measurements) {
        return measurements.stream()
            .map(measurement -> {
                int indexOfSeaTunnelRow = seaTunnelRowType.indexOf(measurement);
                SeaTunnelDataType<?> seaTunnelType = seaTunnelRowType.getFieldType(indexOfSeaTunnelRow);
                return convert(seaTunnelType);
            })
            .collect(Collectors.toList());
    }

    private Function<SeaTunnelRow, List<Object>> createValuesExtractor(SeaTunnelRowType seaTunnelRowType,
                                                                       List<String> measurements,
                                                                       List<TSDataType> measurementTypes) {
        return row -> {
            List<Object> measurementValues = new ArrayList<>(measurements.size());
            for (int i = 0; i < measurements.size(); i++) {
                String measurement = measurements.get(i);
                TSDataType measurementDataType = measurementsType.get(i);

                int indexOfSeaTunnelRow = seaTunnelRowType.indexOf(measurement);
                SeaTunnelDataType seaTunnelDataType = seaTunnelRowType.getFieldType(indexOfSeaTunnelRow);
                Object seaTunnelFieldValue = row.getField(indexOfSeaTunnelRow);

                Object measurementValue = convert(seaTunnelDataType, measurementDataType, seaTunnelFieldValue);
                measurementValues.add(measurementValue);
            }
            return measurementValues;
        };
    }

    private static TSDataType convert(SeaTunnelDataType dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return TSDataType.TEXT;
            case BOOLEAN:
                return TSDataType.BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INT:
                return TSDataType.INT32;
            case BIGINT:
                return TSDataType.INT64;
            case FLOAT:
                return TSDataType.FLOAT;
            case DOUBLE:
                return TSDataType.DOUBLE;
            default:
                throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + dataType);
        }
    }

    private static Object convert(SeaTunnelDataType seaTunnelType,
                                  TSDataType tsDataType,
                                  Object value) {
        if (value == null) {
            return null;
        }
        switch (tsDataType) {
            case INT32:
                return ((Number) value).intValue();
            case INT64:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return Boolean.valueOf((Boolean) value);
            case TEXT:
                return value.toString();
            default:
                throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + tsDataType);
        }
    }
}
