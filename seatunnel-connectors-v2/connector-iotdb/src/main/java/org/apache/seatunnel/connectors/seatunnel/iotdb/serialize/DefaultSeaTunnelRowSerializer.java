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

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.TimeseriesOption;
import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private static final String FIELD_DEVICE = "device";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_MEASUREMENTS = "measurements";
    private static final String FIELD_TYPES = "types";
    private static final String FIELD_VALUES = "values";
    private static final String SEPARATOR = ",";

    private final SeaTunnelRowType seaTunnelRowType;
    private final Map<String, TimeseriesOption> timeseriesOptionMap;
    private final Function<SeaTunnelRow, String> deviceExtractor;
    private final Function<SeaTunnelRow, Long> timestampExtractor;
    private final Function<SeaTunnelRow, List<String>> measurementsExtractor;
    private final Function<SeaTunnelRow, List<TSDataType>> typesExtractor;

    public DefaultSeaTunnelRowSerializer(SeaTunnelRowType seaTunnelRowType,
                                         List<TimeseriesOption> timeseriesOptions) {
        validateRowTypeSchema(seaTunnelRowType);

        this.seaTunnelRowType = seaTunnelRowType;
        this.timeseriesOptionMap = Optional.ofNullable(timeseriesOptions)
                .orElse(Collections.emptyList()).stream()
                .collect(Collectors.toMap(option -> option.getPath(), option -> option));

        final List<String> rowTypeFields = Arrays.asList(seaTunnelRowType.getFieldNames());
        final int deviceIndex = seaTunnelRowType.indexOf(FIELD_DEVICE);
        this.deviceExtractor = seaTunnelRow -> seaTunnelRow.getField(deviceIndex).toString();
        final int timestampIndex = seaTunnelRowType.indexOf(FIELD_TIMESTAMP);
        this.timestampExtractor = rowTypeFields.contains(FIELD_TIMESTAMP) ?
            seaTunnelRow -> Long.parseLong(seaTunnelRow.getField(timestampIndex).toString()) :
            seaTunnelRow -> System.currentTimeMillis();
        final int measurementsIndex = seaTunnelRowType.indexOf(FIELD_MEASUREMENTS);
        this.measurementsExtractor = seaTunnelRow ->
                Arrays.asList(seaTunnelRow.getField(measurementsIndex).toString().split(SEPARATOR));
        final boolean containsTypesField = rowTypeFields.contains(FIELD_TYPES);
        final int typesIndex = containsTypesField ? seaTunnelRowType.indexOf(FIELD_TYPES) : -1;
        this.typesExtractor = seaTunnelRow -> {
            if (!containsTypesField) {
                return null;
            }
            return Arrays.stream(seaTunnelRow.getField(typesIndex).toString().split(SEPARATOR))
                    .map(type -> TSDataType.valueOf(type))
                    .collect(Collectors.toList());
        };
    }

    @Override
    public IoTDBRecord serialize(SeaTunnelRow seaTunnelRow) {
        String device = deviceExtractor.apply(seaTunnelRow);
        Long timestamp = timestampExtractor.apply(seaTunnelRow);
        List<String> measurements = measurementsExtractor.apply(seaTunnelRow);
        List<TSDataType> types = typesExtractor.apply(seaTunnelRow);
        List<Object> values = extractValues(device, measurements, types, seaTunnelRow);
        return new IoTDBRecord(device, timestamp, measurements, types, values);
    }

    private void validateRowTypeSchema(SeaTunnelRowType seaTunnelRowType) throws IllegalArgumentException {
        List<String> rowTypeFields = Lists.newArrayList(seaTunnelRowType.getFieldNames());
        checkArgument(rowTypeFields.contains(FIELD_DEVICE));
        checkArgument(rowTypeFields.contains(FIELD_MEASUREMENTS));
        checkArgument(rowTypeFields.contains(FIELD_VALUES));

        rowTypeFields.remove(FIELD_DEVICE);
        rowTypeFields.remove(FIELD_TIMESTAMP);
        rowTypeFields.remove(FIELD_MEASUREMENTS);
        rowTypeFields.remove(FIELD_TYPES);
        rowTypeFields.remove(FIELD_VALUES);
        checkArgument(rowTypeFields.isEmpty(),
                "Illegal SeaTunnelRowType fields: " + rowTypeFields);
    }

    private List<Object> extractValues(String device,
                                       List<String> measurements,
                                       List<TSDataType> tsDataTypes,
                                       SeaTunnelRow seaTunnelRow) {
        int valuesIndex = seaTunnelRowType.indexOf(FIELD_VALUES);
        String[] valuesStr = StringUtils.trim(
                seaTunnelRow.getField(valuesIndex).toString()).split(SEPARATOR);
        if (tsDataTypes == null || tsDataTypes.isEmpty()) {
            convertTextValues(device, measurements, valuesStr);
            return Arrays.asList(valuesStr);
        }

        List<Object> values = new ArrayList<>();
        for (int i = 0; i < valuesStr.length; i++) {
            TSDataType tsDataType = tsDataTypes.get(i);
            switch (tsDataType) {
                case INT32:
                    values.add(Integer.valueOf(valuesStr[i]));
                    break;
                case INT64:
                    values.add(Long.valueOf(valuesStr[i]));
                    break;
                case FLOAT:
                    values.add(Float.valueOf(valuesStr[i]));
                    break;
                case DOUBLE:
                    values.add(Double.valueOf(valuesStr[i]));
                    break;
                case BOOLEAN:
                    values.add(Boolean.valueOf(valuesStr[i]));
                    break;
                case TEXT:
                    String value = valuesStr[i];
                    if (!value.startsWith("\"") && !value.startsWith("'")) {
                        value = convertToTextValue(value);
                    }
                    values.add(value);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType: " + tsDataType);
            }
        }
        return values;
    }

    private void convertTextValues(String device, List<String> measurements, String[] values) {
        if (device != null
                && measurements != null
                && values != null
                && !timeseriesOptionMap.isEmpty()
                && measurements.size() == values.length) {
            for (int i = 0; i < measurements.size(); i++) {
                String measurement = device + TsFileConstant.PATH_SEPARATOR + measurements.get(i);
                TimeseriesOption timeseriesOption = timeseriesOptionMap.get(measurement);
                if (timeseriesOption != null && TSDataType.TEXT.equals(timeseriesOption.getDataType())) {
                    // The TEXT data type should be covered by " or '
                    values[i] = convertToTextValue(values[i]);
                }
            }
        }
    }

    private String convertToTextValue(Object value) {
        return "'" + value + "'";
    }
}
