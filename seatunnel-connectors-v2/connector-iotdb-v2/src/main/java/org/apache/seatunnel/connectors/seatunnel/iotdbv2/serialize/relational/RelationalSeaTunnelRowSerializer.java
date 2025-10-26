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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.SeaTunnelRowSerializer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import shaded.org.apache.tsfile.enums.TSDataType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RelationalSeaTunnelRowSerializer
        implements SeaTunnelRowSerializer<IoTDBv2RelationalRecord> {

    private final Function<SeaTunnelRow, String> tableNameExtractor;
    private final Function<SeaTunnelRow, Long> timestampExtractor;
    private final Function<SeaTunnelRow, List<String>> tagsExtractor;
    private final Function<SeaTunnelRow, List<String>> attributesExtractor;
    private final Function<SeaTunnelRow, List<Object>> fieldsExtractor;

    public RelationalSeaTunnelRowSerializer(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            @NonNull String database,
            @NonNull String tableNameKey,
            String timestampKey,
            List<String> tagKeys,
            List<String> attributeKeys,
            List<String> fieldNames,
            List<TSDataType> fieldTypes) {
        this.tableNameExtractor = createTableNameExtractor(seaTunnelRowType, tableNameKey);
        this.timestampExtractor = createTimestampExtractor(seaTunnelRowType, timestampKey);
        this.tagsExtractor = createTagAttributeExtractor(seaTunnelRowType, tagKeys);
        this.attributesExtractor = createTagAttributeExtractor(seaTunnelRowType, attributeKeys);
        this.fieldsExtractor = createFieldsExtractor(seaTunnelRowType, fieldNames, fieldTypes);
    }

    @Override
    public IoTDBv2RelationalRecord serialize(SeaTunnelRow seaTunnelRow) {
        String tableName = tableNameExtractor.apply(seaTunnelRow);
        Long timestamp = timestampExtractor.apply(seaTunnelRow);
        List<String> tags = tagsExtractor.apply(seaTunnelRow);
        List<String> attributes = attributesExtractor.apply(seaTunnelRow);
        List<Object> fields = fieldsExtractor.apply(seaTunnelRow);
        return new IoTDBv2RelationalRecord(tableName, timestamp, tags, attributes, fields);
    }

    private Function<SeaTunnelRow, String> createTableNameExtractor(
            SeaTunnelRowType seaTunnelRowType, String tableNameKey) {
        int tableNameIndex = seaTunnelRowType.indexOf(tableNameKey);
        return seaTunnelRow -> {
            return seaTunnelRow.getField(tableNameIndex).toString();
        };
    }

    private Function<SeaTunnelRow, Long> createTimestampExtractor(
            SeaTunnelRowType seaTunnelRowType, String timestampKey) {
        if (Strings.isNullOrEmpty(timestampKey)) {
            return row -> System.currentTimeMillis();
        }

        int timestampFieldIndex = seaTunnelRowType.indexOf(timestampKey);
        return row -> {
            Object timestamp = row.getField(timestampFieldIndex);
            if (timestamp == null) {
                return System.currentTimeMillis();
            }
            SeaTunnelDataType<?> timestampFieldType =
                    seaTunnelRowType.getFieldType(timestampFieldIndex);
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
                    throw new IotdbConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + timestampFieldType);
            }
        };
    }

    private Function<SeaTunnelRow, List<String>> createTagAttributeExtractor(
            SeaTunnelRowType seaTunnelRowType, List<String> keys) {
        List<Integer> indices = new ArrayList<>();
        for (String key : keys) {
            indices.add(seaTunnelRowType.indexOf(key));
        }
        return seaTunnelRow -> {
            List<String> res = new ArrayList<>();
            for (int index : indices) {
                res.add(seaTunnelRow.getField(index).toString());
            }
            return res;
        };
    }

    private Function<SeaTunnelRow, List<Object>> createFieldsExtractor(
            SeaTunnelRowType seaTunnelRowType,
            List<String> fieldList,
            List<TSDataType> fieldTypeList) {
        int fieldSize = fieldList.size();
        return row -> {
            List<Object> values = new ArrayList<>(fieldSize);
            for (int i = 0; i < fieldSize; i++) {
                String curField = fieldList.get(i);
                TSDataType curFieldType = fieldTypeList.get(i);

                int indexOfSeaTunnelRow = seaTunnelRowType.indexOf(curField);
                SeaTunnelDataType seaTunnelDataType =
                        seaTunnelRowType.getFieldType(indexOfSeaTunnelRow);
                Object seaTunnelFieldValue = row.getField(indexOfSeaTunnelRow);

                Object value = convert(seaTunnelDataType, curFieldType, seaTunnelFieldValue);
                values.add(value);
            }
            return values;
        };
    }

    private static Object convert(
            SeaTunnelDataType seaTunnelType, TSDataType tsDataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (tsDataType) {
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            case INT32:
                return ((Number) value).intValue();
            case INT64:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case TIMESTAMP:
                return ((LocalDateTime) value).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            case DATE:
            case TEXT:
            case STRING:
                return value.toString();
            default:
                throw new IotdbConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + tsDataType);
        }
    }
}
