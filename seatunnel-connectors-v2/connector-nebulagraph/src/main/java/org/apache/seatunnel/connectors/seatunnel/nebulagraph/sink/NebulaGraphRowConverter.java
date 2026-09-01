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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphWriteMode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import com.vesoft.nebula.Date;
import com.vesoft.nebula.DateTime;
import com.vesoft.nebula.Time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class NebulaGraphRowConverter {

    private final SeaTunnelRowType rowType;
    private final int vidIndex;
    private final List<String> propertyNames;
    private final List<Integer> propertyIndexes;

    NebulaGraphRowConverter(NebulaGraphSinkConfig config, SeaTunnelRowType rowType) {
        this.rowType = rowType;
        this.vidIndex = fieldIndex(rowType, config.getVidField(), "vid_field");
        validateVidType(rowType.getFieldType(vidIndex), config.getVidField());

        List<String> configuredFields = config.getWriteFields();
        if (configuredFields.isEmpty()) {
            configuredFields = new ArrayList<>(Arrays.asList(rowType.getFieldNames()));
            configuredFields.remove(config.getVidField());
        }
        if (config.getWriteMode() == NebulaGraphWriteMode.UPDATE && configuredFields.isEmpty()) {
            throw invalid("UPDATE mode requires at least one property field.");
        }

        this.propertyNames = new ArrayList<>(configuredFields.size());
        this.propertyIndexes = new ArrayList<>(configuredFields.size());
        for (String field : configuredFields) {
            int index = fieldIndex(rowType, field, "write_fields");
            validatePropertyType(rowType.getFieldType(index), field);
            propertyNames.add(field);
            propertyIndexes.add(index);
        }
    }

    NebulaGraphVertex convert(SeaTunnelRow row) {
        if (row.getArity() != rowType.getTotalFields()) {
            throw invalid(
                    "Input row arity "
                            + row.getArity()
                            + " does not match the configured schema arity "
                            + rowType.getTotalFields()
                            + ".");
        }
        Object vid = convertVid(row.getField(vidIndex));
        Map<String, Object> properties = new LinkedHashMap<>();
        for (int i = 0; i < propertyNames.size(); i++) {
            int index = propertyIndexes.get(i);
            properties.put(
                    propertyNames.get(i),
                    convertProperty(rowType.getFieldType(index), row.getField(index)));
        }
        return new NebulaGraphVertex(vid, properties);
    }

    List<String> getPropertyNames() {
        return new ArrayList<>(propertyNames);
    }

    private Object convertVid(Object value) {
        if (value == null) {
            throw invalid("The vertex ID field must not be null.");
        }
        SqlType sqlType = rowType.getFieldType(vidIndex).getSqlType();
        if (sqlType == SqlType.STRING) {
            return (String) value;
        }
        return ((Number) value).longValue();
    }

    private static Object convertProperty(SeaTunnelDataType<?> dataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
            case BOOLEAN:
            case BYTES:
                return value;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
            case DOUBLE:
                return ((Number) value).doubleValue();
            case DATE:
                LocalDate date = (LocalDate) value;
                return new Date(
                        (short) date.getYear(),
                        (byte) date.getMonthValue(),
                        (byte) date.getDayOfMonth());
            case TIME:
                LocalTime time = (LocalTime) value;
                return new Time(
                        (byte) time.getHour(),
                        (byte) time.getMinute(),
                        (byte) time.getSecond(),
                        time.getNano() / 1000);
            case TIMESTAMP:
                LocalDateTime timestamp = (LocalDateTime) value;
                return new DateTime(
                        (short) timestamp.getYear(),
                        (byte) timestamp.getMonthValue(),
                        (byte) timestamp.getDayOfMonth(),
                        (byte) timestamp.getHour(),
                        (byte) timestamp.getMinute(),
                        (byte) timestamp.getSecond(),
                        timestamp.getNano() / 1000);
            default:
                throw unsupported(dataType.getSqlType(), "property");
        }
    }

    private static int fieldIndex(SeaTunnelRowType rowType, String field, String option) {
        int index = rowType.indexOf(field, false);
        if (index < 0) {
            throw invalid(
                    "Option '"
                            + option
                            + "' references unknown field '"
                            + field
                            + "'. Available fields are "
                            + Arrays.toString(rowType.getFieldNames())
                            + ".");
        }
        return index;
    }

    private static void validateVidType(SeaTunnelDataType<?> dataType, String field) {
        SqlType type = dataType.getSqlType();
        if (type != SqlType.STRING
                && type != SqlType.TINYINT
                && type != SqlType.SMALLINT
                && type != SqlType.INT
                && type != SqlType.BIGINT) {
            throw unsupported(type, "vertex ID field '" + field + "'");
        }
    }

    private static void validatePropertyType(SeaTunnelDataType<?> dataType, String field) {
        switch (dataType.getSqlType()) {
            case STRING:
            case BOOLEAN:
            case BYTES:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return;
            default:
                throw unsupported(dataType.getSqlType(), "property field '" + field + "'");
        }
    }

    private static NebulaGraphConnectorException invalid(String message) {
        return new NebulaGraphConnectorException(
                NebulaGraphConnectorErrorCode.INVALID_CONFIG, message);
    }

    private static NebulaGraphConnectorException unsupported(SqlType type, String context) {
        return new NebulaGraphConnectorException(
                NebulaGraphConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                "SeaTunnel type " + type + " is not supported for NebulaGraph " + context + ".");
    }
}
