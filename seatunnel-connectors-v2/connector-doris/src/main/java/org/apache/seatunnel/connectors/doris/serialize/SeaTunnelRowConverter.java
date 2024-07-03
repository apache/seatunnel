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

package org.apache.seatunnel.connectors.doris.serialize;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class SeaTunnelRowConverter {
    @Builder.Default private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;

    @Builder.Default
    private DateTimeUtils.Formatter dateTimeFormatter =
            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS;

    @Builder.Default private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    protected Object convert(SeaTunnelDataType dataType, Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case STRING:
                return val;
            case DATE:
                return DateUtils.toString((LocalDate) val, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) val, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) val, dateTimeFormatter);
            case ARRAY:
                return convertArray(dataType, val);
            case MAP:
                return convertMap(dataType, val);
            case BYTES:
                return new String((byte[]) val);
            default:
                throw new DorisConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        dataType + " is not supported ");
        }
    }

    public Object[] convertArray(SeaTunnelDataType dataType, Object val) {
        if (dataType instanceof DecimalArrayType) {
            return (BigDecimal[]) val;
        }

        SeaTunnelDataType elementType = ((ArrayType) dataType).getElementType();
        Object[] realValue = (Object[]) val;
        Object[] newArrayValue = new Object[realValue.length];
        for (int i = 0; i < realValue.length; i++) {
            newArrayValue[i] = convert(elementType, realValue[i]);
        }
        return newArrayValue;
    }

    public Map<Object, Object> convertMap(SeaTunnelDataType dataType, Object val) {
        MapType valueMapType = (MapType) dataType;
        Map<Object, Object> realValue = (Map<Object, Object>) val;
        Map<Object, Object> newMapValue = new LinkedHashMap<>();
        for (Map.Entry entry : realValue.entrySet()) {
            newMapValue.put(
                    convert(valueMapType.getKeyType(), entry.getKey()),
                    convert(valueMapType.getValueType(), entry.getValue()));
        }
        return newMapValue;
    }
}
