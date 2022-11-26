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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.text.exception.SeaTunnelTextFormatException;

import lombok.Builder;
import lombok.NonNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Builder
public class TextSerializationSchema implements SerializationSchema {
    @NonNull
    private SeaTunnelRowType seaTunnelRowType;
    @NonNull
    private String delimiter;
    @Builder.Default
    private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
    @Builder.Default
    private DateTimeUtils.Formatter dateTimeFormatter = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    @Builder.Default
    private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        if (element.getFields().length != seaTunnelRowType.getTotalFields()) {
            throw new IndexOutOfBoundsException("The data does not match the configured schema information, please check");
        }
        Object[] fields = element.getFields();
        String[] strings = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            strings[i] = convert(fields[i], seaTunnelRowType.getFieldType(i));
        }
        return String.join(delimiter, strings).getBytes();
    }

    private String convert(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        switch (fieldType.getSqlType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case DECIMAL:
                return field.toString();
            case DATE:
                return DateUtils.toString((LocalDate) field, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) field, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormatter);
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field);
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(field);
            case ROW:
                Object[] fields = ((SeaTunnelRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] = convert(fields[i], ((SeaTunnelRowType) fieldType).getFieldType(i));
                }
                return String.join(delimiter, strings);
            default:
                throw new SeaTunnelTextFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("SeaTunnel format text not supported for parsing this type [%s]", fieldType.getSqlType()));
        }
    }
}
