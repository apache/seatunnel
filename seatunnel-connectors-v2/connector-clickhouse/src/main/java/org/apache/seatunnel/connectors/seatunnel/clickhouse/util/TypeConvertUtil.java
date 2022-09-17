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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseValue;

import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

public class TypeConvertUtil {

    public static SeaTunnelDataType<?> convert(ClickHouseColumn column) {
        Class<?> type = column.getDataType().getObjectClass();
        if (Integer.class.equals(type)) {
            return BasicType.INT_TYPE;
        } else if (Long.class.equals(type)) {
            return BasicType.LONG_TYPE;
        } else if (Short.class.equals(type)) {
            return BasicType.SHORT_TYPE;
        } else if (Byte.class.equals(type)) {
            return BasicType.BYTE_TYPE;
        } else if (Boolean.class.equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        } else if (LocalDate.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        } else if (LocalDateTime.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if (BigDecimal.class.equals(type)) {
            return new DecimalType(column.getPrecision(), column.getScale());
        } else if (String.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Float.class.equals(type)) {
            return BasicType.FLOAT_TYPE;
        } else if (Double.class.equals(type)) {
            return BasicType.DOUBLE_TYPE;
        } else if (Map.class.equals(type)) {
            return new MapType<>(convert(column.getNestedColumns().get(0)), convert(column.getNestedColumns().get(1)));
        } else if (UUID.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Inet4Address.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Inet6Address.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Object.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else {
            // TODO support pojo
            throw new IllegalArgumentException("not supported data type: " + column.getDataType());
        }
    }

    public static Object valueUnwrap(SeaTunnelDataType<?> dataType, ClickHouseValue record) {

        if (dataType instanceof DecimalType) {
            return record.asBigDecimal();
        } else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return record.asBoolean();
        } else if (dataType.equals(BasicType.INT_TYPE)) {
            return record.asInteger();
        } else if (dataType.equals(BasicType.LONG_TYPE)) {
            return record.asLong();
        } else if (dataType.equals(BasicType.SHORT_TYPE)) {
            return record.asShort();
        } else if (dataType.equals(BasicType.BYTE_TYPE)) {
            return record.asByte();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return record.asDate();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return record.asDateTime();
        } else if (dataType.equals(BasicType.STRING_TYPE)) {
            return record.asString();
        } else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return record.asFloat();
        } else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return record.asDouble();
        } else if (dataType instanceof MapType) {
            return record.asMap();
        } else if (dataType instanceof ArrayType) {
            return record.asObject();
        } else {
            // TODO support pojo
            throw new IllegalArgumentException("not supported data type: " + dataType);
        }
    }

}
