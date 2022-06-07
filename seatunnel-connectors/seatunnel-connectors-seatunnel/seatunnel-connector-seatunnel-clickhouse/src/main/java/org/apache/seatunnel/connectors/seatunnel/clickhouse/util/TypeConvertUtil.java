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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class TypeConvertUtil {

    public static SeaTunnelDataType<?> convert(ClickHouseDataType dataType) {
        Class<?> type = dataType.getObjectClass();
        if (Integer.class.equals(type)) {
            return BasicType.INTEGER;
        } else if (Long.class.equals(type)) {
            return BasicType.LONG;
        } else if (Short.class.equals(type)) {
            return BasicType.SHORT;
        } else if (BigInteger.class.equals(type)) {
            return BasicType.BIG_INTEGER;
        } else if (Byte.class.equals(type)) {
            return BasicType.BYTE;
        } else if (Boolean.class.equals(type)) {
            return BasicType.BOOLEAN;
        } else if (LocalDate.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE;
        } else if (LocalDateTime.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE_TIME;
        } else if (BigDecimal.class.equals(type)) {
            return BasicType.BIG_DECIMAL;
        } else if (String.class.equals(type)) {
            return BasicType.STRING;
        } else if (Float.class.equals(type)) {
            return BasicType.FLOAT;
        } else if (Double.class.equals(type)) {
            return BasicType.DOUBLE;
        } else if (Map.class.equals(type)) {
            return new MapType<>(BasicType.STRING, BasicType.STRING);
        } else if (List.class.equals(type)) {
            return new ListType<>(BasicType.STRING);
        } else {
            // TODO support pojo
            throw new IllegalArgumentException("not supported data type: " + dataType);
        }
    }

    public static Object valueUnwrap(SeaTunnelDataType<?> dataType, ClickHouseValue record) {

        if (dataType.equals(BasicType.BIG_DECIMAL)) {
            return record.asBigDecimal();
        } else if (dataType.equals(BasicType.BIG_INTEGER)) {
            return record.asBigInteger();
        } else if (dataType.equals(BasicType.BOOLEAN)) {
            return record.asBoolean();
        } else if (dataType.equals(BasicType.INTEGER)) {
            return record.asInteger();
        } else if (dataType.equals(BasicType.LONG)) {
            return record.asLong();
        } else if (dataType.equals(BasicType.SHORT)) {
            return record.asShort();
        } else if (dataType.equals(BasicType.BYTE)) {
            return record.asByte();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE)) {
            return record.asDate();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME)) {
            return record.asDateTime();
        } else if (dataType.equals(BasicType.STRING)) {
            return record.asString();
        } else if (dataType.equals(BasicType.FLOAT)) {
            return record.asFloat();
        } else if (dataType.equals(BasicType.DOUBLE)) {
            return record.asDouble();
        } else if (dataType instanceof MapType) {
            return record.asMap();
        } else if (dataType instanceof ListType) {
            return record.asTuple();
        } else {
            // TODO support pojo
            throw new IllegalArgumentException("not supported data type: " + dataType);
        }
    }

}
