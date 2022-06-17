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

package org.apache.seatunnel.connectors.seatunnel.mongodb.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class RawTypeConverter {

    public static SeaTunnelDataType<?> convert(String dataType) {
        if ("Integer".equalsIgnoreCase(dataType)) {
            return BasicType.INT_TYPE;
        } else if ("Long".equalsIgnoreCase(dataType)) {
            return BasicType.LONG_TYPE;
        } else if ("Short".equalsIgnoreCase(dataType)) {
            return BasicType.SHORT_TYPE;
        } else if ("BigInteger".equalsIgnoreCase(dataType)) {
            return BasicType.BIG_INT_TYPE;
        } else if ("Byte".equalsIgnoreCase(dataType)) {
            return BasicType.BYTE_TYPE;
        } else if ("Boolean".equalsIgnoreCase(dataType)) {
            return BasicType.BOOLEAN_TYPE;
        } else if ("LocalDate".equalsIgnoreCase(dataType)) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        } else if ("LocalDateTime".equalsIgnoreCase(dataType)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if ("BigDecimal".equalsIgnoreCase(dataType)) {
            return BasicType.BIG_DECIMAL_TYPE;
        } else if ("String".equalsIgnoreCase(dataType)) {
            return BasicType.STRING_TYPE;
        } else if ("Float".equalsIgnoreCase(dataType)) {
            return BasicType.FLOAT_TYPE;
        } else if ("Double".equalsIgnoreCase(dataType)) {
            return BasicType.DOUBLE_TYPE;
        } else if ("Map".equalsIgnoreCase(dataType)) {
            return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        } else if ("List".equalsIgnoreCase(dataType)) {
            return new ListType<>(BasicType.STRING_TYPE);
        } else {
            throw new IllegalArgumentException("not supported data type: " + dataType);
        }
    }

}
