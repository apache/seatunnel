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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import java.time.LocalDateTime;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.math.BigDecimal;

public class FakeData {

    public static final String[] COLUMN_NAME = new String[]{
        "c_void",
        "c_boolean",
        "c_byte",
        "c_short",
        "c_int",
        "c_long",
        "c_float",
        "c_double",
        "c_string",
        "c_decimal",
        "c_local_date_time",
        "c_local_date",
        "c_local_time",
        "c_byte_arr"
    };
    public static final SeaTunnelDataType<?>[] COLUMN_TYPE = new SeaTunnelDataType[]{
        BasicType.VOID_TYPE,
        BasicType.BOOLEAN_TYPE,
        BasicType.BYTE_TYPE,
        BasicType.SHORT_TYPE,
        BasicType.INT_TYPE,
        BasicType.LONG_TYPE,
        BasicType.FLOAT_TYPE,
        BasicType.DOUBLE_TYPE,
        BasicType.STRING_TYPE,
        new DecimalType(38, 18),
        LocalTimeType.LOCAL_DATE_TIME_TYPE,
        LocalTimeType.LOCAL_DATE_TYPE,
        LocalTimeType.LOCAL_TIME_TYPE,
        PrimitiveByteArrayType.INSTANCE
    };

    @SuppressWarnings("magicnumber")
    public static SeaTunnelRow generateRow() {
        Object[] columnValue = {
            Void.TYPE,
            RandomUtils.nextInt(0, 2) == 1,
            (byte) RandomUtils.nextInt(0, Byte.MAX_VALUE),
            (short) RandomUtils.nextInt(Byte.MAX_VALUE, Short.MAX_VALUE),
            RandomUtils.nextInt(Short.MAX_VALUE, Integer.MAX_VALUE),
            RandomUtils.nextLong(Integer.MAX_VALUE, Long.MAX_VALUE),
            RandomUtils.nextFloat(Float.MIN_VALUE, Float.MAX_VALUE),
            RandomUtils.nextDouble(Float.MAX_VALUE, Double.MAX_VALUE),
            RandomStringUtils.randomAlphabetic(10),
            BigDecimal.valueOf(RandomUtils.nextLong(Integer.MAX_VALUE,Long.MAX_VALUE),18),
            LocalDateTime.now(),
            LocalDateTime.now().toLocalDate(),
            LocalDateTime.now().toLocalTime(),
            RandomStringUtils.randomAlphabetic(10).getBytes(),
        };
        if (columnValue.length != columnValue.length || columnValue.length != COLUMN_TYPE.length) {
            throw new RuntimeException("the row data should be equals to column");
        }
        return new SeaTunnelRow(columnValue);
    }

    public static void main(String[] args) {
        System.out.println(BigDecimal.valueOf(RandomUtils.nextDouble(0, Long.MAX_VALUE)));
    }
}
