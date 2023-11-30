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

package org.apache.seatunnel.connectors.seatunnel.paimon.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RowTypeConverterTest {

    private SeaTunnelRowType seaTunnelRowType;

    private RowType rowType;

    @BeforeEach
    public void before() {
        seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "c_tinyint",
                            "c_smallint",
                            "c_int",
                            "c_bigint",
                            "c_float",
                            "c_double",
                            "c_decimal",
                            "c_string",
                            "c_bytes",
                            "c_boolean",
                            "c_date",
                            "c_timestamp",
                            "c_map",
                            "c_array"
                        },
                        new SeaTunnelDataType<?>[] {
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(30, 8),
                            BasicType.STRING_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            BasicType.BOOLEAN_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                            ArrayType.STRING_ARRAY_TYPE
                        });
        rowType =
                DataTypes.ROW(
                        new DataField(0, "c_tinyint", DataTypes.TINYINT()),
                        new DataField(1, "c_smallint", DataTypes.SMALLINT()),
                        new DataField(2, "c_int", DataTypes.INT()),
                        new DataField(3, "c_bigint", DataTypes.BIGINT()),
                        new DataField(4, "c_float", DataTypes.FLOAT()),
                        new DataField(5, "c_double", DataTypes.DOUBLE()),
                        new DataField(6, "c_decimal", DataTypes.DECIMAL(30, 8)),
                        new DataField(7, "c_string", DataTypes.STRING()),
                        new DataField(8, "c_bytes", DataTypes.BYTES()),
                        new DataField(9, "c_boolean", DataTypes.BOOLEAN()),
                        new DataField(10, "c_date", DataTypes.DATE()),
                        new DataField(11, "c_timestamp", DataTypes.TIMESTAMP(6)),
                        new DataField(
                                12, "c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                        new DataField(13, "c_array", DataTypes.ARRAY(DataTypes.STRING())));
    }

    @Test
    public void paimonToSeaTunnel() {
        SeaTunnelRowType convert = RowTypeConverter.convert(rowType);
        Assertions.assertEquals(convert, seaTunnelRowType);
    }

    @Test
    public void seaTunnelToPaimon() {
        RowType convert = RowTypeConverter.convert(seaTunnelRowType);
        Assertions.assertEquals(convert, rowType);
    }
}
