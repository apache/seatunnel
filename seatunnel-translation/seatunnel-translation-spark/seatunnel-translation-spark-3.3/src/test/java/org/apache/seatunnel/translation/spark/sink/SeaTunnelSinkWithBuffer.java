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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;

public class SeaTunnelSinkWithBuffer implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    @Override
    public String getPluginName() {
        return "SeaTunnelSinkWithBuffer";
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return new SeaTunnelRowType(
                new String[] {
                    "int",
                    "string",
                    "boolean",
                    "float",
                    "double",
                    "byte",
                    "short",
                    "long",
                    "decimal",
                    "date",
                    "timestamp",
                    "null",
                    "array_string",
                    "array_boolean",
                    "array_byte",
                    "array_short",
                    "array_int",
                    "array_long",
                    "array_float",
                    "array_double",
                    "map",
                    "row"
                },
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.FLOAT_TYPE,
                    BasicType.DOUBLE_TYPE,
                    BasicType.BYTE_TYPE,
                    BasicType.SHORT_TYPE,
                    BasicType.LONG_TYPE,
                    new DecimalType(10, 2),
                    LocalTimeType.LOCAL_DATE_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    BasicType.VOID_TYPE,
                    ArrayType.STRING_ARRAY_TYPE,
                    ArrayType.BOOLEAN_ARRAY_TYPE,
                    ArrayType.BYTE_ARRAY_TYPE,
                    ArrayType.SHORT_ARRAY_TYPE,
                    ArrayType.INT_ARRAY_TYPE,
                    ArrayType.LONG_ARRAY_TYPE,
                    ArrayType.FLOAT_ARRAY_TYPE,
                    ArrayType.DOUBLE_ARRAY_TYPE,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                    new SeaTunnelRowType(
                            new String[] {
                                "int",
                                "string",
                                "boolean",
                                "float",
                                "double",
                                "byte",
                                "short",
                                "long",
                                "decimal",
                                "date",
                                "timestamp",
                                "null",
                                "array_string",
                                "array_boolean",
                                "array_byte",
                                "array_short",
                                "array_int",
                                "array_long",
                                "array_float",
                                "array_double",
                                "map"
                            },
                            new SeaTunnelDataType[] {
                                BasicType.INT_TYPE,
                                BasicType.STRING_TYPE,
                                BasicType.BOOLEAN_TYPE,
                                BasicType.FLOAT_TYPE,
                                BasicType.DOUBLE_TYPE,
                                BasicType.BYTE_TYPE,
                                BasicType.SHORT_TYPE,
                                BasicType.LONG_TYPE,
                                new DecimalType(10, 2),
                                LocalTimeType.LOCAL_DATE_TYPE,
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                BasicType.VOID_TYPE,
                                ArrayType.STRING_ARRAY_TYPE,
                                ArrayType.BOOLEAN_ARRAY_TYPE,
                                ArrayType.BYTE_ARRAY_TYPE,
                                ArrayType.SHORT_ARRAY_TYPE,
                                ArrayType.INT_ARRAY_TYPE,
                                ArrayType.LONG_ARRAY_TYPE,
                                ArrayType.FLOAT_ARRAY_TYPE,
                                ArrayType.DOUBLE_ARRAY_TYPE,
                                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                            })
                });
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new SeaTunnelSinkWithBufferWriter();
    }
}
