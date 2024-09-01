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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.NullType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class SparkSinkTest {

    @Test
    @DisabledOnJre(
            value = JRE.JAVA_11,
            disabledReason =
                    "We should update apache common lang3 version to 3.8 to avoid NPE, "
                            + "see https://github.com/apache/commons-lang/commit/50ce8c44e1601acffa39f5568f0fc140aade0564")
    public void testSparkSinkWriteDataWithCopy() {
        // We should make sure that the data is written to the sink with copy.
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .appName("testSparkSinkWriteDataWithCopy")
                        .getOrCreate();
        StructType structType =
                new StructType()
                        .add("int", IntegerType)
                        .add("string", StringType)
                        .add("boolean", BooleanType)
                        .add("float", FloatType)
                        .add("double", DoubleType)
                        .add("byte", ByteType)
                        .add("short", ShortType)
                        .add("long", LongType)
                        .add("decimal", new DecimalType(10, 2))
                        .add("date", DateType)
                        // .add("time", TimeType) unsupported time type in Spark 3.3.0. Please trace
                        // https://issues.apache.org/jira/browse/SPARK-41549
                        .add("timestamp", TimestampType)
                        .add("null", NullType)
                        .add("array_string", new ArrayType(StringType, true))
                        .add("array_boolean", new ArrayType(BooleanType, true))
                        .add("array_byte", new ArrayType(ByteType, true))
                        .add("array_short", new ArrayType(ShortType, true))
                        .add("array_int", new ArrayType(IntegerType, true))
                        .add("array_long", new ArrayType(LongType, true))
                        .add("array_float", new ArrayType(FloatType, true))
                        .add("array_double", new ArrayType(DoubleType, true))
                        .add("map", new MapType(StringType, StringType, true));

        GenericRow row1 =
                new GenericRow(
                        new Object[] {
                            42,
                            "string1",
                            true,
                            1.1f,
                            33.33,
                            (byte) 1,
                            (short) 2,
                            Long.MAX_VALUE,
                            new BigDecimal("55.55"),
                            LocalDate.parse("2021-01-01"),
                            Timestamp.valueOf("2021-01-01 00:00:00"),
                            null,
                            Arrays.asList("string1", "string2", "string3"),
                            Arrays.asList(true, false, true),
                            Arrays.asList((byte) 1, (byte) 2, (byte) 3),
                            Arrays.asList((short) 1, (short) 2, (short) 3),
                            Arrays.asList(1, 2, 3),
                            Arrays.asList(1L, 2L, 3L),
                            Arrays.asList(1.1f, 2.2f, 3.3f),
                            Arrays.asList(11.11, 22.22, 33.33),
                            new HashMap<String, String>() {
                                {
                                    put("key1", "value1");
                                    put("key2", "value2");
                                    put("key3", "value3");
                                }
                            }
                        });

        GenericRow row1WithRow =
                new GenericRow(
                        new Object[] {
                            (byte) 1,
                            "test.test.test",
                            new HashMap<String, String>() {
                                {
                                    put("k", "v");
                                }
                            },
                            42,
                            "string1",
                            true,
                            1.1f,
                            33.33,
                            (byte) 1,
                            (short) 2,
                            Long.MAX_VALUE,
                            new BigDecimal("55.55"),
                            LocalDate.parse("2021-01-01"),
                            Timestamp.valueOf("2021-01-01 00:00:00"),
                            null,
                            Arrays.asList("string1", "string2", "string3"),
                            Arrays.asList(true, false, true),
                            Arrays.asList((byte) 1, (byte) 2, (byte) 3),
                            Arrays.asList((short) 1, (short) 2, (short) 3),
                            Arrays.asList(1, 2, 3),
                            Arrays.asList(1L, 2L, 3L),
                            Arrays.asList(1.1f, 2.2f, 3.3f),
                            Arrays.asList(11.11, 22.22, 33.33),
                            new HashMap<String, String>() {
                                {
                                    put("key1", "value1");
                                    put("key2", "value2");
                                    put("key3", "value3");
                                }
                            },
                            row1
                        });

        GenericRow row2 =
                new GenericRow(
                        new Object[] {
                            12,
                            "string2",
                            false,
                            2.2f,
                            43.33,
                            (byte) 5,
                            (short) 42,
                            Long.MAX_VALUE - 1,
                            new BigDecimal("25.55"),
                            LocalDate.parse("2011-01-01"),
                            Timestamp.valueOf("2020-01-01 00:00:00"),
                            null,
                            Arrays.asList("string3", "string2", "string1"),
                            Arrays.asList(true, false, false),
                            Arrays.asList((byte) 3, (byte) 4, (byte) 5),
                            Arrays.asList((short) 2, (short) 6, (short) 8),
                            Arrays.asList(2, 4, 6),
                            Arrays.asList(643634L, 421412L, 543543L),
                            Arrays.asList(1.24f, 21.2f, 32.3f),
                            Arrays.asList(421.11, 5322.22, 323.33),
                            new HashMap<String, String>() {
                                {
                                    put("key2", "value534");
                                    put("key3", "value3");
                                    put("key4", "value43");
                                }
                            }
                        });

        GenericRow row2WithRow =
                new GenericRow(
                        new Object[] {
                            (byte) 1,
                            "test.test.test",
                            new HashMap<String, String>() {
                                {
                                    put("k", "v");
                                }
                            },
                            12,
                            "string2",
                            false,
                            2.2f,
                            43.33,
                            (byte) 5,
                            (short) 42,
                            Long.MAX_VALUE - 1,
                            new BigDecimal("25.55"),
                            LocalDate.parse("2011-01-01"),
                            Timestamp.valueOf("2020-01-01 00:00:00"),
                            null,
                            Arrays.asList("string3", "string2", "string1"),
                            Arrays.asList(true, false, false),
                            Arrays.asList((byte) 3, (byte) 4, (byte) 5),
                            Arrays.asList((short) 2, (short) 6, (short) 8),
                            Arrays.asList(2, 4, 6),
                            Arrays.asList(643634L, 421412L, 543543L),
                            Arrays.asList(1.24f, 21.2f, 32.3f),
                            Arrays.asList(421.11, 5322.22, 323.33),
                            new HashMap<String, String>() {
                                {
                                    put("key2", "value534");
                                    put("key3", "value3");
                                    put("key4", "value43");
                                }
                            },
                            row2
                        });

        GenericRow row3 =
                new GenericRow(
                        new Object[] {
                            233,
                            "string3",
                            true,
                            231.1f,
                            3533.33,
                            (byte) 7,
                            (short) 2,
                            Long.MAX_VALUE - 2,
                            new BigDecimal("65.55"),
                            LocalDate.parse("2001-01-01"),
                            Timestamp.valueOf("2031-01-01 00:00:00"),
                            null,
                            Arrays.asList("string1fsa", "stringdsa2", "strfdsaing3"),
                            Arrays.asList(false, true, true),
                            Arrays.asList((byte) 6, (byte) 2, (byte) 1),
                            Arrays.asList((short) 7, (short) 8, (short) 9),
                            Arrays.asList(3, 77, 22),
                            Arrays.asList(143L, 642L, 533L),
                            Arrays.asList(24.1f, 54.2f, 1.3f),
                            Arrays.asList(431.11, 2422.22, 3243.33),
                            new HashMap<String, String>() {
                                {
                                    put("keyfs1", "valfdsue1");
                                    put("kedfasy2", "vafdslue2");
                                    put("kefdsay3", "vfdasalue3");
                                }
                            }
                        });

        GenericRow row3WithRow =
                new GenericRow(
                        new Object[] {
                            (byte) 1,
                            "test.test.test",
                            new HashMap<String, String>() {
                                {
                                    put("k", "v");
                                }
                            },
                            233,
                            "string3",
                            true,
                            231.1f,
                            3533.33,
                            (byte) 7,
                            (short) 2,
                            Long.MAX_VALUE - 2,
                            new BigDecimal("65.55"),
                            LocalDate.parse("2001-01-01"),
                            Timestamp.valueOf("2031-01-01 00:00:00"),
                            null,
                            Arrays.asList("string1fsa", "stringdsa2", "strfdsaing3"),
                            Arrays.asList(false, true, true),
                            Arrays.asList((byte) 6, (byte) 2, (byte) 1),
                            Arrays.asList((short) 7, (short) 8, (short) 9),
                            Arrays.asList(3, 77, 22),
                            Arrays.asList(143L, 642L, 533L),
                            Arrays.asList(24.1f, 54.2f, 1.3f),
                            Arrays.asList(431.11, 2422.22, 3243.33),
                            new HashMap<String, String>() {
                                {
                                    put("keyfs1", "valfdsue1");
                                    put("kedfasy2", "vafdslue2");
                                    put("kefdsay3", "vfdasalue3");
                                }
                            },
                            row3
                        });

        SeaTunnelRowType rowType =
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
                            new org.apache.seatunnel.api.table.type.DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            BasicType.VOID_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.BOOLEAN_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.BYTE_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.SHORT_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.INT_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.LONG_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.FLOAT_ARRAY_TYPE,
                            org.apache.seatunnel.api.table.type.ArrayType.DOUBLE_ARRAY_TYPE,
                            new org.apache.seatunnel.api.table.type.MapType<>(
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE),
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
                                        new org.apache.seatunnel.api.table.type.DecimalType(10, 2),
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        BasicType.VOID_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .STRING_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .BOOLEAN_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .BYTE_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .SHORT_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .INT_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .LONG_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .FLOAT_ARRAY_TYPE,
                                        org.apache.seatunnel.api.table.type.ArrayType
                                                .DOUBLE_ARRAY_TYPE,
                                        new org.apache.seatunnel.api.table.type.MapType<>(
                                                BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                                    })
                        });
        structType.add("row", structType);
        StructType parcelStructType = (StructType) TypeConverterUtils.parcel(rowType);
        Dataset<Row> dataset =
                spark.createDataFrame(
                        Arrays.asList(row1WithRow, row2WithRow, row3WithRow), parcelStructType);
        SparkSinkInjector.inject(
                        dataset.write(),
                        new SeaTunnelSinkWithBuffer(),
                        new CatalogTable[] {
                            CatalogTableUtil.getCatalogTable(
                                    "test", "test", "test", "test", rowType)
                        },
                        spark.sparkContext().applicationId())
                .option("checkpointLocation", "/tmp")
                .mode(SaveMode.Append)
                .save();
        spark.close();
    }
}
