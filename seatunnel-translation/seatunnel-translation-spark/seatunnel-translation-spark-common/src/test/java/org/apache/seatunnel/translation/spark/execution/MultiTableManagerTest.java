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
package org.apache.seatunnel.translation.spark.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.serialization.InternalMultiRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.spark.utils.InstantConverterUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.MutableAny;
import org.apache.spark.sql.catalyst.expressions.MutableBoolean;
import org.apache.spark.sql.catalyst.expressions.MutableByte;
import org.apache.spark.sql.catalyst.expressions.MutableDouble;
import org.apache.spark.sql.catalyst.expressions.MutableFloat;
import org.apache.spark.sql.catalyst.expressions.MutableInt;
import org.apache.spark.sql.catalyst.expressions.MutableLong;
import org.apache.spark.sql.catalyst.expressions.MutableShort;
import org.apache.spark.sql.catalyst.expressions.MutableValue;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.translation.spark.utils.TypeConverterUtils.ROW_KIND_FIELD;
import static org.apache.seatunnel.translation.spark.utils.TypeConverterUtils.TABLE_ID;
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

public class MultiTableManagerTest {

    private SeaTunnelRowType rowType1;
    private CatalogTable catalogTable1;
    private SeaTunnelRowType rowType2;
    private CatalogTable catalogTable2;
    private SeaTunnelRowType rowType3;
    private CatalogTable catalogTable3;

    private StructType structType1;
    private StructType structType2;
    private StructType structType3;

    private SeaTunnelRow seaTunnelRow1;
    private SeaTunnelRow seaTunnelRow3;

    private SpecificInternalRow specificInternalRow1;
    private SpecificInternalRow specificInternalRow2;
    private SpecificInternalRow specificInternalRow3;

    @Test
    public void testMergeSchema() {
        initSchema();
        MultiTableManager multiTableManager1 =
                new MultiTableManager(new CatalogTable[] {catalogTable1, catalogTable2});
        StructType tableSchema1 = multiTableManager1.getTableSchema();
        Assertions.assertEquals(structType1, tableSchema1);

        MultiTableManager multiTableManager2 =
                new MultiTableManager(new CatalogTable[] {catalogTable2, catalogTable1});
        StructType tableSchema2 = multiTableManager2.getTableSchema();
        Assertions.assertEquals(structType1, tableSchema2);

        MultiTableManager multiTableManager3 =
                new MultiTableManager(new CatalogTable[] {catalogTable2, catalogTable3});
        StructType tableSchema3 = multiTableManager3.getTableSchema();
        Assertions.assertEquals(structType2, tableSchema3);

        MultiTableManager multiTableManager4 =
                new MultiTableManager(
                        new CatalogTable[] {catalogTable1, catalogTable2, catalogTable3});
        StructType tableSchema4 = multiTableManager4.getTableSchema();
        Assertions.assertEquals(structType2, tableSchema4);

        MultiTableManager multiTableManager5 =
                new MultiTableManager(new CatalogTable[] {catalogTable1});
        StructType tableSchema5 = multiTableManager5.getTableSchema();
        Assertions.assertEquals(structType3, tableSchema5);
    }

    @Test
    public void testWriteConverter() throws IOException {
        initSchema();
        initData();
        MultiTableManager multiTableManager =
                new MultiTableManager(new CatalogTable[] {catalogTable1});
        SeaTunnelRow seaTunnelRow = multiTableManager.reconvert(specificInternalRow1);
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            Object[] values = seaTunnelRow.getFields();
            Object[] actual = seaTunnelRow1.getFields();
            for (int v = 0; v < values.length; v++) {
                if (values[v] instanceof Object[]) {
                    Assertions.assertArrayEquals((Object[]) values[v], (Object[]) actual[v]);
                } else {
                    Assertions.assertEquals(values[v], actual[v]);
                }
            }
        }
    }

    @Test
    public void testMultiWriteConverter() throws IOException {
        initSchema();
        initData();
        MultiTableManager multiTableManager =
                new MultiTableManager(
                        new CatalogTable[] {catalogTable1, catalogTable2, catalogTable3});
        SeaTunnelRow seaTunnelRow = multiTableManager.reconvert(specificInternalRow1);
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            Object[] values = seaTunnelRow.getFields();
            Object[] actual = seaTunnelRow1.getFields();
            for (int v = 0; v < values.length; v++) {
                if (values[v] instanceof Object[]) {
                    Assertions.assertArrayEquals((Object[]) values[v], (Object[]) actual[v]);
                } else {
                    Assertions.assertEquals(values[v], actual[v]);
                }
            }
        }
    }

    @Test
    public void testMultiReaderConverter() throws IOException {
        initSchema();
        initData();
        MultiTableManager multiTableManager =
                new MultiTableManager(
                        new CatalogTable[] {catalogTable1, catalogTable2, catalogTable3});
        InternalMultiRowCollector internalMultiRowCollector =
                (InternalMultiRowCollector)
                        multiTableManager.getInternalRowCollector(null, null, null);
        Map<String, InternalRowConverter> rowSerializationMap =
                internalMultiRowCollector.getRowSerializationMap();
        InternalRow internalRow =
                rowSerializationMap.get(seaTunnelRow1.getTableId()).convert(seaTunnelRow1);
        for (int v = 0; v < specificInternalRow2.numFields(); v++) {
            if (specificInternalRow2.genericGet(v) instanceof ArrayBasedMapData) {
                Assertions.assertEquals(
                        specificInternalRow2.getMap(v).keyArray(),
                        internalRow.getMap(v).keyArray());
                Assertions.assertEquals(
                        specificInternalRow2.getMap(v).valueArray(),
                        internalRow.getMap(v).valueArray());
            } else if (specificInternalRow2.genericGet(v) instanceof SpecificInternalRow) {
                SpecificInternalRow expected =
                        (SpecificInternalRow) specificInternalRow2.genericGet(v);
                SpecificInternalRow actual =
                        (SpecificInternalRow) ((SpecificInternalRow) internalRow).genericGet(v);
                for (int o = 0; v < expected.numFields(); v++) {
                    if (expected.genericGet(o) instanceof ArrayBasedMapData) {
                        Assertions.assertEquals(
                                expected.getMap(o).keyArray(), actual.getMap(o).keyArray());
                        Assertions.assertEquals(
                                expected.getMap(o).valueArray(), actual.getMap(o).valueArray());
                    } else {
                        Assertions.assertEquals(
                                expected.genericGet(v),
                                ((SpecificInternalRow) actual).genericGet(v));
                    }
                }
            } else {
                Assertions.assertEquals(
                        specificInternalRow2.genericGet(v),
                        ((SpecificInternalRow) internalRow).genericGet(v));
            }
        }
        InternalRow internalRow3 =
                rowSerializationMap.get(seaTunnelRow3.getTableId()).convert(seaTunnelRow3);
        Assertions.assertEquals(specificInternalRow3, internalRow3);
        for (int v = 0; v < specificInternalRow3.numFields(); v++) {
            Assertions.assertEquals(
                    specificInternalRow3.genericGet(v),
                    ((SpecificInternalRow) internalRow3).genericGet(v));
        }
    }

    @Test
    public void testMultiConvertSeaTunnelRow() throws IOException {
        initSchema();
        initData();
        MultiTableManager multiTableManager =
                new MultiTableManager(
                        new CatalogTable[] {catalogTable1, catalogTable2, catalogTable3});

        GenericRow genericRow1 = multiTableManager.convert(seaTunnelRow1);
        Assertions.assertEquals(seaTunnelRow1, multiTableManager.reconvert(genericRow1));

        GenericRow genericRow3 = multiTableManager.convert(seaTunnelRow3);
        Assertions.assertEquals(seaTunnelRow3, multiTableManager.reconvert(genericRow3));
    }

    @Test
    public void testReaderConverter() throws IOException {
        initSchema();
        initData();
        MultiTableManager multiTableManager =
                new MultiTableManager(new CatalogTable[] {catalogTable1});
        InternalRowCollector internalRowCollector =
                multiTableManager.getInternalRowCollector(null, null, null);
        InternalRowConverter rowSerialization = internalRowCollector.getRowSerialization();
        InternalRow internalRow = rowSerialization.convert(seaTunnelRow1);
        for (int v = 0; v < specificInternalRow1.numFields(); v++) {
            if (specificInternalRow1.genericGet(v) instanceof ArrayBasedMapData) {
                Assertions.assertEquals(
                        specificInternalRow1.getMap(v).keyArray(),
                        internalRow.getMap(v).keyArray());
                Assertions.assertEquals(
                        specificInternalRow1.getMap(v).valueArray(),
                        internalRow.getMap(v).valueArray());
            } else if (specificInternalRow1.genericGet(v) instanceof SpecificInternalRow) {
                SpecificInternalRow expected =
                        (SpecificInternalRow) specificInternalRow1.genericGet(v);
                SpecificInternalRow actual =
                        (SpecificInternalRow) ((SpecificInternalRow) internalRow).genericGet(v);
                for (int o = 0; v < expected.numFields(); v++) {
                    if (expected.genericGet(o) instanceof ArrayBasedMapData) {
                        Assertions.assertEquals(
                                expected.getMap(o).keyArray(), actual.getMap(o).keyArray());
                        Assertions.assertEquals(
                                expected.getMap(o).valueArray(), actual.getMap(o).valueArray());
                    } else {
                        Assertions.assertEquals(
                                expected.genericGet(v),
                                ((SpecificInternalRow) actual).genericGet(v));
                    }
                }
            } else {
                Assertions.assertEquals(
                        specificInternalRow1.genericGet(v),
                        ((SpecificInternalRow) internalRow).genericGet(v));
            }
        }
    }

    public void initSchema() {
        this.rowType1 =
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

        this.rowType2 =
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

        this.rowType3 =
                new SeaTunnelRowType(
                        new String[] {
                            "int",
                            "string",
                            "float1",
                            "float2",
                            "boolean1",
                            "boolean2",
                            "double",
                            "byte1",
                            "byte2",
                            "long",
                            "short",
                            "decimal",
                            "timestamp",
                            "date",
                            "null"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.SHORT_TYPE,
                            new org.apache.seatunnel.api.table.type.DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            BasicType.VOID_TYPE,
                        });

        catalogTable1 = CatalogTableUtil.getCatalogTable("test", "test", "test", "test1", rowType1);

        catalogTable2 = CatalogTableUtil.getCatalogTable("test", "test", "test", "test2", rowType2);

        catalogTable3 = CatalogTableUtil.getCatalogTable("test", "test", "test", "test3", rowType3);

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

        structType1 =
                new StructType()
                        .add(ROW_KIND_FIELD, DataTypes.ByteType)
                        .add(TABLE_ID, DataTypes.StringType)
                        .add("column0", IntegerType)
                        .add("column1", StringType)
                        .add("column2", BooleanType)
                        .add("column3", FloatType)
                        .add("column4", DoubleType)
                        .add("column5", ByteType)
                        .add("column6", ShortType)
                        .add("column7", LongType)
                        .add("column8", new DecimalType(10, 2))
                        .add("column9", DateType)
                        .add("column10", TimestampType)
                        .add("column11", NullType)
                        .add("column12", new ArrayType(StringType, true))
                        .add("column13", new ArrayType(BooleanType, true))
                        .add("column14", new ArrayType(ByteType, true))
                        .add("column15", new ArrayType(ShortType, true))
                        .add("column16", new ArrayType(IntegerType, true))
                        .add("column17", new ArrayType(LongType, true))
                        .add("column18", new ArrayType(FloatType, true))
                        .add("column19", new ArrayType(DoubleType, true))
                        .add("column20", new MapType(StringType, StringType, true))
                        .add("column21", structType);

        structType2 =
                new StructType()
                        .add(ROW_KIND_FIELD, DataTypes.ByteType)
                        .add(TABLE_ID, DataTypes.StringType)
                        .add("column0", IntegerType)
                        .add("column1", StringType)
                        .add("column2", BooleanType)
                        .add("column3", FloatType)
                        .add("column4", DoubleType)
                        .add("column5", ByteType)
                        .add("column6", ShortType)
                        .add("column7", LongType)
                        .add("column8", new DecimalType(10, 2))
                        .add("column9", DateType)
                        .add("column10", TimestampType)
                        .add("column11", NullType)
                        .add("column12", new ArrayType(StringType, true))
                        .add("column13", new ArrayType(BooleanType, true))
                        .add("column14", new ArrayType(ByteType, true))
                        .add("column15", new ArrayType(ShortType, true))
                        .add("column16", new ArrayType(IntegerType, true))
                        .add("column17", new ArrayType(LongType, true))
                        .add("column18", new ArrayType(FloatType, true))
                        .add("column19", new ArrayType(DoubleType, true))
                        .add("column20", new MapType(StringType, StringType, true))
                        .add("column21", structType)
                        .add("column22", FloatType)
                        .add("column23", BooleanType)
                        .add("column24", ByteType);

        structType3 =
                new StructType()
                        .add(ROW_KIND_FIELD, DataTypes.ByteType)
                        .add(TABLE_ID, DataTypes.StringType)
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
                        .add("map", new MapType(StringType, StringType, true))
                        .add("row", structType);
    }

    public void initData() {

        SeaTunnelRow row1 =
                new SeaTunnelRow(
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
                            LocalDateTime.parse("2031-01-01T00:00:00"),
                            null,
                            new String[] {"string1fsa", "stringdsa2", "strfdsaing3"},
                            new Boolean[] {false, true, true},
                            new Byte[] {(byte) 6, (byte) 2, (byte) 1},
                            new Short[] {(short) 7, (short) 8, (short) 9},
                            new Integer[] {3, 77, 22},
                            new Long[] {143L, 642L, 533L},
                            new Float[] {24.1f, 54.2f, 1.3f},
                            new Double[] {431.11, 2422.22, 3243.33},
                            new HashMap<String, String>() {
                                {
                                    put("keyfs1", "valfdsue1");
                                    put("kedfasy2", "vafdslue2");
                                    put("kefdsay3", "vfdasalue3");
                                }
                            }
                        });

        seaTunnelRow1 =
                new SeaTunnelRow(
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
                            LocalDateTime.parse("2031-01-01T00:00:00"),
                            null,
                            new String[] {"string1fsa", "stringdsa2", "strfdsaing3"},
                            new Boolean[] {false, true, true},
                            new Byte[] {(byte) 6, (byte) 2, (byte) 1},
                            new Short[] {(short) 7, (short) 8, (short) 9},
                            new Integer[] {3, 77, 22},
                            new Long[] {143L, 642L, 533L},
                            new Float[] {24.1f, 54.2f, 1.3f},
                            new Double[] {431.11, 2422.22, 3243.33},
                            new HashMap<String, String>() {
                                {
                                    put("keyfs1", "valfdsue1");
                                    put("kedfasy2", "vafdslue2");
                                    put("kefdsay3", "vfdasalue3");
                                }
                            },
                            row1
                        });
        seaTunnelRow1.setRowKind(RowKind.INSERT);
        seaTunnelRow1.setTableId("test.test.test1");

        MutableValue[] mutableValues = new MutableValue[21];
        mutableValues[0] = new MutableInt();
        mutableValues[0].update(233);
        mutableValues[1] = new MutableAny();
        mutableValues[1].update(UTF8String.fromString("string3"));
        mutableValues[2] = new MutableBoolean();
        mutableValues[2].update(true);
        mutableValues[3] = new MutableFloat();
        mutableValues[3].update(231.1f);
        mutableValues[4] = new MutableDouble();
        mutableValues[4].update(3533.33);
        mutableValues[5] = new MutableByte();
        mutableValues[5].update((byte) 7);
        mutableValues[6] = new MutableShort();
        mutableValues[6].update((short) 2);
        mutableValues[7] = new MutableLong();
        mutableValues[7].update(Long.MAX_VALUE - 2);
        mutableValues[8] = new MutableAny();
        mutableValues[8].update(Decimal.apply(new BigDecimal("65.55")));
        mutableValues[9] = new MutableInt();
        mutableValues[9].update((int) LocalDate.parse("2001-01-01").toEpochDay());
        mutableValues[10] = new MutableAny();
        mutableValues[10].update(
                InstantConverterUtils.toEpochMicro(
                        Timestamp.valueOf(LocalDateTime.parse("2031-01-01T00:00:00")).toInstant()));
        mutableValues[11] = new MutableAny();
        mutableValues[12] = new MutableAny();
        mutableValues[12].update(
                ArrayData.toArrayData(
                        new Object[] {
                            UTF8String.fromString("string1fsa"),
                            UTF8String.fromString("stringdsa2"),
                            UTF8String.fromString("strfdsaing3")
                        }));

        mutableValues[13] = new MutableAny();
        mutableValues[13].update(ArrayData.toArrayData(new Boolean[] {false, true, true}));

        mutableValues[14] = new MutableAny();
        mutableValues[14].update(ArrayData.toArrayData(new Byte[] {(byte) 6, (byte) 2, (byte) 1}));

        mutableValues[15] = new MutableAny();
        mutableValues[15].update(
                ArrayData.toArrayData(new Short[] {(short) 7, (short) 8, (short) 9}));

        mutableValues[16] = new MutableAny();
        mutableValues[16].update(ArrayData.toArrayData(new Integer[] {3, 77, 22}));

        mutableValues[17] = new MutableAny();
        mutableValues[17].update(ArrayData.toArrayData(new Long[] {143L, 642L, 533L}));

        mutableValues[18] = new MutableAny();
        mutableValues[18].update(ArrayData.toArrayData(new Float[] {24.1f, 54.2f, 1.3f}));

        mutableValues[19] = new MutableAny();
        mutableValues[19].update(ArrayData.toArrayData(new Double[] {431.11, 2422.22, 3243.33}));

        mutableValues[20] = new MutableAny();
        mutableValues[20].update(
                ArrayBasedMapData.apply(
                        new UTF8String[] {
                            UTF8String.fromString("kefdsay3"),
                            UTF8String.fromString("keyfs1"),
                            UTF8String.fromString("kedfasy2")
                        },
                        new UTF8String[] {
                            UTF8String.fromString("vfdasalue3"),
                            UTF8String.fromString("valfdsue1"),
                            UTF8String.fromString("vafdslue2")
                        }));

        SpecificInternalRow specificInternalRow = new SpecificInternalRow(mutableValues);

        MutableValue[] mutableValues1 = new MutableValue[24];

        mutableValues1[0] = new MutableByte();
        mutableValues1[0].update(RowKind.INSERT.toByteValue());
        mutableValues1[1] = new MutableAny();
        mutableValues1[1].update(UTF8String.fromString("test.test.test1"));
        mutableValues1[2] = new MutableInt();
        mutableValues1[2].update(233);
        mutableValues1[3] = new MutableAny();
        mutableValues1[3].update(UTF8String.fromString("string3"));
        mutableValues1[4] = new MutableBoolean();
        mutableValues1[4].update(true);
        mutableValues1[5] = new MutableFloat();
        mutableValues1[5].update(231.1f);
        mutableValues1[6] = new MutableDouble();
        mutableValues1[6].update(3533.33);
        mutableValues1[7] = new MutableByte();
        mutableValues1[7].update((byte) 7);
        mutableValues1[8] = new MutableShort();
        mutableValues1[8].update((short) 2);
        mutableValues1[9] = new MutableLong();
        mutableValues1[9].update(Long.MAX_VALUE - 2);
        mutableValues1[10] = new MutableAny();
        mutableValues1[10].update(Decimal.apply(new BigDecimal("65.55")));
        mutableValues1[11] = new MutableInt();
        mutableValues1[11].update((int) LocalDate.parse("2001-01-01").toEpochDay());
        mutableValues1[12] = new MutableAny();
        mutableValues1[12].update(
                InstantConverterUtils.toEpochMicro(
                        Timestamp.valueOf(LocalDateTime.parse("2031-01-01T00:00:00")).toInstant()));
        mutableValues1[13] = new MutableAny();
        mutableValues1[14] = new MutableAny();
        mutableValues1[14].update(
                ArrayData.toArrayData(
                        new UTF8String[] {
                            UTF8String.fromString("string1fsa"),
                            UTF8String.fromString("stringdsa2"),
                            UTF8String.fromString("strfdsaing3")
                        }));

        mutableValues1[15] = new MutableAny();
        mutableValues1[15].update(ArrayData.toArrayData(new Boolean[] {false, true, true}));

        mutableValues1[16] = new MutableAny();
        mutableValues1[16].update(ArrayData.toArrayData(new Byte[] {(byte) 6, (byte) 2, (byte) 1}));

        mutableValues1[17] = new MutableAny();
        mutableValues1[17].update(
                ArrayData.toArrayData(new Short[] {(short) 7, (short) 8, (short) 9}));

        mutableValues1[18] = new MutableAny();
        mutableValues1[18].update(ArrayData.toArrayData(new Integer[] {3, 77, 22}));

        mutableValues1[19] = new MutableAny();
        mutableValues1[19].update(ArrayData.toArrayData(new Long[] {143L, 642L, 533L}));

        mutableValues1[20] = new MutableAny();
        mutableValues1[20].update(ArrayData.toArrayData(new Float[] {24.1f, 54.2f, 1.3f}));

        mutableValues1[21] = new MutableAny();
        mutableValues1[21].update(ArrayData.toArrayData(new Double[] {431.11, 2422.22, 3243.33}));

        mutableValues1[22] = new MutableAny();
        mutableValues1[22].update(
                ArrayBasedMapData.apply(
                        new UTF8String[] {
                            UTF8String.fromString("kefdsay3"),
                            UTF8String.fromString("keyfs1"),
                            UTF8String.fromString("kedfasy2")
                        },
                        new UTF8String[] {
                            UTF8String.fromString("vfdasalue3"),
                            UTF8String.fromString("valfdsue1"),
                            UTF8String.fromString("vafdslue2")
                        }));

        mutableValues1[23] = new MutableAny();
        mutableValues1[23].update(specificInternalRow);

        specificInternalRow1 = new SpecificInternalRow(mutableValues1);

        MutableValue[] mutableValues2 = new MutableValue[27];

        for (int i = 0; i < mutableValues1.length; i++) {
            mutableValues2[i] = mutableValues1[i].copy();
        }
        mutableValues2[24] = new MutableAny();
        mutableValues2[25] = new MutableAny();
        mutableValues2[26] = new MutableAny();

        specificInternalRow2 = new SpecificInternalRow(mutableValues2);

        seaTunnelRow3 =
                new SeaTunnelRow(
                        new Object[] {
                            233,
                            "string3",
                            231.1f,
                            231.1f,
                            true,
                            true,
                            3533.33,
                            (byte) 7,
                            (byte) 7,
                            Long.MAX_VALUE - 2,
                            (short) 2,
                            new BigDecimal("65.55"),
                            LocalDateTime.parse("2031-01-01T00:00:00"),
                            LocalDate.parse("2001-01-01"),
                            null
                        });
        seaTunnelRow3.setRowKind(RowKind.INSERT);
        seaTunnelRow3.setTableId("test.test.test3");

        // [0, 1, 3, 22, 2, 23, 4, 5, 24, 7, 6, 8, 10, 9, 11]
        MutableValue[] mutableValues3 = new MutableValue[27];
        mutableValues3[0] = new MutableByte();
        mutableValues3[0].update(RowKind.INSERT.toByteValue());
        mutableValues3[1] = new MutableAny();
        mutableValues3[1].update(UTF8String.fromString("test.test.test3"));

        mutableValues3[2] = new MutableInt();
        mutableValues3[2].update(233);

        mutableValues3[3] = new MutableAny();
        mutableValues3[3].update(UTF8String.fromString("string3"));

        mutableValues3[5] = new MutableFloat();
        mutableValues3[5].update(231.1f);

        mutableValues3[24] = new MutableFloat();
        mutableValues3[24].update(231.1f);

        mutableValues3[4] = new MutableBoolean();
        mutableValues3[4].update(true);

        mutableValues3[25] = new MutableBoolean();
        mutableValues3[25].update(true);

        mutableValues3[6] = new MutableDouble();
        mutableValues3[6].update(3533.33);

        mutableValues3[7] = new MutableByte();
        mutableValues3[7].update((byte) 7);

        mutableValues3[26] = new MutableByte();
        mutableValues3[26].update((byte) 7);

        mutableValues3[9] = new MutableLong();
        mutableValues3[9].update(Long.MAX_VALUE - 2);

        mutableValues3[8] = new MutableShort();
        mutableValues3[8].update((short) 2);

        mutableValues3[10] = new MutableAny();
        mutableValues3[10].update(Decimal.apply(new BigDecimal("65.55")));

        mutableValues3[12] = new MutableLong();
        mutableValues3[12].update(
                InstantConverterUtils.toEpochMicro(
                        Timestamp.valueOf(LocalDateTime.parse("2031-01-01T00:00:00")).toInstant()));

        mutableValues3[11] = new MutableInt();
        mutableValues3[11].update((int) LocalDate.parse("2001-01-01").toEpochDay());

        for (int i = 0; i < mutableValues3.length; i++) {
            if (mutableValues3[i] == null) {
                mutableValues3[i] = new MutableAny();
            }
        }
        specificInternalRow3 = new SpecificInternalRow(mutableValues3);
    }
}
