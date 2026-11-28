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

package org.apache.seatunnel.translation.spark.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.translation.spark.exception.SparkTranslationErrorCode;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.spark.serialization.SeaTunnelRowConverter;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.MutableAny;
import org.apache.spark.sql.catalyst.expressions.MutableByte;
import org.apache.spark.sql.catalyst.expressions.MutableValue;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

class WideDecimalTest {

    private static final BigDecimal INT128_MIN =
            new BigDecimal("-170141183460469231731687303715884105728");
    private static final BigDecimal INT128_MAX =
            new BigDecimal("170141183460469231731687303715884105727");
    private static final BigDecimal SCALED =
            new BigDecimal("-12345678901234567890123456789012345678.0123456789");

    private static final DecimalType DECIMAL_39_0 = new DecimalType(39, 0);
    private static final DecimalType DECIMAL_50_10 = new DecimalType(50, 10);
    private static final DecimalType DECIMAL_10_2 = new DecimalType(10, 2);

    private static final SeaTunnelRowType INFO_TYPE =
            new SeaTunnelRowType(
                    new String[] {"inner_big", "name"},
                    new SeaTunnelDataType<?>[] {DECIMAL_39_0, BasicType.STRING_TYPE});

    /** Wide decimals at the top level and inside a ROW, next to an ordinary decimal. */
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "big", "scaled", "narrow", "info"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE, DECIMAL_39_0, DECIMAL_50_10, DECIMAL_10_2, INFO_TYPE
                    });

    private static List<SeaTunnelRow> sampleRows() {
        return Arrays.asList(
                row(1, INT128_MAX, SCALED, new BigDecimal("12345678.90"), INT128_MIN),
                row(2, INT128_MIN, SCALED.negate(), new BigDecimal("-0.01"), INT128_MAX),
                row(3, BigDecimal.ZERO, new BigDecimal("0E-10"), BigDecimal.ZERO.setScale(2), null),
                row(4, null, null, null, null));
    }

    private static SeaTunnelRow row(
            int id, BigDecimal big, BigDecimal scaled, BigDecimal narrow, BigDecimal innerBig) {
        SeaTunnelRow info = new SeaTunnelRow(new Object[] {innerBig, "n" + id});
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, big, scaled, narrow, info});
        row.setTableId("db.t");
        return row;
    }

    @Test
    void schemaUsesStringCarrierWithMetadata() {
        StructType schema = (StructType) TypeConverterUtils.convert(ROW_TYPE);

        assertCarrier(schema.apply("big"), 39, 0);
        assertCarrier(schema.apply("scaled"), 50, 10);
        Assertions.assertEquals(
                new org.apache.spark.sql.types.DecimalType(10, 2),
                schema.apply("narrow").dataType());
        Assertions.assertEquals(Metadata.empty(), schema.apply("narrow").metadata());

        StructType info = (StructType) schema.apply("info").dataType();
        assertCarrier(info.apply("inner_big"), 39, 0);
    }

    @Test
    void schemaRoundTripKeepsDecimalType() {
        DataType sparkType = TypeConverterUtils.convert(ROW_TYPE);
        Assertions.assertEquals(ROW_TYPE, TypeConverterUtils.convert(sparkType));
    }

    @Test
    void parcelledSchemaKeepsCarrier() {
        StructType schema = (StructType) TypeConverterUtils.parcel(ROW_TYPE);
        assertCarrier(schema.apply("big"), 39, 0);
    }

    @Test
    void internalRowConverterRoundTrip() throws IOException {
        InternalRowConverter converter = new InternalRowConverter(ROW_TYPE);
        for (SeaTunnelRow row : sampleRows()) {
            InternalRow internalRow = converter.convert(row);
            Object big = internalRow.get(3, DataTypes.StringType);
            if (row.getField(1) == null) {
                Assertions.assertTrue(internalRow.isNullAt(3));
            } else {
                Assertions.assertEquals(
                        UTF8String.fromString(((BigDecimal) row.getField(1)).toPlainString()), big);
            }
            assertRowEquals(row, converter.reconvert(internalRow));
        }
    }

    @Test
    void seaTunnelRowConverterRoundTripThroughRowEncoder() throws IOException {
        SeaTunnelRowConverter converter = new SeaTunnelRowConverter(ROW_TYPE);
        StructType schema = (StructType) TypeConverterUtils.parcel(ROW_TYPE);
        ExpressionEncoder<Row> encoder = rowEncoder(schema);
        for (SeaTunnelRow row : sampleRows()) {
            GenericRow genericRow = converter.convert(row);
            if (row.getField(1) != null) {
                Assertions.assertEquals(
                        ((BigDecimal) row.getField(1)).toPlainString(), genericRow.get(3));
            }
            // The transform path hands these rows to Spark through a RowEncoder.
            InternalRow encoded = encoder.createSerializer().apply(genericRow).copy();
            Row decoded = encoder.createDeserializer().apply(encoded);
            assertRowEquals(row, converter.reconvert((GenericRow) decoded));
        }
    }

    @Test
    void writePadsScaleWithoutRounding() throws IOException {
        SeaTunnelRow row = row(5, new BigDecimal("7"), new BigDecimal("5"), null, null);
        InternalRowConverter converter = new InternalRowConverter(ROW_TYPE);
        InternalRow internalRow = converter.convert(row);
        Assertions.assertEquals(
                UTF8String.fromString("5.0000000000"), internalRow.get(4, DataTypes.StringType));
        Assertions.assertEquals(
                new BigDecimal("5.0000000000"), converter.reconvert(internalRow).getField(2));
    }

    @Test
    void writeRejectsRoundingAndOverflow() {
        InternalRowConverter converter = new InternalRowConverter(ROW_TYPE);
        assertInvalidValue(
                () -> converter.convert(row(6, new BigDecimal("1.5"), BigDecimal.ONE, null, null)),
                "`big`",
                "DECIMAL(39, 0)",
                "more than 0 fraction digits");
        BigDecimal fortyDigits = new BigDecimal("1" + repeat('0', 39));
        assertInvalidValue(
                () -> converter.convert(row(7, fortyDigits, BigDecimal.ONE, null, null)),
                "`big`",
                "DECIMAL(39, 0)",
                "more than 39 integer digits");
        SeaTunnelRowConverter genericConverter = new SeaTunnelRowConverter(ROW_TYPE);
        assertInvalidValue(
                () ->
                        genericConverter.convert(
                                row(8, BigDecimal.ONE, BigDecimal.ONE, null, fortyDigits)),
                "`info.inner_big`",
                "DECIMAL(39, 0)",
                "more than 39 integer digits");
    }

    @Test
    void readRejectsMalformedCarrierText() {
        for (String text :
                new String[] {"", " 1", "1 ", "1e5", "1E+5", "NaN", "+1", "1,000", "0x1F", "-"}) {
            assertInvalidValue(
                    () -> WideDecimalUtils.fromCarrierText(text, DECIMAL_39_0, "big"),
                    "`big`",
                    "DECIMAL(39, 0)",
                    "not plain decimal text");
        }
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("1.5", DECIMAL_39_0, "big"),
                "`big`",
                "DECIMAL(39, 0)",
                "has 1 fraction digits, expected 0");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("1.00", DECIMAL_50_10, "scaled"),
                "`scaled`",
                "DECIMAL(50, 10)",
                "has 2 fraction digits, expected 10");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("1" + repeat('0', 39), DECIMAL_39_0, "big"),
                "`big`",
                "DECIMAL(39, 0)",
                "more than 39 digits");
    }

    @Test
    void readErrorNamesNestedColumnWithoutTheValue() {
        String longText = "x" + repeat('1', 100);
        GenericRow info = new GenericRow(new Object[] {longText, "n"});
        GenericRow row = new GenericRow(new Object[] {(byte) 0, "db.t", 1, null, null, null, info});
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () -> new SeaTunnelRowConverter(ROW_TYPE).reconvert(row),
                        "`info.inner_big`",
                        "DECIMAL(39, 0)",
                        "not plain decimal text");
        Assertions.assertFalse(e.getMessage().contains("x111"), e.getMessage());
    }

    @Test
    void internalRowReadRejectsMalformedCarrierText() {
        MutableValue[] values = new MutableValue[ROW_TYPE.getTotalFields() + 2];
        for (int i = 0; i < values.length; i++) {
            values[i] = new MutableAny();
        }
        values[0] = new MutableByte();
        values[0].update((byte) 0);
        values[1].update(UTF8String.fromString("db.t"));
        values[3].update(UTF8String.fromString("12.0"));
        assertInvalidValue(
                () -> new InternalRowConverter(ROW_TYPE).reconvert(new SpecificInternalRow(values)),
                "`big`",
                "DECIMAL(39, 0)",
                "has 1 fraction digits, expected 0");
    }

    @Test
    void ordinaryDecimalWithUnexpectedValueFailsInsteadOfFallingThrough() {
        MutableValue[] values = new MutableValue[ROW_TYPE.getTotalFields() + 2];
        for (int i = 0; i < values.length; i++) {
            values[i] = new MutableAny();
        }
        values[0] = new MutableByte();
        values[0].update((byte) 0);
        values[1].update(UTF8String.fromString("db.t"));
        values[5].update("not a decimal");
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new InternalRowConverter(ROW_TYPE)
                                        .reconvert(new SpecificInternalRow(values)));
        Assertions.assertTrue(e.getMessage().contains("java.lang.String"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("`narrow`"), e.getMessage());
    }

    @Test
    void preflightRejectsWideDecimalInArray() {
        assertCollectionError(arrayOf(DECIMAL_39_0), "demo.t_wide", "tags.element");
    }

    @Test
    void preflightRejectsWideDecimalMapValue() {
        assertCollectionError(
                new MapType<>(BasicType.STRING_TYPE, DECIMAL_39_0), "demo.t_wide", "col.value");
    }

    @Test
    void preflightRejectsWideDecimalMapKey() {
        assertCollectionError(
                new MapType<>(DECIMAL_39_0, BasicType.STRING_TYPE), "demo.t_wide", "col.key");
    }

    @Test
    void preflightRejectsWideDecimalInArrayInsideRow() {
        SeaTunnelRowType info =
                new SeaTunnelRowType(
                        new String[] {"scores"},
                        new SeaTunnelDataType<?>[] {arrayOf(DECIMAL_39_0)});
        assertCollectionError(info, "demo.t_wide", "col.scores.element");
    }

    @Test
    void preflightAllowsNarrowDecimalInCollections() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"tags", "attrs"},
                        new SeaTunnelDataType<?>[] {
                            arrayOf(new DecimalType(38, 0)),
                            new MapType<>(BasicType.STRING_TYPE, new DecimalType(38, 10))
                        });
        Assertions.assertDoesNotThrow(
                () -> new MultiTableManager(new CatalogTable[] {table("demo.ok", rowType)}));
    }

    @Test
    void preflightNamesOriginalTableAndColumnInMultiTableJob() {
        SeaTunnelRowType good =
                new SeaTunnelRowType(
                        new String[] {"id", "big"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, DECIMAL_39_0});
        SeaTunnelRowType bad =
                new SeaTunnelRowType(
                        new String[] {"id", "largeint_values"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, arrayOf(DECIMAL_39_0)});
        SeaTunnelRuntimeException e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                new MultiTableManager(
                                        new CatalogTable[] {
                                            table("demo.a_good", good), table("demo.b_bad", bad)
                                        }));
        Assertions.assertEquals(
                SparkTranslationErrorCode.WIDE_DECIMAL_IN_COLLECTION, e.getSeaTunnelErrorCode());
        Assertions.assertTrue(e.getMessage().contains("demo.b_bad"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("`largeint_values.element`"), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("column0"), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("column1"), e.getMessage());
    }

    @Test
    void multiTableJobWithTopLevelWideDecimalStillRoundTrips() throws IOException {
        SeaTunnelRowType a =
                new SeaTunnelRowType(
                        new String[] {"id", "big"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, DECIMAL_39_0});
        SeaTunnelRowType b =
                new SeaTunnelRowType(
                        new String[] {"scaled"}, new SeaTunnelDataType<?>[] {DECIMAL_50_10});
        MultiTableManager manager =
                new MultiTableManager(new CatalogTable[] {table("demo.a", a), table("demo.b", b)});
        StructType schema = manager.getTableSchema();
        assertCarrier(schema.apply("column1"), 39, 0);
        assertCarrier(schema.apply("column2"), 50, 10);

        SeaTunnelRow rowA = new SeaTunnelRow(new Object[] {1, INT128_MIN});
        rowA.setTableId("demo.a");
        SeaTunnelRow rowB = new SeaTunnelRow(new Object[] {SCALED});
        rowB.setTableId("demo.b");
        for (SeaTunnelRow row : Arrays.asList(rowA, rowB)) {
            GenericRow genericRow = manager.convert(row);
            assertRowEquals(row, manager.reconvert(genericRow));
        }
    }

    @Test
    void collectionConversionOutsidePreflightStillFails() {
        SeaTunnelRuntimeException e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> TypeConverterUtils.convert(arrayOf(DECIMAL_39_0)));
        Assertions.assertTrue(e.getMessage().contains("`element`"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("table is not known"), e.getMessage());
        SeaTunnelRowType rowInArray =
                new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {DECIMAL_39_0});
        e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                TypeConverterUtils.convert(
                                        new MapType<>(
                                                BasicType.STRING_TYPE,
                                                new ArrayType<>(
                                                        SeaTunnelRow[].class, rowInArray))));
        Assertions.assertEquals(
                SparkTranslationErrorCode.WIDE_DECIMAL_IN_COLLECTION, e.getSeaTunnelErrorCode());
        Assertions.assertTrue(e.getMessage().contains("`value.element.v`"), e.getMessage());
        SeaTunnelRowType arrayInRow =
                new SeaTunnelRowType(
                        new String[] {"id", "tags"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, arrayOf(DECIMAL_39_0)});
        e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> TypeConverterUtils.convert(arrayInRow));
        Assertions.assertTrue(e.getMessage().contains("`tags.element`"), e.getMessage());
        Assertions.assertEquals("tags", WideDecimalUtils.childPath("tags", null));
        e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> TypeConverterUtils.convert(arrayOf(DECIMAL_39_0), "tags"));
        Assertions.assertTrue(e.getMessage().contains("`tags.element`"), e.getMessage());
    }

    @Test
    void extremeExponentIsRejectedWithoutExpandingTheValue() {
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> {
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            new BigDecimal("1E+100000000"), DECIMAL_39_0, "big"),
                            "`big`",
                            "more than 39 integer digits");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            new BigDecimal("1E-100000000"),
                                            DECIMAL_50_10,
                                            "scaled"),
                            "`scaled`",
                            "more than 10 fraction digits");
                    Assertions.assertEquals(
                            "0",
                            WideDecimalUtils.toCarrierText(
                                    new BigDecimal("0E+100000000"), DECIMAL_39_0, "big"));
                    Assertions.assertEquals(
                            "0.0000000000",
                            WideDecimalUtils.toCarrierText(
                                    new BigDecimal("0E-100000000"), DECIMAL_50_10, "scaled"));
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.fromCarrierText(
                                            repeat('1', 100_000), DECIMAL_39_0, "big"),
                            "`big`",
                            "not plain decimal text");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.fromCarrierText(
                                            repeat('0', 1_000_000) + "1", DECIMAL_39_0, "big"),
                            "`big`",
                            "not plain decimal text");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.fromCarrierText(
                                            "1", new DecimalType(39, -100_000_000), "big"),
                            "`big`",
                            "not a multiple of 1E+100000000");
                    DecimalType hugeNegativeScale = new DecimalType(39, -800_000);
                    Assertions.assertEquals(
                            new BigDecimal(BigInteger.ONE, -800_000),
                            WideDecimalUtils.fromCarrierText(
                                    "1" + repeat('0', 800_000), hugeNegativeScale, "big"));
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            BigDecimal.valueOf(1, -2_147_483_635),
                                            new DecimalType(39, -2_147_483_635),
                                            "big"),
                            "`big`",
                            "too long to carry");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            BigDecimal.valueOf(1, -1_073_741_821),
                                            new DecimalType(39, -1_073_741_821),
                                            "big"),
                            "`big`",
                            "too long to carry");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.fromCarrierText(
                                            "0." + repeat('9', 800_000),
                                            new DecimalType(39, 800_000),
                                            "big"),
                            "`big`",
                            "more than 39 digits");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            BigDecimal.ZERO,
                                            new DecimalType(39, Integer.MAX_VALUE),
                                            "big"),
                            "`big`",
                            "too long to carry");
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.toCarrierText(
                                            new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE + 1),
                                            new DecimalType(Integer.MAX_VALUE, -1),
                                            "big"),
                            "`big`",
                            "too long to carry");
                    Assertions.assertEquals(
                            "0." + repeat('0', 1_000_000),
                            WideDecimalUtils.toCarrierText(
                                    BigDecimal.ZERO, new DecimalType(39, 1_000_000), "big"));
                    assertInvalidValue(
                            () ->
                                    WideDecimalUtils.fromSparkValue(
                                            UTF8String.fromString(repeat('1', 100_000)),
                                            DECIMAL_39_0,
                                            "big"),
                            "`big`",
                            "not plain decimal text");
                });
    }

    @Test
    void readLengthBoundAcceptsEveryValidValue() {
        // The bound is 3 + precision + |scale|; leading zeros are accepted within it.
        Assertions.assertEquals(
                BigDecimal.ONE,
                WideDecimalUtils.fromCarrierText(repeat('0', 41) + "1", DECIMAL_39_0, "big"));
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText(repeat('0', 42) + "1", DECIMAL_39_0, "big"),
                "not plain decimal text");
        Assertions.assertEquals(
                new BigDecimal("-0.5000000000"),
                WideDecimalUtils.fromCarrierText(
                        "-" + repeat('0', 51) + ".5000000000", DECIMAL_50_10, "scaled"));
        DecimalType widestHundreds = new DecimalType(Integer.MAX_VALUE, -1);
        Assertions.assertEquals(
                "10", WideDecimalUtils.toCarrierText(BigDecimal.TEN, widestHundreds, "huge"));
        Assertions.assertEquals(
                new BigDecimal("1E+1"),
                WideDecimalUtils.fromCarrierText("10", widestHundreds, "huge"));
        DecimalType scaleAbovePrecision = new DecimalType(39, 50);
        String smallest =
                WideDecimalUtils.toCarrierText(
                        new BigDecimal("-1E-50"), scaleAbovePrecision, "tiny");
        Assertions.assertEquals(
                new BigDecimal("-1E-50"),
                WideDecimalUtils.fromCarrierText(smallest, scaleAbovePrecision, "tiny"));
        Assertions.assertEquals(
                BigDecimal.ONE,
                WideDecimalUtils.fromCarrierText(
                        "1", new DecimalType(Integer.MAX_VALUE, 0), "huge"));
        Assertions.assertEquals(
                INT128_MIN,
                WideDecimalUtils.fromSparkValue(
                        UTF8String.fromString(INT128_MIN.toPlainString()), DECIMAL_39_0, "big"));
    }

    @Test
    void writeAcceptsValuesThatNeedNoRounding() {
        Assertions.assertEquals(
                "1", WideDecimalUtils.toCarrierText(new BigDecimal("1.000"), DECIMAL_39_0, "big"));
        Assertions.assertEquals(
                "1", WideDecimalUtils.toCarrierText(new BigDecimal("10E-1"), DECIMAL_39_0, "big"));
        Assertions.assertEquals(
                "100", WideDecimalUtils.toCarrierText(new BigDecimal("1E+2"), DECIMAL_39_0, "big"));
        Assertions.assertEquals(
                INT128_MAX.toPlainString(),
                WideDecimalUtils.toCarrierText(INT128_MAX.setScale(5), DECIMAL_39_0, "big"));
        Assertions.assertEquals(
                "-0.0000000001",
                WideDecimalUtils.toCarrierText(new BigDecimal("-1E-10"), DECIMAL_50_10, "scaled"));
        String maxIntegerPart = repeat('9', 40);
        Assertions.assertEquals(
                maxIntegerPart + ".0000000000",
                WideDecimalUtils.toCarrierText(
                        new BigDecimal(maxIntegerPart), DECIMAL_50_10, "scaled"));
        assertInvalidValue(
                () ->
                        WideDecimalUtils.toCarrierText(
                                new BigDecimal(maxIntegerPart + "9"), DECIMAL_50_10, "scaled"),
                "more than 40 integer digits");
        assertInvalidValue(
                () -> WideDecimalUtils.toCarrierText(new BigDecimal("0.10"), DECIMAL_39_0, "big"),
                "more than 0 fraction digits");
    }

    @Test
    void negativeScaleRoundTrips() {
        DecimalType hundreds = new DecimalType(50, -2);
        String text = WideDecimalUtils.toCarrierText(new BigDecimal("12300"), hundreds, "hundreds");
        Assertions.assertEquals("12300", text);
        Assertions.assertEquals(
                new BigDecimal("1.23E+4"), WideDecimalUtils.fromCarrierText(text, hundreds, "h"));
        StructType schema =
                (StructType)
                        TypeConverterUtils.convert(
                                new SeaTunnelRowType(
                                        new String[] {"h"}, new SeaTunnelDataType<?>[] {hundreds}));
        Assertions.assertEquals(
                new SeaTunnelRowType(new String[] {"h"}, new SeaTunnelDataType<?>[] {hundreds}),
                TypeConverterUtils.convert(schema));
        Assertions.assertEquals(
                "0", WideDecimalUtils.toCarrierText(BigDecimal.ZERO, hundreds, "hundreds"));
        Assertions.assertEquals(0, WideDecimalUtils.fromCarrierText("0", hundreds, "h").signum());
        String maxValue = repeat('9', 50) + "00";
        Assertions.assertEquals(
                maxValue,
                WideDecimalUtils.toCarrierText(new BigDecimal(maxValue), hundreds, "hundreds"));
        Assertions.assertEquals(
                new BigDecimal(maxValue).setScale(-2),
                WideDecimalUtils.fromCarrierText(maxValue, hundreds, "h"));
        assertInvalidValue(
                () -> WideDecimalUtils.toCarrierText(new BigDecimal("12345"), hundreds, "h"),
                "not a multiple of 1E+2");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("12345", hundreds, "h"),
                "not a multiple of 1E+2");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("123.00", hundreds, "h"),
                "has 2 fraction digits, expected 0");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("50", hundreds, "h"),
                "not a multiple of 1E+2");
        Assertions.assertEquals(
                new BigDecimal("-1E+2"), WideDecimalUtils.fromCarrierText("-100", hundreds, "h"));
        Assertions.assertEquals(
                new BigDecimal("1E+2"), WideDecimalUtils.fromCarrierText("000100", hundreds, "h"));
        Assertions.assertEquals(
                0, WideDecimalUtils.fromCarrierText("-000", hundreds, "h").signum());
    }

    @Test
    void longValueThatNeedsRoundingFailsFast() {
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () ->
                        assertInvalidValue(
                                () ->
                                        WideDecimalUtils.toCarrierText(
                                                new BigDecimal("1." + repeat('0', 200_000) + "1"),
                                                DECIMAL_39_0,
                                                "big"),
                                "more than 0 fraction digits"));
    }

    @Test
    void valueErrorDoesNotContainTheValue() {
        String digits = "1234567890123456789012345678901234567890";
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () ->
                                WideDecimalUtils.toCarrierText(
                                        new BigDecimal(digits), DECIMAL_39_0, "big"),
                        "`big`");
        Assertions.assertFalse(e.getMessage().contains("12345678901"), e.getMessage());
        e =
                assertInvalidValue(
                        () -> WideDecimalUtils.fromCarrierText(digits + ".5", DECIMAL_39_0, "big"),
                        "`big`");
        Assertions.assertFalse(e.getMessage().contains("12345678901"), e.getMessage());
    }

    @Test
    void nonTextCarrierValueIsRejectedByBothConverters() {
        GenericRow genericRow =
                new GenericRow(new Object[] {(byte) 0, "db.t", 1, INT128_MAX, null, null, null});
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () -> new SeaTunnelRowConverter(ROW_TYPE).reconvert(genericRow),
                        "`big`",
                        "java.math.BigDecimal instead of the STRING carrier");

        MutableValue[] values = new MutableValue[ROW_TYPE.getTotalFields() + 2];
        for (int i = 0; i < values.length; i++) {
            values[i] = new MutableAny();
        }
        values[0] = new MutableByte();
        values[0].update((byte) 0);
        values[1].update(UTF8String.fromString("db.t"));
        values[3].update(Decimal.apply(1L));
        e =
                assertInvalidValue(
                        () ->
                                new InternalRowConverter(ROW_TYPE)
                                        .reconvert(new SpecificInternalRow(values)),
                        "`big`",
                        "instead of the STRING carrier");
        Assertions.assertTrue(e.getMessage().contains(Decimal.class.getName()), e.getMessage());
    }

    @Test
    void multiTableValueErrorNamesSourceTableAndColumn() {
        SeaTunnelRowType a =
                new SeaTunnelRowType(
                        new String[] {"id", "amount"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, DECIMAL_39_0});
        SeaTunnelRowType b =
                new SeaTunnelRowType(
                        new String[] {"total"}, new SeaTunnelDataType<?>[] {DECIMAL_50_10});
        MultiTableManager manager =
                new MultiTableManager(new CatalogTable[] {table("demo.a", a), table("demo.b", b)});

        SeaTunnelRow rowA =
                new SeaTunnelRow(new Object[] {1, new BigDecimal("1" + repeat('0', 39))});
        rowA.setTableId("demo.a");
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () -> manager.convert(rowA),
                        "`amount` of table `demo.a`",
                        "DECIMAL(39, 0)");
        Assertions.assertFalse(e.getMessage().contains("column"), e.getMessage());

        // demo.b's only column is merged to column2 (column0 = INT, column1 = DECIMAL(39, 0)).
        GenericRow badB = new GenericRow(new Object[] {(byte) 0, "demo.b", null, null, "1.5"});
        e =
                assertInvalidValue(
                        () -> manager.reconvert(badB),
                        "`total` of table `demo.b`",
                        "DECIMAL(50, 10)");
        Assertions.assertFalse(e.getMessage().contains("column2"), e.getMessage());
    }

    @Test
    void singleTableValueErrorNamesTheTable() {
        MultiTableManager manager =
                new MultiTableManager(new CatalogTable[] {table("demo.single", ROW_TYPE)});
        SeaTunnelRow row = row(9, new BigDecimal("1.5"), BigDecimal.ONE, null, null);
        assertInvalidValue(() -> manager.convert(row), "`big` of table `demo.single`");
    }

    @Test
    void valueErrorNamesFullPathOfDeeplyNestedColumn() {
        SeaTunnelRowType outerType =
                new SeaTunnelRowType(
                        new String[] {"id", "outer"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            new SeaTunnelRowType(
                                    new String[] {"info"}, new SeaTunnelDataType<?>[] {INFO_TYPE})
                        });
        BigDecimal fortyDigits = new BigDecimal("1" + repeat('0', 39));
        SeaTunnelRow info = new SeaTunnelRow(new Object[] {fortyDigits, "n"});
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {1, new SeaTunnelRow(new Object[] {info})});
        row.setTableId("db.t");
        assertInvalidValue(
                () -> new InternalRowConverter(outerType).convert(row),
                "`outer.info.inner_big`",
                "more than 39 integer digits");
        assertInvalidValue(
                () -> new SeaTunnelRowConverter(outerType).convert(row),
                "`outer.info.inner_big`",
                "more than 39 integer digits");
    }

    @Test
    void sourceColumnReplacesTheWholeTopLevelName() {
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () -> WideDecimalUtils.fromCarrierText("1.5", DECIMAL_39_0, "a.b"),
                        "`a.b`");
        WideDecimalUtils.InvalidWideDecimalException invalid =
                (WideDecimalUtils.InvalidWideDecimalException) e;
        Assertions.assertTrue(
                invalid.withSource("db.t", "amount")
                        .getMessage()
                        .contains("Column `amount` of table `db.t` declared"),
                invalid.withSource("db.t", "amount").getMessage());
        Assertions.assertTrue(
                invalid.withParent("column1")
                        .withSource(null, "outer.x")
                        .getMessage()
                        .contains("Column `outer.x.a.b` declared"),
                invalid.withParent("column1").withSource(null, "outer.x").getMessage());
    }

    @Test
    void metadataReadBackRejectsInconsistentFields() {
        Metadata onlyPrecision =
                new MetadataBuilder().putLong(WideDecimalUtils.PRECISION_KEY, 39).build();
        Metadata both = WideDecimalUtils.metadata(DECIMAL_39_0);
        Metadata narrow =
                new MetadataBuilder()
                        .putLong(WideDecimalUtils.PRECISION_KEY, 20)
                        .putLong(WideDecimalUtils.SCALE_KEY, 0)
                        .build();
        Metadata wrongType =
                new MetadataBuilder()
                        .putString(WideDecimalUtils.PRECISION_KEY, "39")
                        .putLong(WideDecimalUtils.SCALE_KEY, 0)
                        .build();
        Metadata nullScale =
                Metadata.fromJson(
                        String.format(
                                "{\"%s\":39,\"%s\":null}",
                                WideDecimalUtils.PRECISION_KEY, WideDecimalUtils.SCALE_KEY));
        for (StructField field :
                new StructField[] {
                    new StructField("a", DataTypes.StringType, true, onlyPrecision),
                    new StructField("b", DataTypes.LongType, true, both),
                    new StructField("c", DataTypes.StringType, true, narrow),
                    new StructField("d", DataTypes.StringType, true, wrongType),
                    new StructField("e", DataTypes.StringType, true, nullScale)
                }) {
            SeaTunnelRuntimeException e =
                    Assertions.assertThrows(
                            SeaTunnelRuntimeException.class,
                            () ->
                                    TypeConverterUtils.convert(
                                            new StructType(new StructField[] {field})));
            Assertions.assertEquals(
                    SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_METADATA,
                    e.getSeaTunnelErrorCode());
            Assertions.assertTrue(e.getMessage().contains("`" + field.name() + "`"));
        }
    }

    private static void assertCollectionError(
            SeaTunnelDataType<?> columnType, String tablePath, String columnPath) {
        String columnName = columnPath.substring(0, columnPath.indexOf('.'));
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", columnName},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, columnType});
        SeaTunnelRuntimeException e =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                new MultiTableManager(
                                        new CatalogTable[] {table(tablePath, rowType)}));
        Assertions.assertEquals(
                SparkTranslationErrorCode.WIDE_DECIMAL_IN_COLLECTION, e.getSeaTunnelErrorCode());
        String message = e.getMessage();
        for (String expected :
                new String[] {
                    "`" + columnPath + "`",
                    tablePath,
                    "DECIMAL(39, 0)",
                    "Spark supports DECIMAL precision up to 38",
                    "doris.read.field",
                    "source query",
                    "Zeta or Flink"
                }) {
            Assertions.assertTrue(message.contains(expected), expected + " not in: " + message);
        }
    }

    private static SeaTunnelRuntimeException assertInvalidValue(
            Executable executable, String... expectedParts) {
        SeaTunnelRuntimeException e =
                Assertions.assertThrows(SeaTunnelRuntimeException.class, executable);
        Assertions.assertEquals(
                SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_VALUE, e.getSeaTunnelErrorCode());
        for (String expected : expectedParts) {
            Assertions.assertTrue(
                    e.getMessage().contains(expected), expected + " not in: " + e.getMessage());
        }
        return e;
    }

    private static void assertCarrier(StructField field, long precision, long scale) {
        Assertions.assertEquals(DataTypes.StringType, field.dataType(), field.name());
        Assertions.assertEquals(
                precision, field.metadata().getLong(WideDecimalUtils.PRECISION_KEY), field.name());
        Assertions.assertEquals(
                scale, field.metadata().getLong(WideDecimalUtils.SCALE_KEY), field.name());
    }

    private static void assertRowEquals(SeaTunnelRow expected, SeaTunnelRow actual) {
        Assertions.assertEquals(expected.getArity(), actual.getArity());
        for (int i = 0; i < expected.getArity(); i++) {
            Object e = expected.getField(i);
            Object a = actual.getField(i);
            if (e instanceof SeaTunnelRow) {
                assertRowEquals((SeaTunnelRow) e, (SeaTunnelRow) a);
            } else {
                // BigDecimal.equals also compares the scale.
                Assertions.assertEquals(e, a, "field " + i);
            }
        }
    }

    private static ArrayType<BigDecimal[], BigDecimal> arrayOf(DecimalType type) {
        return new ArrayType<>(BigDecimal[].class, type);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ExpressionEncoder<Row> rowEncoder(StructType schema) {
        // Spark 3.3 API; the Spark 2.4 and 3.3 end-to-end tests cover both versions.
        scala.collection.Seq attributes = (scala.collection.Seq) schema.toAttributes();
        return RowEncoder.apply(schema).resolveAndBind(attributes, SimpleAnalyzer$.MODULE$);
    }

    private static CatalogTable table(String path, SeaTunnelRowType rowType) {
        String[] parts = path.split("\\.");
        return CatalogTableUtil.getCatalogTable("test", parts[0], null, parts[1], rowType);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
