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
                "1.5");
        BigDecimal fortyDigits = new BigDecimal("1" + repeat('0', 39));
        assertInvalidValue(
                () -> converter.convert(row(7, fortyDigits, BigDecimal.ONE, null, null)),
                "`big`",
                "DECIMAL(39, 0)",
                "more than 39 digits");
        SeaTunnelRowConverter genericConverter = new SeaTunnelRowConverter(ROW_TYPE);
        assertInvalidValue(
                () ->
                        genericConverter.convert(
                                row(8, BigDecimal.ONE, BigDecimal.ONE, null, fortyDigits)),
                "`info.inner_big`",
                "DECIMAL(39, 0)",
                "more than 39 digits");
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
                "scale is 1, expected 0");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("1.00", DECIMAL_50_10, "scaled"),
                "`scaled`",
                "DECIMAL(50, 10)",
                "scale is 2, expected 10");
        assertInvalidValue(
                () -> WideDecimalUtils.fromCarrierText("1" + repeat('0', 39), DECIMAL_39_0, "big"),
                "`big`",
                "DECIMAL(39, 0)",
                "more than 39 digits");
    }

    @Test
    void readErrorNamesNestedColumnAndTruncatesValue() {
        String longText = "x" + repeat('1', 100);
        GenericRow info = new GenericRow(new Object[] {longText, "n"});
        GenericRow row = new GenericRow(new Object[] {(byte) 0, "db.t", 1, null, null, null, info});
        SeaTunnelRuntimeException e =
                assertInvalidValue(
                        () -> new SeaTunnelRowConverter(ROW_TYPE).reconvert(row),
                        "`info.inner_big`",
                        "DECIMAL(39, 0)",
                        "not plain decimal text");
        Assertions.assertTrue(e.getMessage().contains(longText.substring(0, 64) + "..."));
        Assertions.assertFalse(e.getMessage().contains(longText));
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
                "scale is 1, expected 0");
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
        for (StructField field :
                new StructField[] {
                    new StructField("a", DataTypes.StringType, true, onlyPrecision),
                    new StructField("b", DataTypes.LongType, true, both),
                    new StructField("c", DataTypes.StringType, true, narrow),
                    new StructField("d", DataTypes.StringType, true, wrongType)
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
