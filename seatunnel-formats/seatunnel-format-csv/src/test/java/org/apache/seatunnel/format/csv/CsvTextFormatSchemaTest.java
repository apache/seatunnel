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

package org.apache.seatunnel.format.csv;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils.Formatter;
import org.apache.seatunnel.format.csv.constant.CsvStringQuoteMode;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CsvTextFormatSchemaTest {
    public String content =
            "\"mess,age\","
                    + "\"message\","
                    + "true,"
                    + "1,"
                    + "2,"
                    + "3,"
                    + "4,"
                    + "6.66,"
                    + "7.77,"
                    + "8.8888888,"
                    + ','
                    + "2022-09-24,"
                    + "22:45:00,"
                    + "2022-09-24 22:45:00,"
                    // row field
                    + String.join("\u0003", Arrays.asList("1", "2", "3", "4", "5", "6"))
                    + '\002'
                    + "tyrantlucifer\00418\003Kris\00421"
                    + ','
                    // array field
                    + String.join("\u0002", Arrays.asList("1", "2", "3", "4", "5", "6"))
                    + ','
                    // map field
                    + "tyrantlucifer"
                    + '\003'
                    + "18"
                    + '\002'
                    + "Kris"
                    + '\003'
                    + "21"
                    + '\002'
                    + "nullValueKey"
                    + '\003'
                    + '\002'
                    + '\003'
                    + "1231";

    public SeaTunnelRowType seaTunnelRowType;

    @BeforeEach
    public void initSeaTunnelRowType() {
        seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "string_field1",
                            "string_field2",
                            "boolean_field",
                            "tinyint_field",
                            "smallint_field",
                            "int_field",
                            "bigint_field",
                            "float_field",
                            "double_field",
                            "decimal_field",
                            "null_field",
                            "date_field",
                            "time_field",
                            "timestamp_field",
                            "row_field",
                            "array_field",
                            "map_field"
                        },
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(30, 8),
                            BasicType.VOID_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            new SeaTunnelRowType(
                                    new String[] {
                                        "array_field", "map_field",
                                    },
                                    new SeaTunnelDataType<?>[] {
                                        ArrayType.INT_ARRAY_TYPE,
                                        new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE),
                                    }),
                            ArrayType.INT_ARRAY_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        });
    }

    @Test
    public void testParse() throws IOException {
        String delimiter = ",";
        CsvDeserializationSchema deserializationSchema =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter(delimiter)
                        .csvLineProcessor(new DefaultCsvLineProcessor())
                        .build();
        CsvSerializationSchema csvSerializationSchema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .dateTimeFormatter(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.MINIMAL)
                        .build();

        CsvSerializationSchema csvSerializationSchemaWithAllQuotes =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .dateTimeFormatter(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.ALL)
                        .build();

        CsvSerializationSchema csvSerializationSchemaWithNoneQuotes =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .dateTimeFormatter(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.NONE)
                        .build();

        SeaTunnelRow seaTunnelRow = deserializationSchema.deserialize(content.getBytes());
        Assertions.assertEquals("mess,age", seaTunnelRow.getField(0));
        Assertions.assertEquals(Boolean.TRUE, seaTunnelRow.getField(2));
        Assertions.assertEquals(Byte.valueOf("1"), seaTunnelRow.getField(3));
        Assertions.assertEquals(Short.valueOf("2"), seaTunnelRow.getField(4));
        Assertions.assertEquals(Integer.valueOf("3"), seaTunnelRow.getField(5));
        Assertions.assertEquals(Long.valueOf("4"), seaTunnelRow.getField(6));
        Assertions.assertEquals(Float.valueOf("6.66"), seaTunnelRow.getField(7));
        Assertions.assertEquals(Double.valueOf("7.77"), seaTunnelRow.getField(8));
        Assertions.assertEquals(BigDecimal.valueOf(8.8888888D), seaTunnelRow.getField(9));
        Assertions.assertNull((seaTunnelRow.getField(10)));
        Assertions.assertEquals(LocalDate.of(2022, 9, 24), seaTunnelRow.getField(11));
        Assertions.assertEquals(((Map<?, ?>) (seaTunnelRow.getField(16))).get("tyrantlucifer"), 18);
        Assertions.assertEquals(((Map<?, ?>) (seaTunnelRow.getField(16))).get("Kris"), 21);
        byte[] serialize = csvSerializationSchema.serialize(seaTunnelRow);
        Assertions.assertEquals(
                "\"mess,age\",message,true,1,2,3,4,6.66,7.77,8.8888888,,2022-09-24,22:45:00,2022-09-24 22:45:00.000000,1\u00032\u00033\u00034\u00035\u00036\u0002tyrantlucifer\u000418\u0003Kris\u000421,1\u00022\u00023\u00024\u00025\u00026,tyrantlucifer\u000318\u0002Kris\u000321\u0002nullValueKey\u0003\u0002\u00031231",
                new String(serialize));

        byte[] serialize1 = csvSerializationSchemaWithAllQuotes.serialize(seaTunnelRow);
        Assertions.assertEquals(
                "\"mess,age\",\"message\",true,1,2,3,4,6.66,7.77,8.8888888,,2022-09-24,22:45:00,2022-09-24 22:45:00.000000,1\u00032\u00033\u00034\u00035\u00036\u0002tyrantlucifer\u000418\u0003Kris\u000421,1\u00022\u00023\u00024\u00025\u00026,tyrantlucifer\u000318\u0002Kris\u000321\u0002nullValueKey\u0003\u0002\u00031231",
                new String(serialize1));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    csvSerializationSchemaWithNoneQuotes.serialize(seaTunnelRow);
                });
    }

    @Test
    public void testSerializationWithTimestamp() {
        String delimiter = ",";

        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"timestamp"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        LocalDateTime timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 123456000);
        CsvSerializationSchema csvSerializationSchema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(schema)
                        .dateTimeFormatter(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS)
                        .delimiter(delimiter)
                        .build();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {timestamp});

        assertEquals(
                "2022-09-24 22:45:00.123456", new String(csvSerializationSchema.serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 0);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "2022-09-24 22:45:00.000000", new String(csvSerializationSchema.serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 1000);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "2022-09-24 22:45:00.000001", new String(csvSerializationSchema.serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 123456);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "2022-09-24 22:45:00.000123", new String(csvSerializationSchema.serialize(row)));
    }

    @Test
    public void testCsvFileDeserialization() throws Exception {
        // Test reading and parsing from CSV file
        Path testFile =
                java.nio.file.Paths.get(
                        getClass().getClassLoader().getResource("testdata.csv").toURI());
        List<String> lines = java.nio.file.Files.readAllLines(testFile);

        // Skip header line
        lines = lines.subList(1, lines.size());

        // Expected test data
        String[][] expectedData = {
            {"New York", "ORDER001", "1000"},
            {"San Francisco,CA", "ORDER,002", "2000"},
            {"Los Angeles", "ORDER003", "3000"},
            {"Miami, FL", "", "5000"},
            {"Seattle", "ORDER,006,USA", "6000"},
            {"Boston", "ORDER007", "7000"},
        };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"city", "order_no", "amount"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        CsvDeserializationSchema schema =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .csvLineProcessor(new DefaultCsvLineProcessor())
                        .build();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

            // Remove quotes for comparison
            String cityField = result.get(0).replaceAll("\"", "").trim();
            String orderField = result.get(1).replaceAll("\"", "").trim();
            String amountField = result.get(2).trim();

            // Verify field values
            Assertions.assertEquals(
                    expectedData[i][0], cityField, "Mismatch in city field at line " + (i + 1));
            Assertions.assertEquals(
                    expectedData[i][1],
                    orderField,
                    "Mismatch in order_no field at line " + (i + 1));
            Assertions.assertEquals(
                    expectedData[i][2], amountField, "Mismatch in amount field at line " + (i + 1));

            // Verify amount is a valid integer
            Assertions.assertDoesNotThrow(
                    () -> Integer.parseInt(amountField),
                    "Amount should be a valid integer at line " + (i + 1));
        }
    }

    @Test
    public void testConfiguredDefaultsForNullMarkerAndMissingColumn() throws Exception {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                (Long) null,
                                                false,
                                                7,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                false,
                                                "unknown",
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "optional",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test");
        CsvDeserializationSchema schema =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .setCatalogTable(catalogTable)
                        .nullFormat("NULL")
                        .build();

        SeaTunnelRow row = schema.getSeaTunnelRow(Collections.singletonMap(0, "NULL"));
        Assertions.assertEquals(7, row.getField(0));
        Assertions.assertEquals("unknown", row.getField(1));
        Assertions.assertNull(row.getField(2));

        Map<Integer, String> emptyValues = new HashMap<>();
        emptyValues.put(0, "");
        emptyValues.put(1, "");
        SeaTunnelRow emptyRow = schema.getSeaTunnelRow(emptyValues);
        Assertions.assertNull(emptyRow.getField(0));
        Assertions.assertNull(emptyRow.getField(1));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(schema);
        }
        CsvDeserializationSchema restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (CsvDeserializationSchema) input.readObject();
        }
        Assertions.assertEquals(
                7, restored.getSeaTunnelRow(Collections.singletonMap(0, "NULL")).getField(0));
    }

    @Test
    public void testDateDefaultBeforeDataWithDifferentFormat() {
        assertTemporalDefaultWithDifferentDataFormat(LocalTimeType.LOCAL_DATE_TYPE, true);
    }

    @Test
    public void testDateDefaultAfterDataWithDifferentFormat() {
        assertTemporalDefaultWithDifferentDataFormat(LocalTimeType.LOCAL_DATE_TYPE, false);
    }

    @Test
    public void testTimestampDefaultBeforeDataWithDifferentFormat() {
        assertTemporalDefaultWithDifferentDataFormat(LocalTimeType.LOCAL_DATE_TIME_TYPE, true);
    }

    @Test
    public void testTimestampDefaultAfterDataWithDifferentFormat() {
        assertTemporalDefaultWithDifferentDataFormat(LocalTimeType.LOCAL_DATE_TIME_TYPE, false);
    }

    private void assertTemporalDefaultWithDifferentDataFormat(
            SeaTunnelDataType<?> type, boolean defaultFirst) {
        boolean date = type.equals(LocalTimeType.LOCAL_DATE_TYPE);
        CsvDeserializationSchema schema =
                schemaWithDefault(type, date ? "2020-01-01" : "2020-01-01 12:34:56");
        Object expectedDefault =
                date ? LocalDate.of(2020, 1, 1) : LocalDateTime.of(2020, 1, 1, 12, 34, 56);
        String data = date ? "2024/02/03" : "2024/02/03 04:05:06";
        Object expectedData =
                date ? LocalDate.of(2024, 2, 3) : LocalDateTime.of(2024, 2, 3, 4, 5, 6);
        if (defaultFirst) {
            assertEquals(
                    expectedDefault, schema.getSeaTunnelRow(Collections.emptyMap()).getField(0));
        }
        assertEquals(
                expectedData,
                schema.getSeaTunnelRow(Collections.singletonMap(0, data)).getField(0));
        assertEquals(
                expectedDefault,
                schema.getSeaTunnelRow(Collections.singletonMap(0, "NULL")).getField(0));
        assertEquals(
                expectedData,
                schema.getSeaTunnelRow(Collections.singletonMap(0, data)).getField(0));
    }

    @Test
    public void testTemporalDefaultsSurviveSerializationAfterData() throws Exception {
        for (boolean date : new boolean[] {true, false}) {
            CsvDeserializationSchema schema =
                    schemaWithDefault(
                            date
                                    ? LocalTimeType.LOCAL_DATE_TYPE
                                    : LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            date ? "2020-01-01" : "2020-01-01 12:34:56");
            Map<Integer, String> data =
                    Collections.singletonMap(0, date ? "2024/02/03" : "2024/02/03 04:05:06");
            Object expectedDefault =
                    date ? LocalDate.of(2020, 1, 1) : LocalDateTime.of(2020, 1, 1, 12, 34, 56);
            assertEquals(
                    expectedDefault,
                    roundTrip(schema).getSeaTunnelRow(Collections.emptyMap()).getField(0));
            SeaTunnelRow expected = schema.getSeaTunnelRow(data);
            CsvDeserializationSchema restored = roundTrip(schema);
            assertEquals(expected.getField(0), restored.getSeaTunnelRow(data).getField(0));
            assertEquals(
                    expectedDefault, restored.getSeaTunnelRow(Collections.emptyMap()).getField(0));
            assertEquals(
                    expected.getField(0), roundTrip(restored).getSeaTunnelRow(data).getField(0));
        }
    }

    @Test
    public void testTypedTemporalDefaultsArePreserved() throws Exception {
        SeaTunnelDataType<?>[] types = {
            LocalTimeType.LOCAL_DATE_TYPE,
            LocalTimeType.LOCAL_TIME_TYPE,
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            LocalTimeType.OFFSET_DATE_TIME_TYPE
        };
        Object[] values = {
            LocalDate.of(2020, 1, 1),
            LocalTime.of(12, 34),
            LocalDateTime.of(2020, 1, 1, 12, 34),
            OffsetDateTime.of(2020, 1, 1, 12, 34, 0, 0, ZoneOffset.ofHours(9))
        };
        for (int i = 0; i < types.length; i++) {
            CsvDeserializationSchema schema = schemaWithDefault(types[i], values[i]);
            Assertions.assertSame(
                    values[i], schema.getSeaTunnelRow(Collections.emptyMap()).getField(0));
            assertEquals(
                    values[i],
                    roundTrip(schema).getSeaTunnelRow(Collections.emptyMap()).getField(0));
        }
    }

    @Test
    public void testStructuredTemporalDefaultsUseIndependentFormats() {
        ArrayType<LocalDate[], LocalDate> dates =
                new ArrayType<>(LocalDate[].class, LocalTimeType.LOCAL_DATE_TYPE);
        LocalDate[] expected = {LocalDate.of(2020, 1, 1), LocalDate.of(2020, 2, 3)};
        for (Object value :
                new Object[] {
                    Arrays.asList("2020-01-01", "2020/02/03"), "2020-01-01\u00022020/02/03"
                }) {
            CsvDeserializationSchema schema = schemaWithDefault(dates, value);
            Assertions.assertArrayEquals(
                    expected,
                    (LocalDate[]) schema.getSeaTunnelRow(Collections.emptyMap()).getField(0));
            Assertions.assertArrayEquals(
                    new LocalDate[] {LocalDate.of(2024, 2, 3)},
                    (LocalDate[])
                            schema.getSeaTunnelRow(Collections.singletonMap(0, "2024.02.03"))
                                    .getField(0));
        }
    }

    @Test
    public void testTimestampTzDefaultDoesNotAffectDataParser() {
        CsvDeserializationSchema schema =
                schemaWithDefault(LocalTimeType.OFFSET_DATE_TIME_TYPE, "2020-01-01T12:34:56+09:00");
        OffsetDateTime expectedDefault = OffsetDateTime.parse("2020-01-01T12:34:56+09:00");
        assertEquals(expectedDefault, schema.getSeaTunnelRow(Collections.emptyMap()).getField(0));
        assertEquals(
                OffsetDateTime.of(2024, 2, 3, 4, 5, 6, 0, ZoneOffset.UTC),
                schema.getSeaTunnelRow(Collections.singletonMap(0, "2024/02/03 04:05:06"))
                        .getField(0));
        assertEquals(expectedDefault, schema.getSeaTunnelRow(Collections.emptyMap()).getField(0));
    }

    @Test
    public void testInvalidTemporalDefaultDoesNotExposeValue() {
        for (SeaTunnelDataType<?> type :
                new SeaTunnelDataType<?>[] {
                    LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                }) {
            String sensitiveDefault = "secret-invalid-temporal-default";
            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> schemaWithDefault(type, sensitiveDefault));
            StringWriter stackTrace = new StringWriter();
            exception.printStackTrace(new PrintWriter(stackTrace));
            Assertions.assertTrue(stackTrace.toString().contains("value"));
            Assertions.assertTrue(stackTrace.toString().contains(type.toString()));
            Assertions.assertFalse(stackTrace.toString().contains(sensitiveDefault));
        }
    }

    private CsvDeserializationSchema schemaWithDefault(SeaTunnelDataType<?> type, Object value) {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "value", type, (Long) null, true, value, null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test");
        return CsvDeserializationSchema.builder()
                .seaTunnelRowType(table.getSeaTunnelRowType())
                .setCatalogTable(table)
                .nullFormat("NULL")
                .build();
    }

    private CsvDeserializationSchema roundTrip(CsvDeserializationSchema schema) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(schema);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (CsvDeserializationSchema) input.readObject();
        }
    }

    @Test
    public void testInvalidConfiguredDefaultIsReportedWithColumnContext() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "enabled",
                                                BasicType.BOOLEAN_TYPE,
                                                (Long) null,
                                                false,
                                                "not-a-boolean",
                                                null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CsvDeserializationSchema.builder()
                                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                                        .setCatalogTable(catalogTable)
                                        .build());
        Assertions.assertTrue(exception.getMessage().contains("enabled"));
        Assertions.assertFalse(exception.getMessage().contains("not-a-boolean"));
    }

    @Test
    public void testInvalidNumericDefaultDoesNotExposeValueInStackTrace() {
        String sensitiveDefault = "secret-csv-default";
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "count",
                                                BasicType.INT_TYPE,
                                                (Long) null,
                                                false,
                                                sensitiveDefault,
                                                null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CsvDeserializationSchema.builder()
                                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                                        .setCatalogTable(catalogTable)
                                        .build());
        StringWriter stackTrace = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTrace));
        Assertions.assertTrue(stackTrace.toString().contains("count"));
        Assertions.assertTrue(stackTrace.toString().contains("INT"));
        Assertions.assertFalse(stackTrace.toString().contains(sensitiveDefault));
    }

    @Test
    public void testStructuredDefaultsAreIndependentBetweenRows() throws Exception {
        SeaTunnelRowType nestedRowType =
                new SeaTunnelRowType(
                        new String[] {"numbers"},
                        new SeaTunnelDataType<?>[] {ArrayType.INT_ARRAY_TYPE});
        Map<String, Object> mapDefault = new HashMap<>();
        mapDefault.put("count", 3);
        Map<String, Object> rowDefault = new HashMap<>();
        rowDefault.put("numbers", Arrays.asList(4, 5));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "arrays",
                                                new ArrayType<>(
                                                        Integer[][].class,
                                                        ArrayType.INT_ARRAY_TYPE),
                                                (Long) null,
                                                true,
                                                Arrays.asList(
                                                        Arrays.asList(1, 2), Arrays.asList(3)),
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "map",
                                                new MapType<>(
                                                        BasicType.STRING_TYPE, BasicType.INT_TYPE),
                                                (Long) null,
                                                true,
                                                mapDefault,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "row",
                                                nestedRowType,
                                                (Long) null,
                                                true,
                                                rowDefault,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "bytes",
                                                PrimitiveByteArrayType.INSTANCE,
                                                (Long) null,
                                                true,
                                                new byte[] {1, 2},
                                                null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test");
        CsvDeserializationSchema schema =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .setCatalogTable(catalogTable)
                        .build();
        mapDefault.clear();
        rowDefault.clear();

        SeaTunnelRow first = schema.getSeaTunnelRow(Collections.emptyMap());
        ((Integer[][]) first.getField(0))[0][0] = 99;
        ((Map<?, ?>) first.getField(1)).clear();
        ((Integer[]) ((SeaTunnelRow) first.getField(2)).getField(0))[0] = 99;
        ((byte[]) first.getField(3))[0] = 99;

        for (CsvDeserializationSchema copy :
                new CsvDeserializationSchema[] {schema, roundTrip(schema)}) {
            SeaTunnelRow second = copy.getSeaTunnelRow(Collections.emptyMap());
            Assertions.assertEquals(1, ((Integer[][]) second.getField(0))[0][0]);
            Assertions.assertEquals(3, ((Map<?, ?>) second.getField(1)).get("count"));
            Assertions.assertEquals(
                    4, ((Integer[]) ((SeaTunnelRow) second.getField(2)).getField(0))[0]);
            Assertions.assertArrayEquals(new byte[] {1, 2}, (byte[]) second.getField(3));
        }
    }

    @Test
    void testTimestampTzRoundTrip() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});

        CsvSerializationSchema ser =
                CsvSerializationSchema.builder().seaTunnelRowType(rowType).delimiter(",").build();
        CsvDeserializationSchema deser =
                CsvDeserializationSchema.builder().seaTunnelRowType(rowType).delimiter(",").build();

        OffsetDateTime[] cases = {
            OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(9)),
            OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(-8)),
            OffsetDateTime.of(2024, 6, 15, 0, 0, 0, 0, ZoneOffset.UTC),
        };

        for (OffsetDateTime original : cases) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {original});
            byte[] serialized = ser.serialize(row);
            SeaTunnelRow deserialized = deser.deserialize(serialized);
            OffsetDateTime result = (OffsetDateTime) deserialized.getField(0);
            Assertions.assertEquals(
                    original.toInstant(), result.toInstant(), "Epoch mismatch for " + original);
            Assertions.assertEquals(
                    original.getOffset(), result.getOffset(), "Offset mismatch for " + original);
        }
    }

    @Test
    void testTimestampTzWallClockUsesSessionTimezone() {
        // Issue #10795: wall-clock serialization must convert the instant to the
        // session (JVM) timezone, not strip the value's own offset.
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            CsvSerializationSchema ser =
                    CsvSerializationSchema.builder()
                            .seaTunnelRowType(rowType)
                            .delimiter(",")
                            .wallClockTimestampTz(true)
                            .build();

            // 2024-01-01T10:00:00Z == 2024-01-01 18:00:00 in Asia/Shanghai.
            SeaTunnelRow utcRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
                            });
            Assertions.assertEquals("2024-01-01 18:00:00", new String(ser.serialize(utcRow)));

            // A value already carrying the session offset keeps its wall-clock.
            SeaTunnelRow localRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 18, 0, 0, 0, ZoneOffset.ofHours(8))
                            });
            Assertions.assertEquals("2024-01-01 18:00:00", new String(ser.serialize(localRow)));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void testTimestampTzBackwardCompatFallback() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});
        CsvDeserializationSchema deser =
                CsvDeserializationSchema.builder().seaTunnelRowType(rowType).delimiter(",").build();

        SeaTunnelRow row = deser.deserialize("2024-01-01 03:00:00".getBytes());
        OffsetDateTime result = (OffsetDateTime) row.getField(0);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
        Assertions.assertEquals(
                java.time.LocalDateTime.of(2024, 1, 1, 3, 0, 0).toInstant(ZoneOffset.UTC),
                result.toInstant());
    }

    @Test
    void testTimestampTzWallClockExplicitZoneIdIsIndependentOfJvmDefault() {
        // When an explicit target zone is supplied, the JVM default must not affect the output
        // (issue #10795 follow-up). Run the assertion under a JVM default that does NOT match the
        // target zone to prove the explicit zone is the only thing that matters.
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});
        CsvSerializationSchema ser =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .wallClockTimestampTz(true)
                        .wallClockTimestampTzZoneId(java.time.ZoneId.of("Asia/Shanghai"))
                        .build();

        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            // 2024-01-01T10:00:00Z == 2024-01-01 18:00:00 in Asia/Shanghai, regardless of JVM
            // default being UTC.
            SeaTunnelRow utcRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
                            });
            Assertions.assertEquals("2024-01-01 18:00:00", new String(ser.serialize(utcRow)));
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
