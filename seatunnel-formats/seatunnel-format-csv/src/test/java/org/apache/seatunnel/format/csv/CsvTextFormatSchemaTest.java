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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils.Formatter;
import org.apache.seatunnel.format.csv.constant.CsvStringQuoteMode;
import org.apache.seatunnel.format.csv.exception.SeaTunnelCsvFormatException;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
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
    void testStringFieldContainingDelimiterRoundTrip() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field", "int_field", "nullable_string_field"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });

        String[] delimiters = {",", "|", "\t", "\u0001"};
        for (String delimiter : delimiters) {
            CsvSerializationSchema serializationSchema =
                    CsvSerializationSchema.builder()
                            .seaTunnelRowType(rowType)
                            .delimiter(delimiter)
                            .quoteMode(CsvStringQuoteMode.MINIMAL)
                            .nullValue("NULL")
                            .build();
            CsvDeserializationSchema deserializationSchema =
                    CsvDeserializationSchema.builder()
                            .seaTunnelRowType(rowType)
                            .delimiter(delimiter)
                            .nullFormat("NULL")
                            .build();

            String value = "a" + delimiter + "b";
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[] {value, 42, null});

            byte[] serialized = serializationSchema.serialize(seaTunnelRow);
            String serializedField = new String(serialized, StandardCharsets.UTF_8);

            Map<Integer, String> splits =
                    deserializationSchema.splitLineBySeaTunnelRowType(serializedField, rowType, 0);
            SeaTunnelRow deserialized = deserializationSchema.getSeaTunnelRow(splits);

            assertEquals(
                    value,
                    deserialized.getField(0),
                    "String field must survive round trip with delimiter [" + delimiter + "]");
            assertEquals(
                    Integer.valueOf(42),
                    deserialized.getField(1),
                    "Int field must survive round trip with delimiter [" + delimiter + "]");
            Assertions.assertNull(
                    deserialized.getField(2),
                    "Null field must survive round trip with delimiter [" + delimiter + "]");
        }
    }

    @Test
    void testStringFieldWithEmbeddedQuotesAndNewlineRoundTrip() {
        String delimiter = "|";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field", "int_field", "nullable_string_field"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });

        CsvSerializationSchema serializationSchema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .quoteMode(CsvStringQuoteMode.MINIMAL)
                        .nullValue("NULL")
                        .build();
        CsvDeserializationSchema deserializationSchema =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .nullFormat("NULL")
                        .build();

        String value = "say \"hi\"\nand bye";
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[] {value, 42, null});

        byte[] serialized = serializationSchema.serialize(seaTunnelRow);
        String serializedField = new String(serialized, StandardCharsets.UTF_8);

        Map<Integer, String> splits =
                deserializationSchema.splitLineBySeaTunnelRowType(serializedField, rowType, 0);
        SeaTunnelRow deserialized = deserializationSchema.getSeaTunnelRow(splits);

        assertEquals(
                value,
                deserialized.getField(0),
                "String field with embedded quotes and newline must survive round trip");
        assertEquals(Integer.valueOf(42), deserialized.getField(1));
        Assertions.assertNull(deserialized.getField(2));
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
    void testInvalidFieldDelimiterFailsFastOnBuild() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        String[] invalidDelimiters = {null, "", "\"", "\r", "\n"};
        for (CsvStringQuoteMode quoteMode :
                new CsvStringQuoteMode[] {CsvStringQuoteMode.MINIMAL, CsvStringQuoteMode.ALL}) {
            for (String delimiter : invalidDelimiters) {
                SeaTunnelCsvFormatException exception =
                        Assertions.assertThrows(
                                SeaTunnelCsvFormatException.class,
                                () ->
                                        CsvSerializationSchema.builder()
                                                .seaTunnelRowType(rowType)
                                                .delimiter(delimiter)
                                                .quoteMode(quoteMode)
                                                .build(),
                                "Building with delimiter ["
                                        + delimiter
                                        + "] and quote mode ["
                                        + quoteMode
                                        + "] must fail fast");
                Assertions.assertTrue(
                        exception.getMessage().contains("field_delimiter"),
                        "Exception must name the field_delimiter option but was: "
                                + exception.getMessage());
            }
        }
    }

    @Test
    void testStringFreeNoneQuotesConstructsAndSerializes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"int_field", "long_field"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.LONG_TYPE});
        CsvSerializationSchema schema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.NONE)
                        .build();

        assertEquals(
                "42,123456789012345",
                new String(
                        schema.serialize(new SeaTunnelRow(new Object[] {42, 123456789012345L}))));
    }

    @Test
    void testNoneQuotesStringFieldRetainsRawIllegalArgumentException() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        CsvSerializationSchema schema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.NONE)
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> schema.serialize(new SeaTunnelRow(new Object[] {"mess,age"})));
        Assertions.assertEquals(IllegalArgumentException.class, exception.getClass());
    }

    @Test
    void testMultiCharDelimiterQuotingAndFullDelimiterRoundTrip() throws IOException {
        String delimiter = "||";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field", "int_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        CsvSerializationSchema schema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .quoteMode(CsvStringQuoteMode.MINIMAL)
                        .build();

        // A value containing only the first character of a multi-character delimiter is
        // over-quoted (a superset of the values containing the full delimiter), never under-quoted.
        assertEquals(
                "\"a|b\"||42",
                new String(schema.serialize(new SeaTunnelRow(new Object[] {"a|b", 42}))));

        // A value containing the literal full delimiter survives a round trip through the CSV
        // reader used for files, which parses with the full delimiter string.
        String value = "a||b";
        byte[] serialized = schema.serialize(new SeaTunnelRow(new Object[] {value, 42}));
        List<String> fields = parseCsvWithFullDelimiter(new String(serialized), delimiter);
        assertEquals(value, fields.get(0));
        assertEquals("42", fields.get(1));
    }

    @Test
    void testMinimalQuotingFollowsConfiguredDelimiterExactOutput() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        CsvSerializationSchema pipeMinimal =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter("|")
                        .quoteMode(CsvStringQuoteMode.MINIMAL)
                        .build();
        // A value that only contains a comma needs no quoting under a pipe delimiter.
        assertEquals(
                "a,b", new String(pipeMinimal.serialize(new SeaTunnelRow(new Object[] {"a,b"}))));
        // A value that contains the configured delimiter is quoted.
        assertEquals(
                "\"a|b\"",
                new String(pipeMinimal.serialize(new SeaTunnelRow(new Object[] {"a|b"}))));

        CsvSerializationSchema commaMinimal =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.MINIMAL)
                        .build();
        // Under the comma delimiter the historical comma quoting is unchanged.
        assertEquals(
                "\"a,b\"",
                new String(commaMinimal.serialize(new SeaTunnelRow(new Object[] {"a,b"}))));

        CsvSerializationSchema pipeAll =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter("|")
                        .quoteMode(CsvStringQuoteMode.ALL)
                        .build();
        // ALL keeps every string field quoted, including the bare-under-MINIMAL comma value.
        assertEquals(
                "\"a,b\"", new String(pipeAll.serialize(new SeaTunnelRow(new Object[] {"a,b"}))));
        assertEquals(
                "\"a|b\"", new String(pipeAll.serialize(new SeaTunnelRow(new Object[] {"a|b"}))));
    }

    @Test
    void testRepeatedSerializeProducesStableOutput() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field", "int_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        CsvSerializationSchema schema =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .quoteMode(CsvStringQuoteMode.ALL)
                        .build();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"a,b", 42});

        assertEquals(
                new String(schema.serialize(row)),
                new String(schema.serialize(row)),
                "Repeated serialization must be stable");
    }

    @Test
    void testSeparatorArrayAndBuilderMutationDoNotAffectBuiltSchema() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"string_field", "int_field"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        String[] callerSeparators = {","};
        CsvSerializationSchema.Builder builder =
                CsvSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .separators(callerSeparators);
        CsvSerializationSchema schema = builder.build();

        // Mutating the caller's array and the builder after build() must not affect the schema.
        callerSeparators[0] = "|";
        builder.delimiter("\t");

        assertEquals(
                "value,42",
                new String(schema.serialize(new SeaTunnelRow(new Object[] {"value", 42}))));
    }

    private static List<String> parseCsvWithFullDelimiter(String line, String delimiter)
            throws IOException {
        try (CSVParser parser =
                CSVParser.parse(
                        line, CSVFormat.DEFAULT.builder().setDelimiter(delimiter).build())) {
            List<String> fields = new ArrayList<>();
            for (CSVRecord record : parser) {
                for (String field : record) {
                    fields.add(field);
                }
            }
            return fields;
        }
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
