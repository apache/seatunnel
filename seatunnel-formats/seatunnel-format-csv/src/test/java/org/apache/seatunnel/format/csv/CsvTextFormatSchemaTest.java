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
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
}
