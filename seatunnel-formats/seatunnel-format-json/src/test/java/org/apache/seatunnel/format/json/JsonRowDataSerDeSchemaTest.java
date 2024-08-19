/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.ArrayType.INT_ARRAY_TYPE;
import static org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BYTE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.SHORT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonRowDataSerDeSchemaTest {

    @Test
    public void testSerDe() throws Exception {
        int intValue = 45536;
        float floatValue = 33.333F;
        long longValue = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        LocalDate date = LocalDate.parse("1990-10-14");
        LocalTime time = LocalTime.parse("12:12:43");
        Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
        Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");
        Map<String, Long> map = new HashMap<>();
        map.put("element", 123L);

        Map<String, Integer> multiSet = new HashMap<>();
        multiSet.put("element", 2);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("bool", true);
        root.put("int", intValue);
        root.put("longValue", longValue);
        root.put("float", floatValue);
        root.put("name", name);
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43");
        root.put("timestamp3", "1990-10-14T12:12:43.123");
        root.put("timestamp9", "1990-10-14T12:12:43.123456789");
        root.putObject("map").put("element", 123);
        root.putObject("multiSet").put("element", 2);
        root.putObject("map2map").putObject("inner_map").put("key", 234);
        ObjectNode rowFieldNodes = root.deepCopy();
        rowFieldNodes.put("date", "1990-10-14T12:12:43.123");
        root.putIfAbsent("row", rowFieldNodes);

        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {
                            "bool",
                            "int",
                            "longValue",
                            "float",
                            "name",
                            "date",
                            "time",
                            "timestamp3",
                            "timestamp9",
                            "map",
                            "multiSet",
                            "map2map",
                            "row"
                        },
                        new SeaTunnelDataType[] {
                            BOOLEAN_TYPE,
                            INT_TYPE,
                            LONG_TYPE,
                            FLOAT_TYPE,
                            STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            new MapType(STRING_TYPE, LONG_TYPE),
                            new MapType(STRING_TYPE, INT_TYPE),
                            new MapType(STRING_TYPE, new MapType(STRING_TYPE, INT_TYPE)),
                            new SeaTunnelRowType(
                                    new String[] {
                                        "bool",
                                        "int",
                                        "longValue",
                                        "float",
                                        "name",
                                        "date",
                                        "time",
                                        "timestamp3",
                                        "timestamp9",
                                        "map",
                                        "multiSet",
                                        "map2map"
                                    },
                                    new SeaTunnelDataType[] {
                                        BOOLEAN_TYPE,
                                        INT_TYPE,
                                        LONG_TYPE,
                                        FLOAT_TYPE,
                                        STRING_TYPE,
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        LocalTimeType.LOCAL_TIME_TYPE,
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        new MapType(STRING_TYPE, LONG_TYPE),
                                        new MapType(STRING_TYPE, INT_TYPE),
                                        new MapType(STRING_TYPE, new MapType(STRING_TYPE, INT_TYPE))
                                    })
                        });
        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);
        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTables, false, false);

        SeaTunnelRow expected = new SeaTunnelRow(13);
        expected.setField(0, true);
        expected.setField(1, intValue);
        expected.setField(2, longValue);
        expected.setField(3, floatValue);
        expected.setField(4, name);
        expected.setField(5, date);
        expected.setField(6, time);
        expected.setField(7, timestamp3.toLocalDateTime());
        expected.setField(8, timestamp9.toLocalDateTime());
        expected.setField(9, map);
        expected.setField(10, multiSet);
        expected.setField(11, nestedMap);

        SeaTunnelRow rowFieldRow = new SeaTunnelRow(12);
        rowFieldRow.setField(0, true);
        rowFieldRow.setField(1, intValue);
        rowFieldRow.setField(2, longValue);
        rowFieldRow.setField(3, floatValue);
        rowFieldRow.setField(4, name);
        rowFieldRow.setField(5, timestamp3.toLocalDateTime());
        rowFieldRow.setField(6, time);
        rowFieldRow.setField(7, timestamp3.toLocalDateTime());
        rowFieldRow.setField(8, timestamp9.toLocalDateTime());
        rowFieldRow.setField(9, map);
        rowFieldRow.setField(10, multiSet);
        rowFieldRow.setField(11, nestedMap);

        expected.setField(12, rowFieldRow);

        SeaTunnelRow seaTunnelRow = deserializationSchema.deserialize(serializedJson);
        assertEquals(expected, seaTunnelRow);

        // test serialization
        JsonSerializationSchema serializationSchema = new JsonSerializationSchema(schema);

        byte[] actualBytes = serializationSchema.serialize(seaTunnelRow);
        assertEquals(new String(serializedJson), new String(actualBytes));
    }

    @Test
    public void testSerDeMultiRows() throws Exception {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"f1", "f2", "f3", "f4", "f5", "f6"},
                        new SeaTunnelDataType[] {
                            INT_TYPE,
                            BOOLEAN_TYPE,
                            STRING_TYPE,
                            new MapType(STRING_TYPE, STRING_TYPE),
                            STRING_ARRAY_TYPE,
                            new SeaTunnelRowType(
                                    new String[] {"f1", "f2"},
                                    new SeaTunnelDataType[] {STRING_TYPE, INT_TYPE})
                        });

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTables, false, false);
        JsonSerializationSchema serializationSchema = new JsonSerializationSchema(schema);

        ObjectMapper objectMapper = new ObjectMapper();

        // the first row
        {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("f1", 1);
            root.put("f2", true);
            root.put("f3", "str");
            ObjectNode map = root.putObject("f4");
            map.put("hello1", "flink");
            ArrayNode array = root.putArray("f5");
            array.add("element1");
            array.add("element2");
            ObjectNode row = root.putObject("f6");
            row.put("f1", "this is row1");
            row.put("f2", 12);
            byte[] serializedJson = objectMapper.writeValueAsBytes(root);
            SeaTunnelRow rowData = deserializationSchema.deserialize(serializedJson);
            byte[] actual = serializationSchema.serialize(rowData);
            assertEquals(new String(serializedJson), new String(actual));
        }

        // the second row
        {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("f1", 10);
            root.put("f2", false);
            root.put("f3", "newStr");
            ObjectNode map = root.putObject("f4");
            map.put("hello2", "json");
            ArrayNode array = root.putArray("f5");
            array.add("element3");
            array.add("element4");
            ObjectNode row = root.putObject("f6");
            row.put("f1", "this is row2");
            row.putNull("f2");
            byte[] serializedJson = objectMapper.writeValueAsBytes(root);
            SeaTunnelRow rowData = deserializationSchema.deserialize(serializedJson);
            byte[] actual = serializationSchema.serialize(rowData);
            assertEquals(new String(serializedJson), new String(actual));
        }
    }

    @Test
    public void testSerDeMultiRowsWithNullValues() throws Exception {
        String[] jsons =
                new String[] {
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{\"k1\":10.01,\"k2\":\"invalid\"}}",
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"},"
                            + "\"ids\":[1,2,3]}",
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{}}",
                };

        String[] expected =
                new String[] {
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{\"k1\":10.01,\"k2\":null}}",
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"},"
                            + "\"ids\":[1,2,3],\"metrics\":null}",
                    "{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{}}",
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"svt", "ops", "ids", "metrics"},
                        new SeaTunnelDataType[] {
                            STRING_TYPE,
                            new SeaTunnelRowType(
                                    new String[] {"id"}, new SeaTunnelDataType[] {STRING_TYPE}),
                            INT_ARRAY_TYPE,
                            new MapType(STRING_TYPE, DOUBLE_TYPE)
                        });

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTables, false, true);

        JsonSerializationSchema serializationSchema = new JsonSerializationSchema(rowType);

        for (int i = 0; i < jsons.length; i++) {
            String json = jsons[i];
            SeaTunnelRow row = deserializationSchema.deserialize(json.getBytes());
            String result = new String(serializationSchema.serialize(row));
            assertEquals(expected[i], result);
        }
    }

    @Test
    public void testDeserializationNullRow() throws Exception {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});
        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTables, true, false);
        String s = null;
        assertNull(deserializationSchema.deserialize(s));
    }

    @Test
    public void testDeserializationMissingNode() throws Exception {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});
        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTables, true, false);
        SeaTunnelRow rowData = deserializationSchema.deserialize("".getBytes());
        assertEquals(null, rowData);
    }

    @Test
    public void testDeserializationPassMissingField() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", 123123123);
        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        // pass on missing field
        final JsonDeserializationSchema deser =
                new JsonDeserializationSchema(catalogTables, false, false);

        SeaTunnelRow expected = new SeaTunnelRow(1);
        SeaTunnelRow actual = deser.deserialize(serializedJson);
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializationMissingField() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", 123123123);
        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        // fail on missing field
        final JsonDeserializationSchema deser =
                new JsonDeserializationSchema(catalogTables, true, false);

        SeaTunnelRuntimeException expected =
                CommonError.jsonOperationError("Common", root.toString());
        SeaTunnelRuntimeException actual =
                assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> {
                            deser.deserialize(serializedJson);
                        },
                        "expecting exception message: " + expected.getMessage());
        assertEquals(actual.getMessage(), expected.getMessage());

        SeaTunnelRuntimeException expectedCause =
                CommonError.jsonOperationError("Common", "Field $.name in " + root.toString());
        Throwable cause = actual.getCause();
        assertEquals(cause.getClass(), expectedCause.getClass());
        assertEquals(cause.getMessage(), expectedCause.getMessage());
    }

    @Test
    public void testDeserializationIgnoreParseError() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", 123123123);
        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});
        SeaTunnelRow expected = new SeaTunnelRow(1);
        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        // ignore on parse error
        final JsonDeserializationSchema deser =
                new JsonDeserializationSchema(catalogTables, false, true);
        assertEquals(expected, deser.deserialize(serializedJson));
    }

    @Test
    public void testDeserializationFailOnMissingFieldIgnoreParseError() throws Exception {
        String errorMessage =
                "ErrorCode:[COMMON-06], ErrorDescription:[Illegal argument] - JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.";

        SeaTunnelJsonFormatException actual =
                assertThrows(
                        SeaTunnelJsonFormatException.class,
                        () -> {
                            new JsonDeserializationSchema(null, true, true);
                        },
                        "expecting exception message: " + errorMessage);
        assertEquals(actual.getMessage(), errorMessage);
    }

    @Test
    public void testDeserializationNoJson() throws Exception {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        String noJson = "{]";
        final JsonDeserializationSchema deser =
                new JsonDeserializationSchema(catalogTables, false, false);
        SeaTunnelRuntimeException expected = CommonError.jsonOperationError("Common", noJson);

        SeaTunnelRuntimeException actual =
                assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> {
                            deser.deserialize(noJson);
                        },
                        "expecting exception message: " + expected.getMessage());

        assertEquals(actual.getMessage(), expected.getMessage());

        actual =
                assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> {
                            deser.deserialize(noJson.getBytes());
                        },
                        "expecting exception message: " + expected.getMessage());

        assertEquals(actual.getMessage(), expected.getMessage());
    }

    @Test
    public void testMapConverterKeyType() throws JsonProcessingException {
        MapType<String, String> stringKeyMapType = new MapType<>(STRING_TYPE, STRING_TYPE);
        MapType<Boolean, String> booleanKeyMapType = new MapType<>(BOOLEAN_TYPE, STRING_TYPE);
        MapType<Byte, String> tinyintKeyMapType = new MapType<>(BYTE_TYPE, STRING_TYPE);
        MapType<Short, String> smallintKeyMapType = new MapType<>(SHORT_TYPE, STRING_TYPE);
        MapType<Integer, String> intKeyMapType = new MapType<>(INT_TYPE, STRING_TYPE);
        MapType<Long, String> bigintKeyMapType = new MapType<>(LONG_TYPE, STRING_TYPE);
        MapType<Float, String> floatKeyMapType = new MapType<>(FLOAT_TYPE, STRING_TYPE);
        MapType<Double, String> doubleKeyMapType = new MapType<>(DOUBLE_TYPE, STRING_TYPE);
        MapType<LocalDate, String> dateKeyMapType =
                new MapType<>(LocalTimeType.LOCAL_DATE_TYPE, STRING_TYPE);
        MapType<LocalTime, String> timeKeyMapType =
                new MapType<>(LocalTimeType.LOCAL_TIME_TYPE, STRING_TYPE);
        MapType<LocalDateTime, String> timestampKeyMapType =
                new MapType<>(LocalTimeType.LOCAL_DATE_TIME_TYPE, STRING_TYPE);
        MapType<BigDecimal, String> decimalKeyMapType =
                new MapType<>(new DecimalType(10, 2), STRING_TYPE);

        JsonToRowConverters converters = new JsonToRowConverters(true, false);

        JsonToRowConverters.JsonToObjectConverter stringConverter =
                converters.createConverter(stringKeyMapType);
        JsonToRowConverters.JsonToObjectConverter booleanConverter =
                converters.createConverter(booleanKeyMapType);
        JsonToRowConverters.JsonToObjectConverter tinyintConverter =
                converters.createConverter(tinyintKeyMapType);
        JsonToRowConverters.JsonToObjectConverter smallintConverter =
                converters.createConverter(smallintKeyMapType);
        JsonToRowConverters.JsonToObjectConverter intConverter =
                converters.createConverter(intKeyMapType);
        JsonToRowConverters.JsonToObjectConverter bigintConverter =
                converters.createConverter(bigintKeyMapType);
        JsonToRowConverters.JsonToObjectConverter floatConverter =
                converters.createConverter(floatKeyMapType);
        JsonToRowConverters.JsonToObjectConverter doubleConverter =
                converters.createConverter(doubleKeyMapType);
        JsonToRowConverters.JsonToObjectConverter dateConverter =
                converters.createConverter(dateKeyMapType);
        JsonToRowConverters.JsonToObjectConverter timeConverter =
                converters.createConverter(timeKeyMapType);
        JsonToRowConverters.JsonToObjectConverter timestampConverter =
                converters.createConverter(timestampKeyMapType);
        JsonToRowConverters.JsonToObjectConverter decimalConverter =
                converters.createConverter(decimalKeyMapType);

        assertMapKeyType("{\"abc\": \"xxx\"}", stringConverter, "abc", "stringConverter");
        assertMapKeyType("{\"false\": \"xxx\"}", booleanConverter, false, "booleanConverter");
        assertMapKeyType("{\"1\": \"xxx\"}", tinyintConverter, (byte) 1, "tinyintConverter");
        assertMapKeyType("{\"12\": \"xxx\"}", smallintConverter, (short) 12, "smallintConverter");
        assertMapKeyType("{\"123\": \"xxx\"}", intConverter, 123, "intConverter");
        assertMapKeyType("{\"12345\": \"xxx\"}", bigintConverter, 12345L, "bigintConverter");
        assertMapKeyType("{\"1.0001\": \"xxx\"}", floatConverter, 1.0001f, "floatConverter");
        assertMapKeyType("{\"999.9999\": \"xxx\"}", doubleConverter, 999.9999, "doubleConverter");
        assertMapKeyType(
                "{\"9999.23\": \"xxx\"}",
                decimalConverter,
                BigDecimal.valueOf(9999.23),
                "decimalConverter");

        LocalDate date =
                DateTimeFormatter.ISO_LOCAL_DATE
                        .parse("2024-01-26")
                        .query(TemporalQueries.localDate());
        assertMapKeyType(
                "{\"2024-01-26\": \"xxx\"}", dateConverter, date, "iso_local_date_string_map");

        LocalTime time =
                JsonToRowConverters.TIME_FORMAT
                        .parse("12:00:12.001")
                        .query(TemporalQueries.localTime());
        assertMapKeyType(
                "{\"12:00:12.001\": \"xxx\"}", timeConverter, time, "time_format_string_map");

        LocalDateTime timestamp = LocalDateTime.of(date, time);
        assertMapKeyType(
                "{\"2024-01-26T12:00:12.001\": \"xxx\"}",
                timestampConverter,
                timestamp,
                "timestamp_string_map");
    }

    private void assertMapKeyType(
            String payload,
            JsonToRowConverters.JsonToObjectConverter converter,
            Object expect,
            String fieldName)
            throws JsonProcessingException {
        JsonNode keyMapNode = JsonUtils.stringToJsonNode(payload);
        Map<?, ?> keyMap = (Map<?, ?>) converter.convert(keyMapNode, fieldName);
        assertEquals(expect, keyMap.keySet().iterator().next());
    }

    @Test
    public void testParseUnsupportedDateTimeFormat() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"date_field"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.LOCAL_DATE_TYPE});
        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(false, false, rowType);
        String content = "{\"date_field\":\"2022-092-24\"}";
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> deserializationSchema.deserialize(content.getBytes()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-32], ErrorDescription:[The date format '2022-092-24' of field 'date_field' is not supported. Please check the date format.]",
                exception.getCause().getCause().getMessage());

        SeaTunnelRowType rowType2 =
                new SeaTunnelRowType(
                        new String[] {"timestamp_field"},
                        new SeaTunnelDataType<?>[] {
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        });
        JsonDeserializationSchema deserializationSchema2 =
                new JsonDeserializationSchema(false, false, rowType2);
        String content2 = "{\"timestamp_field\": \"2022-09-24-22:45:00\"}";
        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> deserializationSchema2.deserialize(content2.getBytes()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-33], ErrorDescription:[The datetime format '2022-09-24-22:45:00' of field 'timestamp_field' is not supported. Please check the datetime format.]",
                exception2.getCause().getCause().getMessage());
    }

    @Test
    public void testSerializationWithNullValue() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {
                            "bool", "int", "longValue", "float", "name", "date", "time", "timestamp"
                        },
                        new SeaTunnelDataType[] {
                            BOOLEAN_TYPE,
                            INT_TYPE,
                            LONG_TYPE,
                            FLOAT_TYPE,
                            STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        Object[] fields = new Object[] {null, null, null, null, null, null, null, null};
        SeaTunnelRow expected = new SeaTunnelRow(fields);
        assertEquals(
                "{\"bool\":\"\\\\N\",\"int\":\"\\\\N\",\"longValue\":\"\\\\N\",\"float\":\"\\\\N\",\"name\":\"\\\\N\",\"date\":\"\\\\N\",\"time\":\"\\\\N\",\"timestamp\":\"\\\\N\"}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(expected)));
    }

    @Test
    public void testSerializationWithMapHasNonStringKey() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"mapii", "mapbb"},
                        new SeaTunnelDataType[] {
                            new MapType(INT_TYPE, INT_TYPE), new MapType(BOOLEAN_TYPE, INT_TYPE)
                        });
        Map<Integer, Integer> mapII = new HashMap<>();
        mapII.put(1, 2);

        Map<Boolean, Integer> mapBI = new HashMap<>();
        mapBI.put(true, 3);

        Object[] fields = new Object[] {mapII, mapBI};
        SeaTunnelRow expected = new SeaTunnelRow(fields);
        assertEquals(
                "{\"mapii\":{\"1\":2},\"mapbb\":{\"true\":3}}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(expected)));
    }

    @Test
    public void testSerializationWithTimestamp() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"timestamp"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        LocalDateTime timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 123456000);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "{\"timestamp\":\"2022-09-24T22:45:00.123456\"}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 0);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "{\"timestamp\":\"2022-09-24T22:45:00\"}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 1000);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "{\"timestamp\":\"2022-09-24T22:45:00.000001\"}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(row)));

        timestamp = LocalDateTime.of(2022, 9, 24, 22, 45, 0, 123456);
        row = new SeaTunnelRow(new Object[] {timestamp});
        assertEquals(
                "{\"timestamp\":\"2022-09-24T22:45:00.000123456\"}",
                new String(new JsonSerializationSchema(schema, "\\N").serialize(row)));
    }
}
