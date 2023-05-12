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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.ArrayType.INT_ARRAY_TYPE;
import static org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

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
                            "map2map"
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
                            new MapType(STRING_TYPE, new MapType(STRING_TYPE, INT_TYPE))
                        });

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(false, false, schema);

        SeaTunnelRow expected = new SeaTunnelRow(12);
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

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(false, false, schema);
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

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(false, true, rowType);
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
        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(true, false, schema);
        String s = null;
        assertNull(deserializationSchema.deserialize(s));
    }

    @Test
    public void testDeserializationMissingNode() throws Exception {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(true, false, schema);
        SeaTunnelRow rowData = deserializationSchema.deserialize("".getBytes());
        assertEquals(null, rowData);
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

        // pass on missing field
        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(false, false, schema);

        SeaTunnelRow expected = new SeaTunnelRow(1);
        SeaTunnelRow actual = deserializationSchema.deserialize(serializedJson);
        assertEquals(expected, actual);

        // fail on missing field
        deserializationSchema = new JsonDeserializationSchema(true, false, schema);

        String errorMessage =
                "ErrorCode:[COMMON-02], ErrorDescription:[Json covert/parse operation failed] - Failed to deserialize JSON '{\"id\":123123123}'.";
        try {
            deserializationSchema.deserialize(serializedJson);
            fail("expecting exception message: " + errorMessage);
        } catch (Throwable t) {
            assertEquals(errorMessage, t.getMessage());
        }

        // ignore on parse error
        deserializationSchema = new JsonDeserializationSchema(false, true, schema);
        assertEquals(expected, deserializationSchema.deserialize(serializedJson));

        errorMessage =
                "ErrorCode:[COMMON-06], ErrorDescription:[Illegal argument] - JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.";
        try {
            // failOnMissingField and ignoreParseErrors both enabled
            new JsonDeserializationSchema(true, true, schema);
            Assertions.fail("expecting exception message: " + errorMessage);
        } catch (Throwable t) {
            assertEquals(errorMessage, t.getMessage());
        }
    }
}
