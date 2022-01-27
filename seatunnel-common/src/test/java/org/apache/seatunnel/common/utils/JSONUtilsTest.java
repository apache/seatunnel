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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.config.CheckResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonUtilsTest {

    @Test
    public void createArrayNodeTest() {
        CheckResult obj = CheckResult.error("my test");
        String result = "[{\"success\":false,\"msg\":\"my test\"},{\"success\":false,\"msg\":\"my test\"}]";
        JsonNode jsonNode = JsonUtils.toJsonNode(obj);

        ArrayNode arrayNode = JsonUtils.createArrayNode();
        ArrayList<JsonNode> objects = new ArrayList<>();
        objects.add(jsonNode);
        objects.add(jsonNode);

        ArrayNode jsonNodes = arrayNode.addAll(objects);
        String s = JsonUtils.toJsonString(jsonNodes);
        Assert.assertEquals(s, result);

    }

    @Test
    public void toJsonNodeTest() {
        CheckResult obj = CheckResult.error("my test");
        String str = "{\"success\":false,\"msg\":\"my test\"}";

        JsonNode jsonNodes = JsonUtils.toJsonNode(obj);
        String s = JsonUtils.toJsonString(jsonNodes);
        Assert.assertEquals(s, str);

    }

    @Test
    public void createObjectNodeTest() {
        String jsonStr = "{\"a\":\"b\",\"b\":\"d\"}";

        ObjectNode objectNode = JsonUtils.createObjectNode();
        objectNode.put("a", "b");
        objectNode.put("b", "d");
        String s = JsonUtils.toJsonString(objectNode);
        Assert.assertEquals(s, jsonStr);
    }

    @Test
    public void toMap() {

        String jsonStr = "{\"id\":\"1001\",\"name\":\"Jobs\"}";

        Map<String, String> models = JsonUtils.toMap(jsonStr);
        Assert.assertEquals("1001", models.get("id"));
        Assert.assertEquals("Jobs", models.get("name"));

    }

    @Test
    public void string2MapTest() {
        String str = list2String();

        List<LinkedHashMap> maps = JsonUtils.toList(str, LinkedHashMap.class);

        Assert.assertEquals(1, maps.size());
        Assert.assertEquals("mysql200", maps.get(0).get("mysql service name"));
        Assert.assertEquals("192.168.xx.xx", maps.get(0).get("mysql address"));
        Assert.assertEquals("3306", maps.get(0).get("port"));
        Assert.assertEquals("80", maps.get(0).get("no index of number"));
        Assert.assertEquals("190", maps.get(0).get("database client connections"));
    }

    public String list2String() {

        LinkedHashMap<String, String> map1 = new LinkedHashMap<>();
        map1.put("mysql service name", "mysql200");
        map1.put("mysql address", "192.168.xx.xx");
        map1.put("port", "3306");
        map1.put("no index of number", "80");
        map1.put("database client connections", "190");

        List<LinkedHashMap<String, String>> maps = new ArrayList<>();
        maps.add(0, map1);
        String resultJson = JsonUtils.toJsonString(maps);
        return resultJson;
    }

    @Test
    public void testParseObject() {
        Assert.assertNull(JsonUtils.parseObject(""));
        Assert.assertNull(JsonUtils.parseObject("foo", String.class));
    }

    @Test
    public void testNodeString() {
        Assert.assertEquals("", JsonUtils.getNodeString("", "key"));
        Assert.assertEquals("", JsonUtils.getNodeString("abc", "key"));
        Assert.assertEquals("", JsonUtils.getNodeString("{\"bar\":\"foo\"}", "key"));
        Assert.assertEquals("foo", JsonUtils.getNodeString("{\"bar\":\"foo\"}", "bar"));
        Assert.assertEquals("[1,2,3]", JsonUtils.getNodeString("{\"bar\": [1,2,3]}", "bar"));
        Assert.assertEquals("{\"1\":\"2\",\"2\":3}", JsonUtils.getNodeString("{\"bar\": {\"1\":\"2\",\"2\":3}}", "bar"));
    }

    @Test
    public void testJsonByteArray() {
        String str = "foo";
        byte[] serializeByte = JsonUtils.toJsonByteArray(str);
        String deserialize = JsonUtils.parseObject(serializeByte, String.class);
        Assert.assertEquals(str, deserialize);
        str = null;
        serializeByte = JsonUtils.toJsonByteArray(str);
        deserialize = JsonUtils.parseObject(serializeByte, String.class);
        Assert.assertNull(deserialize);
    }

    @Test
    public void testToList() {
        Assert.assertEquals(new ArrayList(),
                JsonUtils.toList("A1B2C3", null));
        Assert.assertEquals(new ArrayList(),
                JsonUtils.toList("", null));
    }

    @Test
    public void testCheckJsonValid() {
        Assert.assertTrue(JsonUtils.checkJsonValid("3"));
        Assert.assertFalse(JsonUtils.checkJsonValid(""));
    }

    @Test
    public void testFindValue() {
        Assert.assertNull(JsonUtils.findValue(
                new ArrayNode(new JsonNodeFactory(true)), null));
    }

    @Test
    public void testToMap() {
        Map<String, String> map = new HashMap<>();
        map.put("foo", "bar");

        Assert.assertTrue(map.equals(JsonUtils.toMap(
                "{\n" + "\"foo\": \"bar\"\n" + "}")));

        Assert.assertFalse(map.equals(JsonUtils.toMap(
                "{\n" + "\"bar\": \"foo\"\n" + "}")));

        Assert.assertNull(JsonUtils.toMap("3"));
        Assert.assertNull(JsonUtils.toMap(null));

        String str = "{\"resourceList\":[],\"localParams\":[],\"rawScript\":\"#!/bin/bash\\necho \\\"shell-1\\\"\"}";
        Map<String, String> m = JsonUtils.toMap(str);
        Assert.assertNotNull(m);
    }

    @Test
    public void testToJsonString() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", "bar");

        Assert.assertEquals("{\"foo\":\"bar\"}",
                JsonUtils.toJsonString(map));
        Assert.assertEquals(String.valueOf((Object) null),
                JsonUtils.toJsonString(null));

        Assert.assertEquals("{\"foo\":\"bar\"}",
                JsonUtils.toJsonString(map, SerializationFeature.WRITE_NULL_MAP_VALUES));
    }

    @Test
    public void parseObject() {
        String str = "{\"color\":\"yellow\",\"type\":\"renault\"}";
        ObjectNode node = JsonUtils.parseObject(str);

        Assert.assertEquals("yellow", node.path("color").asText());
        node.put("price", "100");
        Assert.assertEquals("100", node.path("price").asText());

        node.put("color", "red");
        Assert.assertEquals("red", node.path("color").asText());
    }

    @Test
    public void parseArray() {
        String str = "[{\"color\":\"yellow\",\"type\":\"renault\"}]";
        ArrayNode node = JsonUtils.parseArray(str);

        Assert.assertEquals("yellow", node.path(0).path("color").asText());
    }
}
