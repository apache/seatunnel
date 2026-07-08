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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class JsonUtilsTest {

    private static final String CHINESE_JSON =
            "{\"company_name\":\"乐\",\"industry\":\"互联网\",\"status\":\"在营\"}";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Simulate the old broken code path inside parseObject(String): text.getBytes() on a GBK
     * platform produces GBK bytes, then OBJECT_MAPPER.readTree(gbkBytes) fails with "Invalid UTF-8
     * middle byte".
     */
    @Test
    public void testOldGetBytesWithGBKFails() throws Exception {
        // Simulate: String json = text.getBytes() where platform default is GBK
        byte[] gbkBytes = CHINESE_JSON.getBytes(Charset.forName("GBK"));

        // This is what parseObject(byte[]) does internally: OBJECT_MAPPER.readTree(content)
        Assertions.assertThrows(
                Exception.class,
                () -> OBJECT_MAPPER.readTree(gbkBytes),
                "GBK bytes should fail when Jackson parses as UTF-8, simulating the old bug");
    }

    /**
     * Verify the fixed code path: text.getBytes(StandardCharsets.UTF_8) always produces valid UTF-8
     * bytes, regardless of platform default charset.
     */
    @Test
    public void testFixedGetBytesWithUTF8Succeeds() throws Exception {
        // This is the fix: text.getBytes(StandardCharsets.UTF_8)
        byte[] utf8Bytes = CHINESE_JSON.getBytes(StandardCharsets.UTF_8);

        // OBJECT_MAPPER.readTree(utf8Bytes) parses as UTF-8 → succeeds
        JsonNode node = OBJECT_MAPPER.readTree(utf8Bytes);
        Assertions.assertEquals("乐", node.get("company_name").asText());
        Assertions.assertEquals("互联网", node.get("industry").asText());
        Assertions.assertEquals("在营", node.get("status").asText());
    }

    /** End-to-end test: JsonUtils.parseObject(String) with Chinese characters works correctly. */
    @Test
    public void testParseObjectWithChineseCharacters() {
        ObjectNode result = JsonUtils.parseObject(CHINESE_JSON);
        Assertions.assertEquals("乐", result.get("company_name").asText());
        Assertions.assertEquals("互联网", result.get("industry").asText());
        Assertions.assertEquals("在营", result.get("status").asText());
    }
}
