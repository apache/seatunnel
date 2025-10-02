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
package org.apache.seatunnel.connectors.seatunnel.http.client;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class HttpClientProviderTest {

    @Test
    void testAddDefaultJsonContentTypeWhenNotPresent() throws Exception {
        HttpPost mockRequest = new HttpPost("http://localhost:8080");
        Map<String, Object> body = new HashMap<>();
        body.put("key", "value");

        HttpClientProvider.addBody(mockRequest, body);

        // case 1: user not define content-type, use default content type
        assertNotNull(mockRequest.getFirstHeader("Content-Type"));
        Assertions.assertEquals(
                "application/json", mockRequest.getFirstHeader("Content-Type").getValue());
    }

    @Test
    void testPreserveExistingContentType() throws Exception {
        HttpPost mockRequest = new HttpPost("http://localhost:8080");
        mockRequest.addHeader(new BasicHeader("Content-Type", "text/plain"));

        Map<String, Object> body = new HashMap<>();
        body.put("key", "value");

        HttpClientProvider.addBody(mockRequest, body);

        // case 2: if user define content-type, set it
        assertNotNull(mockRequest.getFirstHeader("Content-Type"));
        Assertions.assertEquals(
                "text/plain", mockRequest.getFirstHeader("Content-Type").getValue());
    }

    @Test
    void addBody() throws Exception {
        HttpPost post = new HttpPost("http://localhost:8080");
        Map<String, Object> body = new HashMap<>();
        Header[] originalHeaders = post.getAllHeaders();
        HttpClientProvider.addBody(post, body);

        // ensure the original headers are preserved
        Header[] currentHeaders = post.getAllHeaders();
        Assertions.assertEquals(0, originalHeaders.length);
        Assertions.assertEquals(1, currentHeaders.length);
        for (int i = 0; i < originalHeaders.length; i++) {
            Assertions.assertEquals(
                    originalHeaders[i].getName(),
                    currentHeaders[i].getName(),
                    "Header name mismatch at index " + i);
            Assertions.assertEquals(
                    originalHeaders[i].getValue(),
                    currentHeaders[i].getValue(),
                    "Header value mismatch at index " + i);
        }
        // ensure no manually set content type or encoding
        Assertions.assertNull(post.getEntity().getContentEncoding());
    }

    @Test
    void testFixedBodyParsingPreservesNestedJsonStructure() throws Exception {
        // Given: a nested JSON body with object, array, and primitive
        String body =
                "{\n"
                        + "          \"user\": {\n"
                        + "            \"name\": \"Alice\",\n"
                        + "            \"age\": 30,\n"
                        + "            \"address\": {\n"
                        + "              \"city\": \"Beijing\",\n"
                        + "              \"country\": \"China\"\n"
                        + "            }\n"
                        + "          },\n"
                        + "          \"active\": true,\n"
                        + "          \"scores\": [95, 87, 92]\n"
                        + "        }";

        ;

        // When: parsing using the FIXED logic
        Method parseMethod =
                HttpClientProvider.class.getDeclaredMethod("parseBodyToMap", String.class);
        parseMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) parseMethod.invoke(null, body);

        // Then: nested structure is fully preserved
        Assertions.assertTrue(result.containsKey("user"));
        Assertions.assertTrue(result.containsKey("active"));
        Assertions.assertTrue(result.containsKey("scores"));

        // Ensure NO flattened keys exist
        Assertions.assertFalse(result.containsKey("user.name"));
        Assertions.assertFalse(result.containsKey("user.age"));
        Assertions.assertFalse(result.containsKey("user.address.city"));

        // Validate nested objects
        @SuppressWarnings("unchecked")
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        Assertions.assertEquals("Alice", user.get("name"));
        Assertions.assertEquals(30, user.get("age"));

        @SuppressWarnings("unchecked")
        Map<String, Object> address = (Map<String, Object>) user.get("address");
        Assertions.assertEquals("Beijing", address.get("city"));
        Assertions.assertEquals("China", address.get("country"));

        // Validate array
        @SuppressWarnings("unchecked")
        java.util.List<Integer> scores = (java.util.List<Integer>) result.get("scores");
        Assertions.assertEquals(java.util.Arrays.asList(95, 87, 92), scores);

        // Validate primitive
        Assertions.assertEquals(true, result.get("active"));
    }
}
