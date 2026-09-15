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

package org.apache.seatunnel.connectors.seatunnel.splunk;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSourceParameter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SplunkSourceReaderTest {

    @Test
    public void testExactlyTwoRowsWithPreviewDeduplication() throws Exception {
        String ndjson =
                "{\"preview\":true,\"offset\":0,\"result\":{\"a\":\"1\"}}\n"
                        + "{\"preview\":false,\"offset\":0,\"result\":{\"a\":\"1\"}}\n"
                        + "{\"preview\":false,\"offset\":1,\"result\":{\"a\":\"2\"}}";

        String filtered = SplunkSourceReader.filterAndUnwrapNdjson(ndjson);
        String[] rows = filtered.split("\n");

        assertEquals(2, rows.length);
        assertTrue(rows[0].contains("\"a\":\"1\""));
        assertTrue(rows[1].contains("\"a\":\"2\""));
    }

    @Test
    public void testFailsFastWhenResponseExceedsMaxSize() throws Exception {
        MockWebServer server = new MockWebServer();
        server.start();
        server.enqueue(new okhttp3.mockwebserver.MockResponse().setBody("exceeds-limit"));

        java.util.HashMap<String, Object> configMap = new java.util.HashMap<>();
        configMap.put("url", server.url("/").toString());
        configMap.put("api_key", "Splunk test-key");
        configMap.put("max_response_size_bytes", 5L); // Tiny limit

        org.apache.seatunnel.api.configuration.ReadonlyConfig config =
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(configMap);
        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, "Splunk test-key");

        SplunkSourceReader reader = new SplunkSourceReader(
                parameter, null, null, null, null);

        org.junit.jupiter.api.Assertions.assertThrows(
                HttpConnectorException.class, () -> reader.executeRequest());

        server.shutdown();
    }
}
