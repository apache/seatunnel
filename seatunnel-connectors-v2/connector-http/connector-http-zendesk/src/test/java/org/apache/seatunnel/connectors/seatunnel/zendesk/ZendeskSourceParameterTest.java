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

package org.apache.seatunnel.connectors.seatunnel.zendesk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.zendesk.source.config.ZendeskSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

class ZendeskSourceParameterTest {

    @Test
    void buildWithConfigSetsBasicAuthHeaderAndPreservesExistingHeaders() {
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("X-Custom-Header", "custom-value");

        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://your-subdomain.zendesk.com/api/v2/tickets.json");
        options.put("email", "agent@example.com");
        options.put("api_token", "secret-token");
        options.put("headers", customHeaders);

        ZendeskSourceParameter parameter = new ZendeskSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));

        Map<String, String> headers = parameter.getHeaders();
        String expected =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        "agent@example.com/token:secret-token"
                                                .getBytes(StandardCharsets.UTF_8));
        // Zendesk API-token auth uses HTTP Basic with {email}/token:{api_token}
        Assertions.assertEquals(expected, headers.get("Authorization"));
        // requests ask for a JSON response
        Assertions.assertEquals("application/json", headers.get("Accept"));
        // existing custom headers are preserved
        Assertions.assertEquals("custom-value", headers.get("X-Custom-Header"));
    }
}
