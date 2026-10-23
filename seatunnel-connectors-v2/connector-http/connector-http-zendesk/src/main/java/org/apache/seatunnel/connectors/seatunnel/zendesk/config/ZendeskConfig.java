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

package org.apache.seatunnel.connectors.seatunnel.zendesk.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ZendeskConfig extends HttpCommonOptions {

    public static final String AUTHORIZATION = "Authorization";
    public static final String BASIC = "Basic ";
    public static final String ACCEPT = "Accept";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";

    public static final Option<String> EMAIL =
            Options.key("email")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Zendesk account email used for API token authentication");

    public static final Option<String> API_TOKEN =
            Options.key("api_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Zendesk API token");

    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Minimum interval in milliseconds between Zendesk API requests, must be >= 0.");

    public static final Option<Integer> RATE_LIMIT_BACKOFF_MS =
            Options.key("rate_limit_backoff_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Base backoff time in milliseconds when Zendesk returns 429, must be >= 0.");

    public static final Option<Integer> RATE_LIMIT_MAX_RETRIES =
            Options.key("rate_limit_max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum retries after receiving Zendesk 429 responses, must be >= 0.");

    public static Map<String, String> buildAuthHeaders(
            String email, String apiToken, Map<String, String> existingHeaders) {
        Map<String, String> headers =
                Optional.ofNullable(existingHeaders).map(HashMap::new).orElse(new HashMap<>());
        String credentials = email + "/token:" + apiToken;
        String encoded =
                Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        headers.put(AUTHORIZATION, BASIC + encoded);
        headers.put(ACCEPT, APPLICATION_JSON);
        headers.put(CONTENT_TYPE, APPLICATION_JSON);
        return headers;
    }
}
