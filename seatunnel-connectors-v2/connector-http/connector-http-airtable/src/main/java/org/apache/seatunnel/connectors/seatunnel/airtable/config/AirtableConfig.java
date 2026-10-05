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

package org.apache.seatunnel.connectors.seatunnel.airtable.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AirtableConfig extends HttpCommonOptions {

    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";

    public static final String DEFAULT_API_BASE_URL = "https://api.airtable.com";

    private static final String API_VERSION_PATH = "/v0";

    public static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Airtable API base URL, default is https://api.airtable.com");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("api_key")
                    .withDescription("Airtable personal access token");

    public static final Option<String> BASE_ID =
            Options.key("base_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Airtable base ID");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Airtable table name or table ID");

    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(220)
                    .withDescription(
                            "Minimum interval in milliseconds between Airtable API requests, must be >= 0.");

    public static final Option<Integer> RATE_LIMIT_BACKOFF_MS =
            Options.key("rate_limit_backoff_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Base backoff time in milliseconds when Airtable returns 429, must be >= 0.");

    public static final Option<Integer> RATE_LIMIT_MAX_RETRIES =
            Options.key("rate_limit_max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum retries after receiving Airtable 429 responses, must be >= 0.");

    public static String buildBaseUrl(String apiBaseUrl, String baseId, String table) {
        String normalized =
                apiBaseUrl.endsWith("/")
                        ? apiBaseUrl.substring(0, apiBaseUrl.length() - 1)
                        : apiBaseUrl;
        if (!normalized.endsWith(API_VERSION_PATH)) {
            normalized = normalized + API_VERSION_PATH;
        }
        return normalized + "/" + baseId + "/" + encodePathSegment(table);
    }

    public static String encodePathSegment(String value) {
        try {
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8.name());
            return encoded.replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding is not supported", e);
        }
    }

    public static Map<String, String> buildAuthHeaders(
            String token, Map<String, String> existingHeaders) {
        Map<String, String> headers =
                Optional.ofNullable(existingHeaders).map(HashMap::new).orElse(new HashMap<>());
        headers.put(AUTHORIZATION, BEARER + " " + token);
        headers.put(CONTENT_TYPE, APPLICATION_JSON);
        return headers;
    }
}
