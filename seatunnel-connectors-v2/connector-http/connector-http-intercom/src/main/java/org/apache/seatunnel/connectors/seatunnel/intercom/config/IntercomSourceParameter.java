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

package org.apache.seatunnel.connectors.seatunnel.intercom.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.util.HashMap;

/**
 * Builds the HTTP request parameters for the Intercom contacts endpoint: the contacts URL, the
 * Bearer Authorization and Intercom-Version headers, the page size and retry defaults. Validates
 * that api_key and api_version are not blank and that per_page is within 1..150.
 */
public class IntercomSourceParameter extends HttpParameter {

    private static final int MAX_PER_PAGE = 150;

    private static String requireNonBlank(String value, String optionKey) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Intercom option '%s' must not be blank.", optionKey));
        }
        return value.trim();
    }

    /**
     * Builds the request for {@code GET {base_url}/contacts}. Validates that api_key, api_version
     * and base_url are not blank, that per_page is within 1..150 and that the retry backoff values
     * are not negative. Retry defaults to 3 only when the user did not set {@code retry}. Warns
     * when the bearer token would be sent over a non-HTTPS base_url.
     */
    public void buildWithConfig(ReadonlyConfig config) {
        super.buildWithConfig(config);
        String apiKey = requireNonBlank(config.get(IntercomSourceOptions.API_KEY), "api_key");
        String apiVersion =
                requireNonBlank(config.get(IntercomSourceOptions.API_VERSION), "api_version");
        int perPage = config.get(IntercomSourceOptions.PER_PAGE);
        if (perPage < 1 || perPage > MAX_PER_PAGE) {
            throw new IllegalArgumentException(
                    String.format(
                            "Intercom option 'per_page' must be between 1 and %d.", MAX_PER_PAGE));
        }

        String base = requireNonBlank(config.get(IntercomSourceOptions.BASE_URL), "base_url");
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        setUrl(base + "/contacts");
        setMethod(HttpRequestMethod.GET);

        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put("Authorization", "Bearer " + apiKey);
        headers.put("Intercom-Version", apiVersion);
        headers.put("Accept", "application/json");
        // The base class runs this check before the URL and Authorization header exist, so run it
        // again now that both are set.
        validateCredentialScheme();

        params = new HashMap<>();
        params.put("per_page", String.valueOf(perPage));
        setKeepPageParamAsHttpParam(true);
        setJsonFiledMissedReturnNull(true);

        if (!config.getOptional(IntercomSourceOptions.RETRY).isPresent()) {
            setRetry(3);
        }
        setRetryBackoffMultiplierMillis(
                config.get(IntercomSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        setRetryBackoffMaxMillis(config.get(IntercomSourceOptions.RETRY_BACKOFF_MAX_MS));

        if (getRetryBackoffMultiplierMillis() < 0 || getRetryBackoffMaxMillis() < 0) {
            throw new IllegalArgumentException(
                    "Intercom retry backoff values must not be negative.");
        }
    }
}
