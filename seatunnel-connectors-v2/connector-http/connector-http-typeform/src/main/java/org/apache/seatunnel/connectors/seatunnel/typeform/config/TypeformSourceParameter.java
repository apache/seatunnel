/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.util.HashMap;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Builds the HTTP request parameters for the Typeform responses endpoint: the responses URL for one
 * form, the Bearer Authorization header, the page size, the optional filters and the retry
 * defaults.
 */
public class TypeformSourceParameter extends HttpParameter {

    private static final int MAX_PAGE_SIZE = 1000;
    private static final Pattern FORM_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

    private static String requireNonBlank(String value, String optionKey) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Typeform option '%s' must not be blank.", optionKey));
        }
        return value.trim();
    }

    /**
     * Builds the request for {@code GET {base_url}/forms/{form_id}/responses}. Validates that
     * api_key, form_id and base_url are not blank, that form_id contains only letters, digits, '_'
     * and '-', that page_size is within 1..1000 and that the retry backoff values are not negative.
     * Passes since, until and response_type to Typeform only when they are set. Retry defaults to 3
     * only when the user did not set {@code retry}. Warns when the bearer token would be sent over
     * a non-HTTPS base_url.
     */
    public void buildWithConfig(ReadonlyConfig config) {
        super.buildWithConfig(config);
        String apiKey = requireNonBlank(config.get(TypeformSourceOptions.API_KEY), "api_key");
        String formId = requireNonBlank(config.get(TypeformSourceOptions.FORM_ID), "form_id");
        if (!FORM_ID_PATTERN.matcher(formId).matches()) {
            throw new IllegalArgumentException(
                    "Typeform option 'form_id' may only contain letters, digits, '_' and '-'.");
        }
        int pageSize = config.get(TypeformSourceOptions.PAGE_SIZE);
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    String.format(
                            "Typeform option 'page_size' must be between 1 and %d.",
                            MAX_PAGE_SIZE));
        }

        String base = requireNonBlank(config.get(TypeformSourceOptions.BASE_URL), "base_url");
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        setUrl(base + "/forms/" + formId + "/responses");
        setMethod(HttpRequestMethod.GET);

        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put("Authorization", "Bearer " + apiKey);
        headers.put("Accept", "application/json");
        // The base class runs this check before the URL and Authorization header exist, so run it
        // again now that both are set.
        validateCredentialScheme();

        params = new HashMap<>();
        params.put("page_size", String.valueOf(pageSize));
        putIfSet(config, TypeformSourceOptions.SINCE);
        putIfSet(config, TypeformSourceOptions.UNTIL);
        putIfSet(config, TypeformSourceOptions.RESPONSE_TYPE);
        setKeepPageParamAsHttpParam(true);
        setJsonFiledMissedReturnNull(true);

        if (!config.getOptional(TypeformSourceOptions.RETRY).isPresent()) {
            setRetry(3);
        }
        setRetryBackoffMultiplierMillis(
                config.get(TypeformSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        setRetryBackoffMaxMillis(config.get(TypeformSourceOptions.RETRY_BACKOFF_MAX_MS));

        if (getRetryBackoffMultiplierMillis() < 0 || getRetryBackoffMaxMillis() < 0) {
            throw new IllegalArgumentException(
                    "Typeform retry backoff values must not be negative.");
        }
    }

    /** Sends an optional filter to Typeform under its own option name, only when it is set. */
    private void putIfSet(ReadonlyConfig config, Option<String> option) {
        Optional<String> value = config.getOptional(option);
        if (value.isPresent() && !value.get().trim().isEmpty()) {
            params.put(option.key(), value.get().trim());
        }
    }
}
