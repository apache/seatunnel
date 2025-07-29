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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

@Slf4j
public class ApiKeyEncodedAuthProvider extends AbstractAuthenticationProvider {

    private static final String AUTH_TYPE = "api_key_encoded";
    private static final String API_KEY_HEADER = "Authorization";
    private static final String API_KEY_PREFIX = "ApiKey ";

    @Override
    protected void configureAuthentication(
            HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config) {
        Optional<String> apiKeyEncoded =
                config.getOptional(ElasticsearchBaseOptions.API_KEY_ENCODED);

        if (apiKeyEncoded.isPresent()) {
            log.debug("Configuring encoded API key authentication");

            // Add API key header to all requests
            httpClientBuilder.addInterceptorFirst(
                    (org.apache.http.HttpRequestInterceptor)
                            (request, context) -> {
                                request.setHeader(
                                        API_KEY_HEADER, API_KEY_PREFIX + apiKeyEncoded.get());
                            });

            log.info("Encoded API key authentication configured successfully");
        } else {
            log.debug(
                    "No encoded API key provided, skipping encoded API key authentication configuration");
        }
    }

    @Override
    public String getAuthType() {
        return AUTH_TYPE;
    }

    @Override
    public void validate(ReadonlyConfig config) {
        Optional<String> apiKeyEncoded =
                config.getOptional(ElasticsearchBaseOptions.API_KEY_ENCODED);
        if (!apiKeyEncoded.isPresent()) {
            throw new IllegalArgumentException(
                    "API key authentication with auth_type='api_key_encoded' requires api_key_encoded");
        }
        validateEncodedApiKey(apiKeyEncoded.get());

        log.debug("Encoded API key authentication configuration validated");
    }

    /** Validate encoded API key. */
    private void validateEncodedApiKey(String apiKeyEncoded) {
        if (apiKeyEncoded == null || apiKeyEncoded.trim().isEmpty()) {
            throw new IllegalArgumentException("Encoded API key cannot be null or empty");
        }

        try {
            byte[] decoded = Base64.getDecoder().decode(apiKeyEncoded);
            String decodedStr = new String(decoded, StandardCharsets.UTF_8);

            if (!decodedStr.contains(":")) {
                throw new IllegalArgumentException(
                        "Encoded API key must be Base64 encoded 'id:key' format");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid encoded API key format: " + e.getMessage(), e);
        }
    }
}
