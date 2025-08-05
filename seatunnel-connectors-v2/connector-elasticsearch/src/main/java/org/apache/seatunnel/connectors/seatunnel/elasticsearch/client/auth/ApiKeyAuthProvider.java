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
public class ApiKeyAuthProvider extends AbstractAuthenticationProvider {

    private static final String AUTH_TYPE = "api_key";
    private static final String API_KEY_HEADER = "Authorization";
    private static final String API_KEY_PREFIX = "ApiKey ";

    @Override
    protected void configureAuthentication(
            HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config) {
        String encodedApiKey = getEncodedApiKey(config);

        if (encodedApiKey != null) {
            log.debug("Configuring API key authentication");

            // Add API key header to all requests
            httpClientBuilder.addInterceptorFirst(
                    (org.apache.http.HttpRequestInterceptor)
                            (request, context) -> {
                                request.setHeader(API_KEY_HEADER, API_KEY_PREFIX + encodedApiKey);
                            });

            log.info("API key authentication configured successfully");
        } else {
            log.debug(
                    "No API key credentials provided, skipping API key authentication configuration");
        }
    }

    @Override
    public String getAuthType() {
        return AUTH_TYPE;
    }

    @Override
    public void validate(ReadonlyConfig config) {
        Optional<String> apiKeyId = config.getOptional(ElasticsearchBaseOptions.API_KEY_ID);
        Optional<String> apiKey = config.getOptional(ElasticsearchBaseOptions.API_KEY);
        Optional<String> apiKeyEncoded =
                config.getOptional(ElasticsearchBaseOptions.API_KEY_ENCODED);

        if (!apiKeyId.isPresent() || !apiKey.isPresent()) {
            throw new IllegalArgumentException(
                    "API key authentication with auth_type='api_key' requires both api_key_id and api_key");
        }
        validateApiKeyIdAndSecret(apiKeyId.get(), apiKey.get());

        log.debug("API key authentication configuration validated");
    }

    /**
     * Get the encoded API key from configuration.
     *
     * @param config the configuration
     * @return the Base64 encoded API key, or null if not configured
     */
    private String getEncodedApiKey(ReadonlyConfig config) {
        Optional<String> apiKeyId = config.getOptional(ElasticsearchBaseOptions.API_KEY_ID);
        Optional<String> apiKey = config.getOptional(ElasticsearchBaseOptions.API_KEY);

        if (apiKeyId.isPresent() && apiKey.isPresent()) {
            String credentials = apiKeyId.get() + ":" + apiKey.get();
            return Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        }

        return null;
    }

    /** Validate API key ID and secret. */
    private void validateApiKeyIdAndSecret(String apiKeyId, String apiKey) {
        if (apiKeyId == null || apiKeyId.trim().isEmpty()) {
            throw new IllegalArgumentException("API key ID cannot be null or empty");
        }

        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalArgumentException("API key cannot be null or empty");
        }
    }
}
