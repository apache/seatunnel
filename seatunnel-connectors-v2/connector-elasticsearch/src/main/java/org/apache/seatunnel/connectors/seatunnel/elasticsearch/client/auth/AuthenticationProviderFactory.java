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
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.AuthTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode.UNSUPPORTED_AUTH_TYPE;

@Slf4j
public class AuthenticationProviderFactory {

    private static final AuthTypeEnum DEFAULT_AUTH_TYPE = AuthTypeEnum.BASIC;

    private static final Map<AuthTypeEnum, Class<? extends AuthenticationProvider>>
            PROVIDER_REGISTRY = new HashMap<>();

    static {
        // Register built-in authentication providers
        PROVIDER_REGISTRY.put(AuthTypeEnum.BASIC, BasicAuthProvider.class);
        PROVIDER_REGISTRY.put(AuthTypeEnum.API_KEY, ApiKeyAuthProvider.class);
        PROVIDER_REGISTRY.put(AuthTypeEnum.API_KEY_ENCODED, ApiKeyEncodedAuthProvider.class);
    }

    /**
     * Create an authentication provider based on the configuration.
     *
     * <p>This method examines the auth_type configuration parameter and creates the appropriate
     * authentication provider. If no auth_type is specified, it defaults to basic authentication
     * for backward compatibility.
     *
     * @param config the readonly configuration containing authentication settings
     * @return the appropriate authentication provider
     * @throws ElasticsearchConnectorException if the auth_type is not supported
     */
    public static AuthenticationProvider createProvider(ReadonlyConfig config) {
        AuthTypeEnum authType =
                config.getOptional(ElasticsearchBaseOptions.AUTH_TYPE).orElse(DEFAULT_AUTH_TYPE);

        log.debug("Creating authentication provider for type: {}", authType);

        Class<? extends AuthenticationProvider> providerClass = PROVIDER_REGISTRY.get(authType);
        if (providerClass == null) {
            throw new ElasticsearchConnectorException(
                    UNSUPPORTED_AUTH_TYPE,
                    String.format(
                            "Unsupported authentication type: %s. Supported types: %s",
                            authType, PROVIDER_REGISTRY.keySet()));
        }

        try {
            AuthenticationProvider provider = providerClass.getDeclaredConstructor().newInstance();
            provider.validate(config);
            log.info("Successfully created authentication provider: {}", authType);
            return provider;
        } catch (Exception e) {
            throw new ElasticsearchConnectorException(
                    UNSUPPORTED_AUTH_TYPE,
                    String.format(
                            "Failed to create authentication provider for type: %s", authType),
                    e);
        }
    }
}
