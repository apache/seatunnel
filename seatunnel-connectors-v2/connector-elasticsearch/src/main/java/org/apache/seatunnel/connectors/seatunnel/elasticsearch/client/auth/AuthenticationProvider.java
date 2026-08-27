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

import org.elasticsearch.client.RestClientBuilder;

public interface AuthenticationProvider {

    /**
     * Configure the Elasticsearch RestClient with authentication and TLS settings.
     *
     * <p>This method is called during client initialization to set up the appropriate
     * authentication mechanism and TLS configuration on the RestClientBuilder. The implementation
     * should handle both authentication and TLS configuration to ensure they work together
     * properly.
     *
     * @param builder the RestClientBuilder to configure
     * @param config the readonly configuration containing authentication and TLS parameters
     * @throws IllegalArgumentException if the configuration is invalid
     * @throws RuntimeException if authentication or TLS setup fails
     */
    void configure(RestClientBuilder builder, ReadonlyConfig config);

    /**
     * Get the authentication type identifier.
     *
     * <p>This identifier is used to match the authentication provider with the configured auth_type
     * parameter. It should be a unique, lowercase string that clearly identifies the authentication
     * mechanism.
     *
     * @return the authentication type identifier (e.g., "basic", "api_key", "oauth2")
     */
    String getAuthType();

    /**
     * Validate the authentication configuration.
     *
     * <p>This method is called before authentication setup to ensure that all required
     * configuration parameters are present and valid. It should throw an exception if the
     * configuration is incomplete or invalid.
     *
     * @param config the readonly configuration to validate
     * @throws IllegalArgumentException if required parameters are missing or invalid
     */
    void validate(ReadonlyConfig config);
}
