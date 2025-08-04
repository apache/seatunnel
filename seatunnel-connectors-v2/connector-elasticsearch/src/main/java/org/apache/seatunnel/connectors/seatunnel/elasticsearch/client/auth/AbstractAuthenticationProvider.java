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
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.util.SSLUtils;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContexts;

import org.elasticsearch.client.RestClientBuilder;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.util.Optional;

@Slf4j
public abstract class AbstractAuthenticationProvider implements AuthenticationProvider {

    @Override
    public final void configure(RestClientBuilder builder, ReadonlyConfig config) {
        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    // Configure authentication first
                    configureAuthentication(httpClientBuilder, config);

                    // Then configure TLS
                    configureTLS(httpClientBuilder, config);

                    return httpClientBuilder;
                });
    }

    /**
     * Configure the specific authentication mechanism.
     *
     * <p>Subclasses should implement this method to set up their specific authentication logic on
     * the HttpAsyncClientBuilder.
     *
     * @param httpClientBuilder the HTTP client builder to configure
     * @param config the readonly configuration containing authentication parameters
     */
    protected abstract void configureAuthentication(
            HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config);

    /**
     * Configure TLS settings for the HTTP client.
     *
     * <p>This method handles SSL/TLS configuration including certificate verification, hostname
     * verification, and custom keystores/truststores.
     *
     * @param httpClientBuilder the HTTP client builder to configure
     * @param config the readonly configuration containing TLS parameters
     */
    protected void configureTLS(HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config) {
        boolean tlsVerifyCertificate = config.get(ElasticsearchBaseOptions.TLS_VERIFY_CERTIFICATE);
        boolean tlsVerifyHostnames = config.get(ElasticsearchBaseOptions.TLS_VERIFY_HOSTNAME);

        try {
            if (tlsVerifyCertificate) {
                Optional<String> keystorePath =
                        config.getOptional(ElasticsearchBaseOptions.TLS_KEY_STORE_PATH);
                Optional<String> keystorePassword =
                        config.getOptional(ElasticsearchBaseOptions.TLS_KEY_STORE_PASSWORD);
                Optional<String> truststorePath =
                        config.getOptional(ElasticsearchBaseOptions.TLS_TRUST_STORE_PATH);
                Optional<String> truststorePassword =
                        config.getOptional(ElasticsearchBaseOptions.TLS_TRUST_STORE_PASSWORD);

                Optional<SSLContext> sslContext =
                        SSLUtils.buildSSLContext(
                                keystorePath, keystorePassword, truststorePath, truststorePassword);

                if (sslContext.isPresent()) {
                    httpClientBuilder.setSSLContext(sslContext.get());
                    log.debug("Custom SSL context configured with keystore/truststore");
                } else {
                    log.debug("No custom SSL context configured, using default");
                }
            } else {
                // Trust all certificates (not recommended for production)
                SSLContext sslContext =
                        SSLContexts.custom().loadTrustMaterial(new TrustAllStrategy()).build();
                httpClientBuilder.setSSLContext(sslContext);
                log.warn("TLS certificate verification disabled - not recommended for production");
            }

            if (!tlsVerifyHostnames) {
                httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                log.warn("TLS hostname verification disabled - not recommended for production");
            }

            log.debug(
                    "TLS configuration completed - certificate verification: {}, hostname verification: {}",
                    tlsVerifyCertificate,
                    tlsVerifyHostnames);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure TLS settings", e);
        }
    }
}
