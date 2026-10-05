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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class BasicAuthProvider extends AbstractAuthenticationProvider {

    private static final String AUTH_TYPE = "basic";

    @Override
    protected void configureAuthentication(
            HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config) {
        Optional<String> username = config.getOptional(ElasticsearchBaseOptions.USERNAME);
        Optional<String> password = config.getOptional(ElasticsearchBaseOptions.PASSWORD);

        if (username.isPresent() && password.isPresent()) {
            log.debug("Configuring basic authentication for user: {}", username.get());

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(username.get(), password.get()));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

            log.info("Basic authentication configured successfully for user: {}", username.get());
        } else {
            log.debug("No username/password provided, skipping basic authentication configuration");
        }
    }

    @Override
    public String getAuthType() {
        return AUTH_TYPE;
    }

    /**
     * No-op. Username/password presence and pairing are now enforced declaratively via {@code
     * OptionRule} (see {@code ElasticsearchSourceFactory} / {@code ElasticsearchSinkFactory} /
     * {@code ElasticSearchCatalogFactory}). This method is kept to honor the {@link
     * AuthenticationProvider} contract.
     */
    @Override
    public void validate(ReadonlyConfig config) {
        // intentionally empty - validation handled by OptionRule
    }
}
