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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

public class PulsarConfigUtil {

    public static final String IDENTIFIER = "pulsar";

    private PulsarConfigUtil() {
    }

    public static PulsarAdmin createAdmin(PulsarAdminConfig config) {
        PulsarAdminBuilder builder = PulsarAdmin.builder();
        builder.serviceHttpUrl(config.getAdminUrl());
        builder.authentication(createAuthentication(config));
        try {
            return builder.build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static PulsarClient createClient(PulsarClientConfig config) {
        ClientBuilder builder = PulsarClient.builder();
        builder.serviceUrl(config.getServiceUrl());
        builder.authentication(createAuthentication(config));
        try {
            return builder.build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static ConsumerBuilder<byte[]> createConsumerBuilder(
        PulsarClient client, PulsarConsumerConfig config) {
        ConsumerBuilder<byte[]> builder = client.newConsumer(Schema.BYTES);
        builder.subscriptionName(config.getSubscriptionName());
        return builder;
    }

    private static Authentication createAuthentication(BasePulsarConfig config) {
        if (StringUtils.isBlank(config.getAuthPluginClassName())) {
            return AuthenticationDisabled.INSTANCE;
        }

        if (StringUtils.isNotBlank(config.getAuthPluginClassName())) {
            try {
                return AuthenticationFactory.create(config.getAuthPluginClassName(), config.getAuthParams());
            } catch (PulsarClientException.UnsupportedAuthenticationException e) {
                throw new RuntimeException("Failed to create the authentication plug-in.", e);
            }
        } else {
            throw new IllegalArgumentException("Authentication parameters are required when using authentication plug-in.");
        }
    }
}
