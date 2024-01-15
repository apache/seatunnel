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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.PULSAR_CONFIG;

public class PulsarConfigUtil {

    public static final String IDENTIFIER = "Pulsar";

    private PulsarConfigUtil() {}

    public static PulsarAdmin createAdmin(PulsarAdminConfig config) {
        PulsarAdminBuilder builder = PulsarAdmin.builder();
        builder.serviceHttpUrl(config.getAdminUrl());
        builder.authentication(createAuthentication(config));
        try {
            return builder.build();
        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.OPEN_PULSAR_ADMIN_FAILED, e);
        }
    }

    public static PulsarClient createClient(
            PulsarClientConfig config, PulsarSemantics pulsarSemantics) {
        ClientBuilder builder = PulsarClient.builder();
        builder.serviceUrl(config.getServiceUrl());
        builder.authentication(createAuthentication(config));
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            builder.enableTransaction(true);
        }
        try {
            return builder.build();
        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.OPEN_PULSAR_CLIENT_FAILED, e);
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
                return AuthenticationFactory.create(
                        config.getAuthPluginClassName(), config.getAuthParams());
            } catch (PulsarClientException.UnsupportedAuthenticationException e) {
                throw new PulsarConnectorException(
                        PulsarConnectorErrorCode.PULSAR_AUTHENTICATION_FAILED, e);
            }
        } else {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.PULSAR_AUTHENTICATION_FAILED,
                    "Authentication parameters are required when using authentication plug-in.");
        }
    }

    /**
     * get TransactionCoordinatorClient
     *
     * @param pulsarClient
     * @return
     */
    public static TransactionCoordinatorClient getTcClient(PulsarClient pulsarClient) {
        TransactionCoordinatorClient coordinatorClient =
                ((PulsarClientImpl) pulsarClient).getTcClient();
        // enabled transaction.
        if (coordinatorClient == null) {
            throw new IllegalArgumentException("You haven't enable transaction in Pulsar client.");
        }

        return coordinatorClient;
    }

    /**
     * create transaction
     *
     * @param pulsarClient
     * @param timeout
     * @return
     * @throws PulsarClientException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static Transaction getTransaction(PulsarClient pulsarClient, int timeout)
            throws PulsarClientException, InterruptedException, ExecutionException {
        Transaction transaction =
                pulsarClient
                        .newTransaction()
                        .withTransactionTimeout(timeout, TimeUnit.SECONDS)
                        .build()
                        .get();
        return transaction;
    }

    /**
     * create a Producer
     *
     * @param pulsarClient
     * @param topic
     * @param pulsarSemantics
     * @param pluginConfig
     * @param messageRoutingMode
     * @return
     * @throws PulsarClientException
     */
    public static Producer<byte[]> createProducer(
            PulsarClient pulsarClient,
            String topic,
            PulsarSemantics pulsarSemantics,
            ReadonlyConfig pluginConfig,
            MessageRoutingMode messageRoutingMode)
            throws PulsarClientException {
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer(Schema.BYTES);
        producerBuilder.topic(topic);
        producerBuilder.messageRoutingMode(messageRoutingMode);
        producerBuilder.blockIfQueueFull(true);

        if (pluginConfig.get(PULSAR_CONFIG) != null) {
            Map<String, String> pulsarProperties = new HashMap<>();
            pluginConfig
                    .get(PULSAR_CONFIG)
                    .forEach((key, value) -> pulsarProperties.put(key, value));
            producerBuilder.properties(pulsarProperties);
        }
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            /**
             * A condition for pulsar to open a transaction Only producers disabled sendTimeout are
             * allowed to produce transactional messages
             */
            producerBuilder.sendTimeout(0, TimeUnit.SECONDS);
        }
        return producerBuilder.create();
    }

    /**
     * create TypedMessageBuilder
     *
     * @param producer
     * @param transaction
     * @return
     * @throws PulsarClientException
     */
    public static TypedMessageBuilder<byte[]> createTypedMessageBuilder(
            Producer<byte[]> producer, TransactionImpl transaction) throws PulsarClientException {
        ProducerBase<byte[]> producerBase = (ProducerBase<byte[]>) producer;
        return new TypedMessageBuilderImpl<byte[]>(producerBase, Schema.BYTES, transaction);
    }
}
