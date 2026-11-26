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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Checks existing queue metadata using an independently owned, non-recovering connection. */
public final class RabbitmqSourceDryRunValidator {
    private static final int MAX_TIMEOUT_MS = 10_000;
    private static final int CLOSE_TIMEOUT_MS = 1_000;

    private RabbitmqSourceDryRunValidator() {}

    public static void validate(ReadonlyConfig options) throws Exception {
        validate(options, RabbitmqClient::createConnectionFactory);
    }

    static void validate(
            ReadonlyConfig options, Function<RabbitmqConfig, ConnectionFactory> factoryProvider)
            throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("RabbitMQ connect dry-run interrupted");
        }
        Connection connection = null;
        Exception failure = null;
        String stage = "connection configuration";
        boolean interrupted = false;
        try {
            RabbitmqConfig config = new RabbitmqConfig(options);
            ConnectionFactory factory = factoryProvider.apply(config);
            // Zero means infinite in the client. Cap preflight waits without changing job options.
            int configuredTimeout = factory.getConnectionTimeout();
            if (configuredTimeout < 0) {
                throw new IllegalArgumentException("Invalid connection timeout");
            }
            int timeout =
                    configuredTimeout == 0
                            ? MAX_TIMEOUT_MS
                            : Math.min(configuredTimeout, MAX_TIMEOUT_MS);
            factory.setConnectionTimeout(timeout);
            factory.setHandshakeTimeout(timeout);
            factory.setChannelRpcTimeout(timeout);
            factory.setShutdownTimeout(CLOSE_TIMEOUT_MS);
            factory.setAutomaticRecoveryEnabled(false);
            factory.setTopologyRecoveryEnabled(false);

            stage = "connection or authentication";
            connection = factory.newConnection();
            stage = "channel creation";
            Channel channel = connection.createChannel();
            if (channel == null) {
                throw new IOException("No channel available");
            }
            List<Map<String, Object>> tables =
                    options.getOptional(RabbitmqSourceOptions.TABLE_CONFIGS)
                            .orElse(
                                    Collections.singletonList(
                                            Collections.singletonMap(
                                                    RabbitmqSourceOptions.QUEUE_NAME.key(),
                                                    config.getQueueName())));
            stage = "existing queue metadata (pre-create configured queues before dry-run)";
            for (Map<String, Object> table : tables) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("RabbitMQ connect dry-run interrupted");
                }
                // Never use the runtime declaration path: passive=false may create queues.
                channel.queueDeclarePassive(
                        ReadonlyConfig.fromMap(table).get(RabbitmqSourceOptions.QUEUE_NAME));
            }
        } catch (InterruptedException e) {
            interrupted = true;
            failure = new InterruptedException("RabbitMQ connect dry-run interrupted");
        } catch (Exception e) {
            // Driver/configuration errors can contain URI credentials or broker-controlled text.
            failure = new IOException("RabbitMQ connect dry-run could not validate " + stage);
        } finally {
            interrupted |= Thread.interrupted();
            if (connection != null) {
                try {
                    // Abort owns channel cleanup too, including a channel closed by a failed RPC.
                    connection.abort(CLOSE_TIMEOUT_MS);
                } catch (Exception e) {
                    if (failure == null) {
                        failure =
                                new IOException(
                                        "RabbitMQ connect dry-run connection cleanup failed");
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                if (failure == null) {
                    failure = new InterruptedException("RabbitMQ connect dry-run interrupted");
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
