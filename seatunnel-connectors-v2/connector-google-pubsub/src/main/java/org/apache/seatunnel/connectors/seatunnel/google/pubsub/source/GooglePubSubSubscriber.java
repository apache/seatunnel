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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.source;

import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** Google Pub/Sub client lifecycle and transport configuration for a source reader. */
class GooglePubSubSubscriber implements PubSubSubscriber {

    private static final long CLOSE_TIMEOUT_SECONDS = 60;

    private final Subscriber subscriber;
    private final ManagedChannel emulatorChannel;

    private GooglePubSubSubscriber(Subscriber subscriber, ManagedChannel emulatorChannel) {
        this.subscriber = subscriber;
        this.emulatorChannel = emulatorChannel;
    }

    static PubSubSubscriber create(
            GooglePubSubSourceConfig config,
            MessageReceiverWithAckResponse receiver,
            Consumer<Throwable> failureHandler) {
        ManagedChannel emulatorChannel = null;
        try {
            Subscriber.Builder subscriberBuilder =
                    Subscriber.newBuilder(
                            ProjectSubscriptionName.of(
                                    config.getProjectId(), config.getSubscription()),
                            receiver);

            configureFlowControl(subscriberBuilder, config);

            if (config.getEmulatorHost() != null) {
                emulatorChannel =
                        ManagedChannelBuilder.forTarget(config.getEmulatorHost())
                                .usePlaintext()
                                .build();
                subscriberBuilder
                        .setChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(emulatorChannel)))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            } else if (config.getCredentialsPath() != null) {
                try (FileInputStream credentialsStream =
                        new FileInputStream(config.getCredentialsPath())) {
                    subscriberBuilder.setCredentialsProvider(
                            FixedCredentialsProvider.create(
                                    GoogleCredentials.fromStream(credentialsStream)
                                            .createScoped(
                                                    SubscriberStubSettings
                                                            .getDefaultServiceScopes())));
                }
            }

            Subscriber subscriber = subscriberBuilder.build();
            subscriber.addListener(
                    new ApiService.Listener() {
                        @Override
                        public void failed(ApiService.State from, Throwable failure) {
                            failureHandler.accept(failure);
                        }
                    },
                    Runnable::run);
            return new GooglePubSubSubscriber(subscriber, emulatorChannel);
        } catch (Exception e) {
            if (emulatorChannel != null) {
                emulatorChannel.shutdownNow();
            }
            throw new GooglePubSubConnectorException(
                    GooglePubSubConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Google Pub/Sub subscriber for subscription "
                            + config.getSubscription(),
                    e);
        }
    }

    private static void configureFlowControl(
            Subscriber.Builder subscriberBuilder, GooglePubSubSourceConfig config) {
        if (config.getMaxOutstandingMessages() != null || config.getMaxOutstandingBytes() != null) {
            FlowControlSettings.Builder flowControlSettings =
                    Subscriber.Builder.getDefaultFlowControlSettings().toBuilder();
            if (config.getMaxOutstandingMessages() != null) {
                flowControlSettings.setMaxOutstandingElementCount(
                        config.getMaxOutstandingMessages());
            }
            if (config.getMaxOutstandingBytes() != null) {
                flowControlSettings.setMaxOutstandingRequestBytes(config.getMaxOutstandingBytes());
            }
            subscriberBuilder.setFlowControlSettings(flowControlSettings.build());
        }
        if (config.getParallelPullCount() != null) {
            subscriberBuilder.setParallelPullCount(config.getParallelPullCount());
        }
    }

    @Override
    public void start() {
        try {
            subscriber.startAsync().awaitRunning();
        } catch (RuntimeException e) {
            throw new GooglePubSubConnectorException(
                    GooglePubSubConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to start Google Pub/Sub subscriber",
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        Throwable failure = null;
        subscriber.stopAsync();
        try {
            subscriber.awaitTerminated(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            failure = e;
        }

        if (emulatorChannel != null) {
            emulatorChannel.shutdown();
            try {
                if (!emulatorChannel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    emulatorChannel.shutdownNow();
                    IOException timeout =
                            new IOException("Timed out while closing the Pub/Sub emulator channel");
                    if (failure == null) {
                        failure = timeout;
                    } else {
                        failure.addSuppressed(timeout);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                emulatorChannel.shutdownNow();
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        if (failure != null) {
            throw new IOException("Failed to close Google Pub/Sub subscriber", failure);
        }
    }
}
