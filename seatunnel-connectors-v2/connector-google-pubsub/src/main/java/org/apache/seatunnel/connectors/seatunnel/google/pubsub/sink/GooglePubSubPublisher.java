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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.sink;

import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

class GooglePubSubPublisher implements PubSubPublisher {

    private static final long CLOSE_TIMEOUT_SECONDS = 60;

    private final Publisher publisher;
    private final ManagedChannel emulatorChannel;

    private GooglePubSubPublisher(Publisher publisher, ManagedChannel emulatorChannel) {
        this.publisher = publisher;
        this.emulatorChannel = emulatorChannel;
    }

    static PubSubPublisher create(GooglePubSubSinkConfig config) {
        ManagedChannel emulatorChannel = null;
        try {
            Publisher.Builder publisherBuilder =
                    Publisher.newBuilder(TopicName.of(config.getProjectId(), config.getTopic()));

            if (config.getEmulatorHost() != null) {
                emulatorChannel =
                        ManagedChannelBuilder.forTarget(config.getEmulatorHost())
                                .usePlaintext()
                                .build();
                publisherBuilder
                        .setChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(emulatorChannel)))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            } else if (config.getCredentialsPath() != null) {
                try (FileInputStream credentialsStream =
                        new FileInputStream(config.getCredentialsPath())) {
                    publisherBuilder.setCredentialsProvider(
                            FixedCredentialsProvider.create(
                                    GoogleCredentials.fromStream(credentialsStream)
                                            .createScoped(
                                                    PublisherStubSettings
                                                            .getDefaultServiceScopes())));
                }
            }

            return new GooglePubSubPublisher(publisherBuilder.build(), emulatorChannel);
        } catch (Exception e) {
            if (emulatorChannel != null) {
                emulatorChannel.shutdownNow();
            }
            throw new GooglePubSubConnectorException(
                    GooglePubSubConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Google Pub/Sub publisher for topic " + config.getTopic(),
                    e);
        }
    }

    @Override
    public ApiFuture<String> publish(PubsubMessage message) {
        return publisher.publish(message);
    }

    @Override
    public void publishAllOutstanding() {
        publisher.publishAllOutstanding();
    }

    @Override
    public void close() throws IOException {
        Throwable failure = null;
        publisher.shutdown();
        try {
            if (!publisher.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                failure = new IOException("Timed out while closing the Google Pub/Sub publisher");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
            throw new IOException("Failed to close Google Pub/Sub publisher", failure);
        }
    }
}
