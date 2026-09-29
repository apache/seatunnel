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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class GooglePubSubSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final PubSubPublisher publisher;
    private final SerializationSchema serializationSchema;
    private final Set<ApiFuture<String>> pendingPublishes = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Throwable> publishError = new AtomicReference<>();

    GooglePubSubSinkWriter(
            SeaTunnelRowType rowType, GooglePubSubSinkConfig config, PubSubPublisher publisher) {
        this.publisher = publisher;
        this.serializationSchema = createSerializationSchema(rowType, config);
    }

    public GooglePubSubSinkWriter(SeaTunnelRowType rowType, GooglePubSubSinkConfig config) {
        this(rowType, config, GooglePubSubPublisher.create(config));
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        checkPublishError();

        PubsubMessage message =
                PubsubMessage.newBuilder()
                        .setData(ByteString.copyFrom(serializationSchema.serialize(row)))
                        .build();
        ApiFuture<String> publishFuture;
        try {
            publishFuture = publisher.publish(message);
        } catch (RuntimeException e) {
            throw writeFailure(e);
        }

        pendingPublishes.add(publishFuture);
        ApiFutures.addCallback(
                publishFuture,
                new ApiFutureCallback<String>() {
                    @Override
                    public void onSuccess(String messageId) {
                        pendingPublishes.remove(publishFuture);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        publishError.compareAndSet(null, throwable);
                        pendingPublishes.remove(publishFuture);
                    }
                },
                Runnable::run);
        checkPublishError();
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    private void flush() {
        checkPublishError();
        try {
            publisher.publishAllOutstanding();
        } catch (RuntimeException e) {
            throw writeFailure(e);
        }
        try {
            ApiFutures.allAsList(new ArrayList<>(pendingPublishes)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw writeFailure(e);
        } catch (ExecutionException e) {
            throw writeFailure(e.getCause());
        }
        checkPublishError();
    }

    @Override
    public void close() throws IOException {
        GooglePubSubConnectorException failure = null;
        try {
            flush();
        } catch (GooglePubSubConnectorException e) {
            failure = e;
        }

        try {
            publisher.close();
        } catch (Exception e) {
            if (failure == null) {
                failure =
                        new GooglePubSubConnectorException(
                                GooglePubSubConnectorErrorCode.CLOSE_FAILED,
                                "Failed to close Google Pub/Sub publisher",
                                e);
            } else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void checkPublishError() {
        Throwable failure = publishError.get();
        if (failure != null) {
            throw writeFailure(failure);
        }
    }

    private GooglePubSubConnectorException writeFailure(Throwable cause) {
        return new GooglePubSubConnectorException(
                GooglePubSubConnectorErrorCode.WRITE_FAILED,
                "Failed to publish message to Google Pub/Sub",
                cause);
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, GooglePubSubSinkConfig config) {
        if (config.getFormat() == MessageFormat.JSON) {
            return new JsonSerializationSchema(rowType);
        }
        return TextSerializationSchema.builder()
                .seaTunnelRowType(rowType)
                .delimiter(config.getFieldDelimiter())
                .build();
    }
}
