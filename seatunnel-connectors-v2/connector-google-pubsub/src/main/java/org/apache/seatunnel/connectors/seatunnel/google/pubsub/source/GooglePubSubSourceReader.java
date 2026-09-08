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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Reads Pub/Sub messages and acknowledges them only after their SeaTunnel checkpoint completes. */
public class GooglePubSubSourceReader implements SourceReader<SeaTunnelRow, SingleSplit> {

    private static final long POLL_TIMEOUT_MILLIS = 500;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final SubscriberFactory subscriberFactory;
    // Guards all message acknowledgement state shared with checkpoint callbacks.
    private final Object acknowledgementLock = new Object();
    private final BlockingQueue<ReceivedMessage> receivedMessages = new LinkedBlockingQueue<>();
    // Messages emitted since the last completed checkpoint.
    private final Set<AckReplyConsumerWithResponse> unacknowledgedMessages = new LinkedHashSet<>();
    // Immutable acknowledgement snapshots keyed by SeaTunnel checkpoint ID.
    private final NavigableMap<Long, List<AckReplyConsumerWithResponse>> pendingAcknowledgements =
            new TreeMap<>();
    // First asynchronous subscriber failure observed by the polling thread.
    private final AtomicReference<Throwable> subscriberFailure = new AtomicReference<>();

    private PubSubSubscriber subscriber;
    private boolean splitAssigned;

    public GooglePubSubSourceReader(
            GooglePubSubSourceConfig config,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this(
                deserializationSchema,
                (receiver, failureHandler) ->
                        GooglePubSubSubscriber.create(config, receiver, failureHandler));
    }

    GooglePubSubSourceReader(
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            SubscriberFactory subscriberFactory) {
        this.deserializationSchema = deserializationSchema;
        this.subscriberFactory = subscriberFactory;
    }

    @Override
    public void open() {
        // The subscriber starts after the source split has been assigned.
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        checkSubscriberFailure();
        ReceivedMessage receivedMessage =
                receivedMessages.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (receivedMessage == null) {
            checkSubscriberFailure();
            return;
        }

        synchronized (output.getCheckpointLock()) {
            try {
                deserializationSchema.deserialize(
                        receivedMessage.message.getData().toByteArray(), output);
                synchronized (acknowledgementLock) {
                    unacknowledgedMessages.add(receivedMessage.acknowledgement);
                }
            } catch (Exception e) {
                receivedMessage.acknowledgement.nack();
                throw new GooglePubSubConnectorException(
                        GooglePubSubConnectorErrorCode.READ_FAILED,
                        "Failed to deserialize Google Pub/Sub message "
                                + receivedMessage.message.getMessageId(),
                        e);
            }
        }
    }

    @Override
    public List<SingleSplit> snapshotState(long checkpointId) {
        synchronized (acknowledgementLock) {
            pendingAcknowledgements.put(checkpointId, new ArrayList<>(unacknowledgedMessages));
        }
        return Collections.singletonList(new SingleSplit(null));
    }

    @Override
    public void addSplits(List<SingleSplit> splits) {
        if (splits.size() != 1) {
            throw new IllegalArgumentException(
                    "Google Pub/Sub source expects exactly one source split");
        }
        if (splitAssigned) {
            return;
        }

        subscriber =
                subscriberFactory.create(
                        (message, acknowledgement) ->
                                receivedMessages.add(new ReceivedMessage(message, acknowledgement)),
                        failure -> subscriberFailure.compareAndSet(null, failure));
        subscriber.start();
        splitAssigned = true;
    }

    @Override
    public void handleNoMoreSplits() {
        // The single subscription split remains active for the lifetime of the streaming job.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        List<AckReplyConsumerWithResponse> acknowledgements;
        synchronized (acknowledgementLock) {
            Map.Entry<Long, List<AckReplyConsumerWithResponse>> checkpoint =
                    pendingAcknowledgements.floorEntry(checkpointId);
            if (checkpoint == null) {
                return;
            }
            acknowledgements = new ArrayList<>(checkpoint.getValue());
        }

        // Advance local state only after every acknowledgement in the selected checkpoint succeeds.
        // If Pub/Sub accepts only part of the batch, failing the callback leaves the remaining
        // messages eligible for redelivery instead of silently losing them from checkpoint state.
        acknowledge(acknowledgements, checkpointId);
        synchronized (acknowledgementLock) {
            unacknowledgedMessages.removeAll(acknowledgements);
            pendingAcknowledgements.headMap(checkpointId, true).clear();
            for (List<AckReplyConsumerWithResponse> pending : pendingAcknowledgements.values()) {
                pending.removeAll(acknowledgements);
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        synchronized (acknowledgementLock) {
            pendingAcknowledgements.remove(checkpointId);
        }
    }

    @Override
    public void close() throws IOException {
        if (subscriber != null) {
            subscriber.close();
        }
    }

    private void acknowledge(
            List<AckReplyConsumerWithResponse> acknowledgements, long checkpointId) {
        List<ApiFuture<AckResponse>> futures = new ArrayList<>(acknowledgements.size());
        for (AckReplyConsumerWithResponse acknowledgement : acknowledgements) {
            futures.add(acknowledgement.ack());
        }

        try {
            for (AckResponse response : ApiFutures.allAsList(futures).get()) {
                if (response != AckResponse.SUCCESSFUL) {
                    throw new GooglePubSubConnectorException(
                            GooglePubSubConnectorErrorCode.ACKNOWLEDGE_FAILED,
                            "Google Pub/Sub returned "
                                    + response
                                    + " while acknowledging checkpoint "
                                    + checkpointId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw acknowledgeFailure(checkpointId, e);
        } catch (ExecutionException e) {
            throw acknowledgeFailure(checkpointId, e.getCause());
        }
    }

    private void checkSubscriberFailure() {
        Throwable failure = subscriberFailure.get();
        if (failure != null) {
            throw new GooglePubSubConnectorException(
                    GooglePubSubConnectorErrorCode.READ_FAILED,
                    "Google Pub/Sub subscriber stopped unexpectedly",
                    failure);
        }
    }

    private GooglePubSubConnectorException acknowledgeFailure(long checkpointId, Throwable cause) {
        return new GooglePubSubConnectorException(
                GooglePubSubConnectorErrorCode.ACKNOWLEDGE_FAILED,
                "Failed to acknowledge Google Pub/Sub messages for checkpoint " + checkpointId,
                cause);
    }

    @FunctionalInterface
    interface SubscriberFactory {
        PubSubSubscriber create(
                MessageReceiverWithAckResponse receiver, Consumer<Throwable> failureHandler);
    }

    private static final class ReceivedMessage {
        private final PubsubMessage message;
        private final AckReplyConsumerWithResponse acknowledgement;

        private ReceivedMessage(
                PubsubMessage message, AckReplyConsumerWithResponse acknowledgement) {
            this.message = message;
            this.acknowledgement = acknowledgement;
        }
    }
}
