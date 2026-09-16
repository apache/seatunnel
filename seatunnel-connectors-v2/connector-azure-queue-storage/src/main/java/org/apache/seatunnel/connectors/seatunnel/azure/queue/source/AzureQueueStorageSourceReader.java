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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Reads Azure Queue messages and deletes them only after their checkpoint completes. */
@Slf4j
public class AzureQueueStorageSourceReader implements SourceReader<SeaTunnelRow, SingleSplit> {

    private final AzureQueueSourceConfig config;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final ReceiverFactory receiverFactory;
    private final Object acknowledgementLock = new Object();
    private final Set<AzureQueueMessage> leasedMessages = new LinkedHashSet<>();
    private final Set<AzureQueueMessage> unacknowledgedMessages = new LinkedHashSet<>();
    private final NavigableMap<Long, List<AzureQueueMessage>> pendingAcknowledgements =
            new TreeMap<>();
    private final AtomicReference<Throwable> visibilityRenewalFailure = new AtomicReference<>();

    private AzureQueueReceiver receiver;
    private ScheduledExecutorService visibilityRenewalExecutor;
    private volatile boolean splitAssigned;

    public AzureQueueStorageSourceReader(
            AzureQueueSourceConfig config,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this(config, deserializationSchema, () -> AzureQueueStorageReceiver.create(config));
    }

    AzureQueueStorageSourceReader(
            AzureQueueSourceConfig config,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            ReceiverFactory receiverFactory) {
        this.config = config;
        this.deserializationSchema = deserializationSchema;
        this.receiverFactory = receiverFactory;
    }

    @Override
    public void open() {
        // The client starts after the source split has been assigned.
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        if (!splitAssigned) {
            return;
        }
        checkVisibilityRenewalFailure();
        int availableCapacity;
        synchronized (acknowledgementLock) {
            availableCapacity = config.getMaxInFlightMessages() - leasedMessages.size();
        }
        if (availableCapacity <= 0) {
            sleepBeforeNextPoll();
            return;
        }

        List<AzureQueueMessage> messages;
        try {
            messages = receiver.receive(Math.min(config.getBatchSize(), availableCapacity));
        } catch (Exception e) {
            throw readFailure("Failed to receive Azure Queue Storage messages", e);
        }
        if (messages.isEmpty()) {
            sleepBeforeNextPoll();
            return;
        }

        synchronized (acknowledgementLock) {
            leasedMessages.addAll(messages);
        }
        for (int index = 0; index < messages.size(); index++) {
            AzureQueueMessage message = messages.get(index);
            try {
                synchronized (output.getCheckpointLock()) {
                    deserializationSchema.deserialize(message.getBody(), output);
                    synchronized (acknowledgementLock) {
                        unacknowledgedMessages.add(message);
                    }
                }
            } catch (Exception e) {
                releaseMessages(messages, index, e);
                throw readFailure(
                        "Failed to process Azure Queue Storage message " + message.getMessageId(),
                        e);
            }
        }
        checkVisibilityRenewalFailure();
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
                    "Azure Queue Storage source expects exactly one source split");
        }
        if (splitAssigned) {
            return;
        }

        receiver = receiverFactory.create();
        visibilityRenewalExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "azure-queue-visibility-renewal");
                            thread.setDaemon(true);
                            return thread;
                        });
        long renewalIntervalSeconds = Math.max(1L, config.getVisibilityTimeoutSeconds() / 3L);
        visibilityRenewalExecutor.scheduleWithFixedDelay(
                this::renewVisibilitySafely,
                renewalIntervalSeconds,
                renewalIntervalSeconds,
                TimeUnit.SECONDS);
        splitAssigned = true;
    }

    @Override
    public void handleNoMoreSplits() {
        // The queue split remains active for the lifetime of the streaming job.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        List<AzureQueueMessage> messages;
        synchronized (acknowledgementLock) {
            Map.Entry<Long, List<AzureQueueMessage>> checkpoint =
                    pendingAcknowledgements.floorEntry(checkpointId);
            if (checkpoint == null) {
                return;
            }
            messages = new ArrayList<>(checkpoint.getValue());
        }

        for (AzureQueueMessage message : messages) {
            try {
                synchronized (message) {
                    if (!message.isDeleted()) {
                        receiver.delete(message);
                    }
                }
            } catch (Exception e) {
                throw new AzureQueueConnectorException(
                        AzureQueueConnectorErrorCode.ACKNOWLEDGE_FAILED,
                        "Failed to delete Azure Queue Storage messages for checkpoint "
                                + checkpointId,
                        e);
            }
        }

        synchronized (acknowledgementLock) {
            leasedMessages.removeAll(messages);
            unacknowledgedMessages.removeAll(messages);
            pendingAcknowledgements.headMap(checkpointId, true).clear();
            for (List<AzureQueueMessage> pending : pendingAcknowledgements.values()) {
                pending.removeAll(messages);
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
        if (visibilityRenewalExecutor != null) {
            visibilityRenewalExecutor.shutdownNow();
        }

        IOException closeFailure = null;
        List<AzureQueueMessage> messages;
        synchronized (acknowledgementLock) {
            messages = new ArrayList<>(leasedMessages);
        }
        for (AzureQueueMessage message : messages) {
            try {
                release(message);
            } catch (Exception e) {
                if (closeFailure == null) {
                    closeFailure = new IOException("Failed to release Azure Queue message", e);
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        synchronized (acknowledgementLock) {
            leasedMessages.clear();
            unacknowledgedMessages.clear();
            pendingAcknowledgements.clear();
        }
        if (receiver != null) {
            receiver.close();
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    void renewVisibilityNow() {
        List<AzureQueueMessage> messages;
        synchronized (acknowledgementLock) {
            messages = new ArrayList<>(leasedMessages);
        }
        for (AzureQueueMessage message : messages) {
            synchronized (message) {
                if (!message.isDeleted()) {
                    receiver.renewVisibility(message);
                }
            }
        }
    }

    void renewVisibilitySafely() {
        try {
            renewVisibilityNow();
        } catch (Throwable failure) {
            visibilityRenewalFailure.compareAndSet(null, failure);
            log.error("Failed to renew Azure Queue Storage message visibility", failure);
        }
    }

    private void releaseMessages(
            List<AzureQueueMessage> messages, int firstMessage, Exception primaryFailure) {
        for (int index = firstMessage; index < messages.size(); index++) {
            try {
                release(messages.get(index));
            } catch (Exception releaseFailure) {
                primaryFailure.addSuppressed(releaseFailure);
            }
        }
    }

    private void release(AzureQueueMessage message) {
        synchronized (message) {
            if (!message.isDeleted()) {
                receiver.release(message);
            }
        }
        synchronized (acknowledgementLock) {
            leasedMessages.remove(message);
            unacknowledgedMessages.remove(message);
            for (List<AzureQueueMessage> pending : pendingAcknowledgements.values()) {
                pending.remove(message);
            }
        }
    }

    private void sleepBeforeNextPoll() {
        try {
            Thread.sleep(config.getPollIntervalMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw readFailure("Interrupted while polling Azure Queue Storage", e);
        }
    }

    private void checkVisibilityRenewalFailure() {
        Throwable failure = visibilityRenewalFailure.get();
        if (failure != null) {
            throw new AzureQueueConnectorException(
                    AzureQueueConnectorErrorCode.VISIBILITY_RENEWAL_FAILED,
                    "Azure Queue Storage message visibility renewal failed",
                    failure);
        }
    }

    private AzureQueueConnectorException readFailure(String message, Throwable cause) {
        return new AzureQueueConnectorException(
                AzureQueueConnectorErrorCode.READ_FAILED, message, cause);
    }

    @FunctionalInterface
    interface ReceiverFactory {
        AzureQueueReceiver create();
    }
}
