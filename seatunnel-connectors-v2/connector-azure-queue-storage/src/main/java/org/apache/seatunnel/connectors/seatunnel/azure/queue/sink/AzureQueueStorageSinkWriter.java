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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class AzureQueueStorageSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    static final long MAX_ENCODED_MESSAGE_BYTES = 64L * 1024L;

    private final AzureQueueSender sender;
    private final SerializationSchema serializationSchema;
    private final MessageEncoding messageEncoding;
    private final long operationTimeoutMillis;
    private final Semaphore sendPermits;
    private final Set<CompletableFuture<Void>> pendingSends = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Throwable> sendError = new AtomicReference<>();

    AzureQueueStorageSinkWriter(
            SeaTunnelRowType rowType, AzureQueueSinkConfig config, AzureQueueSender sender) {
        this.sender = sender;
        this.serializationSchema = createSerializationSchema(rowType, config);
        this.messageEncoding = config.getMessageEncoding();
        this.operationTimeoutMillis = config.getOperationTimeoutMillis();
        this.sendPermits = new Semaphore(config.getMaxInFlight());
    }

    public AzureQueueStorageSinkWriter(SeaTunnelRowType rowType, AzureQueueSinkConfig config) {
        this(rowType, config, AzureQueueStorageSender.create(config));
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        checkSendError();
        byte[] payload = serializationSchema.serialize(row);
        validateMessageSize(payload.length);
        acquireSendPermit();

        try {
            checkSendError();
        } catch (AzureQueueConnectorException e) {
            sendPermits.release();
            throw e;
        }

        CompletableFuture<Void> sendFuture;
        try {
            sendFuture = sender.send(new String(payload, StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            sendPermits.release();
            throw writeFailure(e);
        }

        pendingSends.add(sendFuture);
        sendFuture.whenComplete(
                (ignored, error) -> {
                    if (error != null) {
                        sendError.compareAndSet(null, unwrap(error));
                    }
                    pendingSends.remove(sendFuture);
                    sendPermits.release();
                });
        checkSendError();
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    private void acquireSendPermit() {
        try {
            if (!sendPermits.tryAcquire(operationTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw writeFailure(
                        new TimeoutException(
                                "Timed out waiting for an available Azure Queue send slot"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw writeFailure(e);
        }
    }

    private void flush() {
        CompletableFuture<?>[] futures = pendingSends.toArray(new CompletableFuture<?>[0]);
        if (futures.length > 0) {
            try {
                CompletableFuture.allOf(futures).get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw writeFailure(e);
            } catch (ExecutionException e) {
                throw writeFailure(unwrap(e.getCause()));
            } catch (TimeoutException e) {
                throw writeFailure(e);
            }
        }
        checkSendError();
    }

    @Override
    public void close() throws IOException {
        AzureQueueConnectorException failure = null;
        try {
            flush();
        } catch (AzureQueueConnectorException e) {
            failure = e;
        }

        try {
            sender.close();
        } catch (Exception e) {
            if (failure == null) {
                failure =
                        new AzureQueueConnectorException(
                                AzureQueueConnectorErrorCode.CLOSE_FAILED,
                                "Failed to close Azure Queue Storage sender",
                                e);
            } else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void validateMessageSize(int payloadSize) {
        long encodedSize = payloadSize;
        if (messageEncoding == MessageEncoding.BASE64) {
            encodedSize = 4L * ((payloadSize + 2L) / 3L);
        }
        if (encodedSize > MAX_ENCODED_MESSAGE_BYTES) {
            throw new AzureQueueConnectorException(
                    AzureQueueConnectorErrorCode.MESSAGE_TOO_LARGE,
                    "Serialized message is "
                            + encodedSize
                            + " bytes after "
                            + messageEncoding.name().toLowerCase(Locale.ROOT)
                            + " encoding, exceeding the Azure Queue limit of "
                            + MAX_ENCODED_MESSAGE_BYTES
                            + " bytes");
        }
    }

    private void checkSendError() {
        Throwable failure = sendError.get();
        if (failure != null) {
            throw writeFailure(failure);
        }
    }

    private AzureQueueConnectorException writeFailure(Throwable cause) {
        return new AzureQueueConnectorException(
                AzureQueueConnectorErrorCode.WRITE_FAILED,
                "Failed to send message to Azure Queue Storage",
                cause);
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null
                && (cause instanceof java.util.concurrent.CompletionException
                        || cause instanceof ExecutionException)) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, AzureQueueSinkConfig config) {
        if (config.getFormat() == MessageFormat.JSON) {
            return new JsonSerializationSchema(rowType);
        }
        return TextSerializationSchema.builder()
                .seaTunnelRowType(rowType)
                .delimiter(config.getFieldDelimiter())
                .build();
    }
}
