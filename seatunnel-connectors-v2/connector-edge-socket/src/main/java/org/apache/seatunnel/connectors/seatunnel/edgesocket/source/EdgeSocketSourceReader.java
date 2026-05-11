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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketAuthType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketRecordQueue;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.LocalEdgeSocketRecordQueue;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.QueueOfferResult;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.DefaultEdgeSocketPayloadDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.DefaultEdgeSocketRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPayloadDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketRecordDeserializer;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EdgeSocketSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final String INGRESS_ACK_RESPONSE = "ACK";
    private static final String INGRESS_RETRY_RESPONSE = "RETRY";
    private static final String INGRESS_AUTH_FAILED_RESPONSE = "AUTH_FAILED";
    private static final String AUTH_TOKEN_PREFIX = "__AUTH__:";

    private final EdgeSocketConfig parameter;
    private final SingleSplitReaderContext context;
    private final EdgeSocketRecordQueue recordQueue;
    private final EdgeSocketRecordDeserializer recordDeserializer;
    private final EdgeSocketPayloadDeserializer payloadDeserializer;
    private final DeserializationSchema<SeaTunnelRow> rowDeserializationSchema;
    private final Object lifecycleLock = new Object();

    private volatile ServerSocket serverSocket;
    private volatile ExecutorService receiverExecutor;
    private volatile Future<?> receiverFuture;

    EdgeSocketSourceReader(
            EdgeSocketConfig parameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> rowDeserializationSchema) {
        this.parameter = parameter;
        this.context = context;
        this.recordQueue = new LocalEdgeSocketRecordQueue(parameter.getLocalQueueCapacity());
        this.recordDeserializer = DefaultEdgeSocketRecordDeserializer.create(parameter);
        this.payloadDeserializer = DefaultEdgeSocketPayloadDeserializer.create();
        this.rowDeserializationSchema = rowDeserializationSchema;
    }

    @Override
    public void open() {
        synchronized (lifecycleLock) {
            if (isReceiverAlive()) {
                log.warn("Edge socket source reader is already running, skip duplicate open");
                return;
            }
            startReceiverLoop();
        }
    }

    @Override
    public void close() throws IOException {
        closeServerSocket();
        synchronized (lifecycleLock) {
            stopReceiverLoop();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        // Source readers must hold checkpoint lock while emitting records.
        synchronized (output.getCheckpointLock()) {
            EdgeSocketQueuedRecord record = recordQueue.poll();
            if (record != null) {
                emitRecordSafely(record, output);
                return;
            }
        }
    }

    private void emitRecordSafely(EdgeSocketQueuedRecord record, Collector<SeaTunnelRow> output) {
        try {
            String payload = payloadDeserializer.deserializeRecord(record);
            SeaTunnelRow row =
                    rowDeserializationSchema.deserialize(payload.getBytes(StandardCharsets.UTF_8));
            if (row != null) {
                output.collect(row);
            }
        } catch (Exception deserializeException) {
            // Schema mode is strict: fail-fast instead of silently skipping bad records.
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Deserialize queued record to SeaTunnelRow failed. "
                            + "Incoming data does not match configured schema or payload format.",
                    deserializeException);
        }
    }

    private void openServerSocketWithRetry() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            return;
        }

        int maxRetries = parameter.getMaxNumRetries();
        int retries = 0;
        while (isReceiverActive() && (maxRetries < 0 || retries <= maxRetries)) {
            try {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(resolveBindAddress(parameter.getPort()));
                serverSocket.setSoTimeout(parameter.getAcceptTimeoutMs());
                // This log is the key observability point for ingress readiness.
                log.info(
                        "Edge socket ingress started, bind host:[{}], port:[{}], advertise host:[{}], attempt:[{}]",
                        "0.0.0.0",
                        parameter.getPort(),
                        parameter.getHost(),
                        retries + 1);
                return;
            } catch (IOException bindException) {
                try {
                    closeServerSocket();
                } catch (IOException closeException) {
                    throw new EdgeSocketConnectorException(
                            EdgeSocketConnectorErrorCode.SOURCE_BIND_FAILED,
                            "Close edge socket ingress server failed during reopen",
                            closeException);
                }
                // Exhaust retries only when maxRetries is configured to a finite value.
                if (maxRetries >= 0 && retries >= maxRetries) {
                    throw new EdgeSocketConnectorException(
                            EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED,
                            String.format(
                                    "Bind edge socket ingress %s:%s failed after %s retries",
                                    "0.0.0.0", parameter.getPort(), maxRetries),
                            bindException);
                }
                retries++;
                if (!sleepBeforeRetry()) {
                    return;
                }
            }
        }

        if (!isReceiverActive()) {
            return;
        }
        throw new EdgeSocketConnectorException(
                EdgeSocketConnectorErrorCode.SOURCE_BIND_FAILED,
                String.format(
                        "Unexpected bind state for edge socket ingress %s:%s",
                        "0.0.0.0", parameter.getPort()));
    }

    private void receiveLoop() {
        while (isReceiverActive()) {
            try {
                openServerSocketWithRetry();
                if (!isReceiverActive()) {
                    return;
                }
            } catch (RuntimeException openException) {
                if (!isReceiverActive()) {
                    return;
                }
                // Keep retrying while reader is alive; ingress should self-heal on transient
                // errors.
                log.warn(
                        "Open edge socket ingress failed on {}:{}, retrying",
                        "0.0.0.0",
                        parameter.getPort(),
                        openException);
                if (!sleepBeforeRetry()) {
                    return;
                }
                continue;
            }
            try (Socket collectorSocket = serverSocket.accept();
                    BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            collectorSocket.getInputStream(),
                                            StandardCharsets.UTF_8));
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            collectorSocket.getOutputStream(),
                                            StandardCharsets.UTF_8))) {
                collectorSocket.setSoTimeout(parameter.getAcceptTimeoutMs());
                log.info(
                        "Accepted edge collector connection from {}",
                        collectorSocket.getRemoteSocketAddress());
                // Reject unauthenticated collectors before entering record receive loop.
                if (!authenticateCollector(reader, writer)) {
                    continue;
                }
                receiveFromCollector(reader, writer);
            } catch (SocketTimeoutException timeoutException) {
                log.warn(
                        "Accept edge collector connection timeout on {}:{}, continue waiting",
                        "0.0.0.0",
                        parameter.getPort(),
                        timeoutException);
                // accept timeout: continue loop to check receiver state.
            } catch (IOException acceptException) {
                if (!isReceiverActive()) {
                    return;
                }
                log.warn(
                        "Failed to accept edge collector connection on {}:{}, retrying",
                        "0.0.0.0",
                        parameter.getPort(),
                        acceptException);
                if (!sleepBeforeRetry()) {
                    return;
                }
            } catch (RuntimeException runtimeException) {
                if (!isReceiverActive()) {
                    return;
                }
                log.warn("Edge socket receiver loop runtime exception, retrying", runtimeException);
                if (!sleepBeforeRetry()) {
                    return;
                }
            }
        }
    }

    private boolean authenticateCollector(BufferedReader reader, BufferedWriter writer)
            throws IOException {
        // Phase-1 keeps auth mode simple: only token auth is accepted.
        if (parameter.getAuthType() != EdgeSocketAuthType.TOKEN) {
            writeResponse(writer, INGRESS_AUTH_FAILED_RESPONSE);
            log.warn("Unsupported auth type: {}", parameter.getAuthType());
            return false;
        }
        String authLine;
        try {
            authLine = reader.readLine();
        } catch (SocketTimeoutException timeoutException) {
            writeResponse(writer, INGRESS_AUTH_FAILED_RESPONSE);
            log.warn("Collector authentication timeout, connection rejected");
            return false;
        }
        if (authLine == null) {
            writeResponse(writer, INGRESS_AUTH_FAILED_RESPONSE);
            log.warn("Collector closed connection before authentication");
            return false;
        }
        String presentedToken = parseAuthToken(authLine);
        // Authentication failures are returned with unified AUTH_FAILED response.
        if (!parameter.getAuthToken().equals(presentedToken)) {
            writeResponse(writer, INGRESS_AUTH_FAILED_RESPONSE);
            log.warn("Collector authentication failed");
            return false;
        }
        writeResponse(writer, INGRESS_ACK_RESPONSE);
        return true;
    }

    private String parseAuthToken(String authLine) {
        if (authLine.startsWith(AUTH_TOKEN_PREFIX)) {
            return authLine.substring(AUTH_TOKEN_PREFIX.length());
        }
        return authLine;
    }

    private void receiveFromCollector(BufferedReader reader, BufferedWriter writer)
            throws IOException {
        while (isReceiverActive()) {
            String record;
            try {
                record = reader.readLine();
            } catch (SocketTimeoutException timeoutException) {
                continue;
            }
            if (record == null) {
                return;
            }
            writeResponse(writer, enqueueIncomingRecord(record));
        }
    }

    private String enqueueIncomingRecord(String incomingRecord) {
        try {
            EdgeSocketQueuedRecord decoded =
                    recordDeserializer.deserializeRecord(stripTailCarriageReturn(incomingRecord));
            QueueOfferResult offerResult = recordQueue.offer(decoded);
            // ACK only means record was accepted into local queue.
            if (offerResult == QueueOfferResult.ACCEPTED) {
                return INGRESS_ACK_RESPONSE;
            }
            return INGRESS_RETRY_RESPONSE;
        } catch (Exception decodeException) {
            // Decode/enqueue failures are retried by collector protocol.
            log.warn(
                    "Decode or enqueue ingress packet failed, collector should retry",
                    decodeException);
            return INGRESS_RETRY_RESPONSE;
        }
    }

    private void writeResponse(BufferedWriter writer, String response) throws IOException {
        writer.write(response);
        writer.newLine();
        writer.flush();
    }

    private boolean sleepBeforeRetry() {
        try {
            TimeUnit.MILLISECONDS.sleep(parameter.getReconnectIntervalMs());
            return true;
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String stripTailCarriageReturn(String value) {
        if (value.endsWith("\r")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private void closeServerSocket() throws IOException {
        ServerSocket current = serverSocket;
        serverSocket = null;
        if (current != null) {
            current.close();
        }
    }

    private void startReceiverLoop() {
        receiverExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "edge-socket-receiver");
                            thread.setDaemon(false);
                            return thread;
                        });
        receiverFuture = receiverExecutor.submit(this::receiveLoop);
    }

    private void stopReceiverLoop() {
        if (receiverFuture != null) {
            receiverFuture.cancel(true);
            receiverFuture = null;
        }
        if (receiverExecutor != null) {
            receiverExecutor.shutdownNow();
            try {
                if (!receiverExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.warn("Edge socket receiver executor did not terminate within timeout");
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            } finally {
                receiverExecutor = null;
            }
        }
    }

    private InetSocketAddress resolveBindAddress(int port) {
        return new InetSocketAddress(port);
    }

    private boolean isReceiverActive() {
        ExecutorService executor = receiverExecutor;
        return executor != null
                && !executor.isShutdown()
                && !Thread.currentThread().isInterrupted();
    }

    private boolean isReceiverAlive() {
        Future<?> future = receiverFuture;
        return future != null && !future.isDone();
    }
}
