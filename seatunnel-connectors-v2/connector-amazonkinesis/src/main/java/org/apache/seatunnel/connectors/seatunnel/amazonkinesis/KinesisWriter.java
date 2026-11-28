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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Destructive requests use one physical SDK attempt. Only explicit failed slots are replayed. */
final class KinesisWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final KinesisConnection connection;
    private final KinesisConnection.Settings target;
    private final AmazonKinesisSink.SinkSettings options;
    private final JsonSerializationSchema serializer;
    private final boolean buffered;
    private final ExecutorService io =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread worker = new Thread(task, "kinesis-sink-io");
                        worker.setDaemon(true);
                        return worker;
                    });
    private final Map<String, String> previousSequence = new HashMap<>();
    private final List<PutRecordsRequestEntry> batch = new ArrayList<>();
    private int bytes;
    private volatile boolean closed;
    private boolean failed;
    private volatile Thread flushThread;

    KinesisWriter(
            KinesisConnection.Settings target,
            AmazonKinesisSink.SinkSettings options,
            boolean buffered) {
        this.target = target;
        this.options = options;
        this.buffered = buffered;
        serializer =
                "json".equals(options.format) ? new JsonSerializationSchema(options.type) : null;
        connection = new KinesisConnection(target);
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        ensureOpen();
        if (row.getRowKind() != RowKind.INSERT) {
            failed = true;
            throw new IOException("Kinesis supports append-only rows");
        }
        try {
            String key =
                    options.keyIndex >= 0
                            ? (String) row.getField(options.keyIndex)
                            : options.fixedKey != null
                                    ? options.fixedKey
                                    : UUID.randomUUID().toString();
            verifyKey(key);
            byte[] payload = payload(row);
            int keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
            if (payload.length > options.recordBytes - keyBytes) {
                throw new IllegalArgumentException("record exceeds the configured limit");
            }
            if (options.ordered) {
                orderedPut(key, payload);
                return;
            }
            int size = payload.length + keyBytes;
            if (size > options.batchBytes - bytes) {
                flush();
            }
            batch.add(
                    PutRecordsRequestEntry.builder()
                            .partitionKey(key)
                            .data(SdkBytes.fromByteArray(payload))
                            .build());
            bytes += size;
            if (!buffered || batch.size() == options.batchSize || bytes == options.batchBytes) {
                flush();
            }
        } catch (RuntimeException ex) {
            failed = true;
            throw new IOException("Kinesis record has an invalid payload, key or size");
        }
    }

    /** A writer without checkpoints sends immediately, and ordered mode chains key-local acks. */
    private void orderedPut(String key, byte[] payload) throws IOException {
        if (!previousSequence.containsKey(key) && previousSequence.size() == 8192) {
            failed = true;
            throw new IOException("Kinesis ordered key budget exceeded");
        }
        flushThread = Thread.currentThread();
        try {
            ensureOpen();
            PutRecordRequest request =
                    PutRecordRequest.builder()
                            .streamName(target.streamName)
                            .streamARN(target.streamArn)
                            .partitionKey(key)
                            .data(SdkBytes.fromByteArray(payload))
                            .sequenceNumberForOrdering(previousSequence.get(key))
                            .build();
            PutRecordResponse response = send("PutRecord", () -> connection.sdk.putRecord(request));
            if (response.sequenceNumber() == null
                    || !response.sequenceNumber().matches("[0-9]{1,129}")
                    || response.shardId() == null
                    || response.shardId().isEmpty()) {
                throw new IOException("Kinesis PutRecord returned an invalid acknowledgment");
            }
            previousSequence.put(key, response.sequenceNumber());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            failed = true;
            throw new IOException("Kinesis ordered write interrupted");
        } catch (IOException ex) {
            failed = true;
            throw ex;
        } finally {
            flushThread = null;
        }
    }

    private byte[] payload(SeaTunnelRow row) {
        if (serializer != null) {
            return serializer.serialize(row);
        }
        Object value = row.getField(options.dataIndex);
        if (value == null) {
            throw new IllegalArgumentException("Kinesis payload cannot be null");
        }
        return "binary".equals(options.format)
                ? (byte[]) value
                : ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    static void verifyKey(String key) {
        if (key == null
                || key.isEmpty()
                || key.codePointCount(0, key.length()) > 256
                || !StandardCharsets.UTF_8.newEncoder().canEncode(key)) {
            throw new IllegalArgumentException(
                    "Kinesis partition key must have 1 to 256 Unicode characters");
        }
    }

    /** Run in the writer's engine thread, also invoked from the checkpoint and flush signal. */
    void flush() throws IOException {
        ensureOpen();
        if (batch.isEmpty()) {
            return;
        }
        flushThread = Thread.currentThread();
        try {
            List<PutRecordsRequestEntry> pending = new ArrayList<>(batch);
            for (int attempt = 1; ; attempt++) {
                ensureOpen();
                PutRecordsRequest request =
                        PutRecordsRequest.builder()
                                .streamName(target.streamName)
                                .streamARN(target.streamArn)
                                .records(pending)
                                .build();
                PutRecordsResponse response =
                        send("PutRecords", () -> connection.sdk.putRecords(request));
                pending = retryable(response, pending);
                if (pending.isEmpty()) {
                    batch.clear();
                    bytes = 0;
                    return;
                }
                if (attempt >= options.attempts) {
                    throw new IOException("Kinesis partial retry budget exhausted");
                }
                KinesisConnection.waitRetry(attempt);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            failed = true;
            throw new IOException("Kinesis flush interrupted");
        } catch (IOException ex) {
            failed = true;
            throw ex;
        } finally {
            flushThread = null;
        }
    }

    /**
     * Validate all positional replies before deciding which of the failed slots are safe to retry.
     */
    static List<PutRecordsRequestEntry> retryable(
            PutRecordsResponse response, List<PutRecordsRequestEntry> sent) throws IOException {
        // The current AWS response model permits absent FailedRecordCount on an all-success result.
        if (response == null || response.records().size() != sent.size()) {
            throw new IOException(
                    "Kinesis PutRecords acknowledgment cardinality differs from request");
        }
        List<PutRecordsRequestEntry> failures = new ArrayList<>();
        boolean permanent = false;
        for (int i = 0; i < sent.size(); i++) {
            PutRecordsResultEntry slot = response.records().get(i);
            if (slot == null) {
                throw new IOException("Kinesis PutRecords returned an invalid acknowledgment");
            }
            String code = slot.errorCode();
            boolean validSuccess =
                    slot.shardId() != null
                            && !slot.shardId().isEmpty()
                            && slot.sequenceNumber() != null
                            && slot.sequenceNumber().matches("[0-9]{1,129}");
            if (code == null && validSuccess && slot.errorMessage() == null) {
                continue;
            }
            if (code == null
                    || code.isEmpty()
                    || slot.sequenceNumber() != null
                    || slot.shardId() != null) {
                throw new IOException("Kinesis PutRecords returned an invalid acknowledgment");
            }
            failures.add(sent.get(i));
            permanent |=
                    !"ProvisionedThroughputExceededException".equals(code)
                            && !"InternalFailure".equals(code);
        }
        Integer failedCount = response.failedRecordCount();
        if (failedCount == null ? !failures.isEmpty() : failedCount != failures.size()) {
            throw new IOException("Kinesis PutRecords failure count differs from reply entries");
        }
        if (permanent) {
            throw new IOException("Kinesis PutRecords returned a permanent record failure");
        }
        return failures;
    }

    /**
     * Interruption of the engine writer aborts the socket even when the worker is still in the SDK.
     */
    private <T> T send(String operation, Supplier<T> call)
            throws IOException, InterruptedException {
        Future<T> active;
        try {
            active = io.submit(call::get);
        } catch (RuntimeException ex) {
            throw new IOException("Kinesis writer closed before sending");
        }
        try {
            return active.get(target.timeoutMs + 1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            active.cancel(true);
            connection.close();
            throw ex;
        } catch (TimeoutException ex) {
            active.cancel(true);
            connection.close();
            throw new IOException(
                    "Kinesis " + operation + " did not return an unambiguous acknowledgment");
        } catch (ExecutionException ex) {
            throw new IOException(
                    "Kinesis " + operation + " did not return an unambiguous acknowledgment");
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || failed || Thread.currentThread().isInterrupted()) {
            throw new IOException("Kinesis writer is closed or failed");
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        close();
    }

    @Override
    public void close() {
        closed = true;
        io.shutdownNow();
        connection.close();
        Thread interrupted = flushThread;
        if (interrupted != null && interrupted != Thread.currentThread()) {
            interrupted.interrupt();
        }
    }
}
