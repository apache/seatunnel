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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.FirehoseClientBuilder;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Owns a synchronous SDK client and one daemon worker for a bounded flush operation. Only validated
 * failed positions are retried; ambiguous calls fail without internal whole-batch replay.
 */
final class FirehoseBatchClient implements AutoCloseable {
    private final String stream;
    private final int timeoutMillis;
    private final int maxRetries;
    private final FirehoseTransport transport;
    private final FirehoseClient service;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "seatunnel-firehose-sink");
                        thread.setDaemon(true);
                        return thread;
                    });

    // Guarded by this monitor; close prevents new submissions and cancels the published Future.
    private boolean closed;
    // Guarded by this monitor; waiting/sending happens outside it so close can abort active I/O.
    private Future<Void> inFlight;

    /*
     * Use the pinned client-local signer/transport and disable implicit SDK retry/compression.
     * The single outer flush budget owns any cancellation, including SDK and explicit subset attempts.
     */
    FirehoseBatchClient(
            String stream,
            String region,
            URI endpoint,
            AwsCredentialsProvider credentials,
            int timeoutMillis,
            int maxRetries) {
        this.stream = stream;
        this.timeoutMillis = timeoutMillis;
        this.maxRetries = maxRetries;
        this.transport = new FirehoseTransport(endpoint, timeoutMillis);
        FirehoseClientBuilder builder =
                FirehoseClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(credentials)
                        .httpClient(transport)
                        .putAuthScheme(new FirehoseAuthScheme())
                        .overrideConfiguration(
                                conf ->
                                        conf.retryPolicy(RetryPolicy.none())
                                                .apiCallTimeout(Duration.ofMillis(timeoutMillis))
                                                .compressionConfiguration(
                                                        compression ->
                                                                compression
                                                                        .requestCompressionEnabled(
                                                                                false)));
        if (endpoint != null) {
            builder.endpointOverride(endpoint);
        }
        this.service = builder.build();
    }

    /**
     * Wait under one monotonic flush budget, including SDK calls and failed-subset backoff. Publish
     * the Future before waiting so a concurrent close can abort it without taking the worker lock.
     */
    void put(List<Record> records) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Future<Void> request;
        synchronized (this) {
            if (closed || inFlight != null) {
                throw failure("writer is closed or already sending a batch");
            }
            List<Record> snapshot = new ArrayList<>(records);
            request =
                    worker.submit(
                            () -> {
                                send(snapshot, deadline);
                                return null;
                            });
            inFlight = request;
        }
        try {
            request.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            if (deadline - System.nanoTime() <= 0) {
                throw new TimeoutException();
            }
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw uncertain("batch interrupted");
        } catch (TimeoutException e) {
            close();
            throw uncertain("batch deadline exceeded");
        } catch (CancellationException e) {
            throw uncertain("batch cancelled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof BatchFailure) {
                throw (BatchFailure) e.getCause();
            }
            throw uncertain("batch failed");
        } finally {
            synchronized (this) {
                inFlight = null;
            }
        }
    }

    /*
     * Validate each complete positional response before deriving the next retry subset.
     * The same deadline spans every attempt; SDK implicit retries remain disabled to avoid ambiguous replay.
     */
    private void send(List<Record> pending, long deadline)
            throws IOException, InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw uncertain("batch interrupted");
            }
            if (deadline - System.nanoTime() <= 0) {
                throw uncertain("batch deadline exceeded");
            }
            PutRecordBatchResponse response;
            try {
                response =
                        service.putRecordBatch(
                                PutRecordBatchRequest.builder()
                                        .deliveryStreamName(stream)
                                        .records(pending)
                                        .build());
            } catch (SdkException e) {
                Throwable detail = e.getCause();
                while (detail != null && detail != detail.getCause()) {
                    if (detail instanceof FirehoseTransport.TransportFailure) {
                        throw uncertain(detail.getMessage());
                    }
                    detail = detail.getCause();
                }
                throw uncertain("request failed before a valid Firehose acknowledgement");
            }
            pending = failedRecords(response, pending);
            if (pending.isEmpty()) {
                return;
            }
            if (attempt >= maxRetries) {
                throw failure("retries exhausted for " + pending.size() + " failed records");
            }
            long delay = Math.min(100L << attempt, 4000) + ThreadLocalRandom.current().nextInt(50);
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (delay >= remaining) {
                throw failure("batch deadline cannot accommodate the next failed-record retry");
            }
            Thread.sleep(delay);
        }
    }

    /*
     * Require one consistent result per sent record before retrying any explicitly rejected slot.
     * Provider messages are deliberately neither logged nor retained in connector failures.
     */
    private static List<Record> failedRecords(PutRecordBatchResponse response, List<Record> sent)
            throws IOException {
        Integer count = response.failedPutCount();
        List<PutRecordBatchResponseEntry> entries = response.requestResponses();
        if (count == null
                || count < 0
                || count > sent.size()
                || !response.hasRequestResponses()
                || entries.size() != sent.size()) {
            throw uncertain("invalid acknowledgement count");
        }
        List<Record> failed = new ArrayList<>(count);
        for (int i = 0; i < sent.size(); i++) {
            PutRecordBatchResponseEntry entry = entries.get(i);
            if (entry == null) {
                throw uncertain("missing acknowledgement entry");
            }
            String id = entry.recordId();
            String error = entry.errorCode();
            if (id != null
                    && !id.trim().isEmpty()
                    && error == null
                    && entry.errorMessage() == null) {
                continue;
            }
            if (id != null || error == null || error.trim().isEmpty()) {
                throw uncertain("contradictory acknowledgement entry");
            }
            if (!"ServiceUnavailableException".equals(error) && !"InternalFailure".equals(error)) {
                throw uncertain("unrecognized per-record error");
            }
            failed.add(sent.get(i));
        }
        if (failed.size() != count) {
            throw uncertain("reported failure count does not match acknowledgement entries");
        }
        return failed;
    }

    /*
     * Permanently close publication first, then abort transport/Future and release owned resources.
     * Teardown does not submit retries or wait for an uninterruptible platform DNS resolver.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        transport.close();
        if (inFlight != null) {
            inFlight.cancel(true);
        }
        worker.shutdownNow();
        service.close();
    }

    private static BatchFailure failure(String reason) {
        return new BatchFailure("Amazon Data Firehose Sink " + reason);
    }

    private static BatchFailure uncertain(String reason) {
        return failure(reason + "; Firehose may have accepted records");
    }

    /**
     * Carries only connector-generated diagnostic text, without provider payloads or SDK cause
     * chains. It distinguishes expected sink failures from unexpected worker failures at the Future
     * boundary.
     */
    private static final class BatchFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private BatchFailure(String message) {
            super(message);
        }
    }
}
