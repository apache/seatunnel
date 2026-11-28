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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Demands one bounded observation at a time off the checkpoint monitor. Accepted rows return to
 * native engine handoff; inherited null split state deliberately replays a full scan on restore.
 */
final class CbsStatLineReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int HTTP_TIMEOUT_MS = 30000;
    private final URI start;
    private final String dataset;
    private final int maxRows;
    private final SingleSplitReaderContext context;
    private final CbsStatLineCursor.Limits limits;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    // Publication before the sticky cancellation check prevents a late unowned request.
    private final AtomicReference<HttpGet> inFlight = new AtomicReference<>();
    private volatile Future<SeaTunnelRow> pending;
    private CloseableHttpClient client;
    private ExecutorService worker;
    private ScheduledExecutorService timer;
    private CbsStatLineCursor cursor;
    // This local count is not checkpointed: restored null splits retain whole-scan replay.
    private long emitted;

    CbsStatLineReader(URI start, String dataset, int maxRows, SingleSplitReaderContext context) {
        this(start, dataset, maxRows, context, CbsStatLineCursor.Limits.defaults());
    }

    CbsStatLineReader(
            URI start,
            String dataset,
            int maxRows,
            SingleSplitReaderContext context,
            CbsStatLineCursor.Limits limits) {
        this.start = start;
        this.dataset = dataset;
        this.maxRows = maxRows;
        this.context = context;
        this.limits = limits;
    }

    /**
     * Owns one client, worker and timer. The registry retains system TLS/protocol/cipher settings;
     * useSystemProperties retains native proxy routing while headers gain a finite admission bound.
     */
    @Override
    public void open() throws IOException {
        PoolingHttpClientConnectionManager manager =
                new PoolingHttpClientConnectionManager(
                        RegistryBuilder.<ConnectionSocketFactory>create()
                                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                                .register(
                                        "https",
                                        SSLConnectionSocketFactory.getSystemSocketFactory())
                                .build());
        manager.setDefaultConnectionConfig(
                ConnectionConfig.custom()
                        .setMessageConstraints(
                                MessageConstraints.custom()
                                        .setMaxHeaderCount(64)
                                        .setMaxLineLength(8192)
                                        .build())
                        .build());
        try {
            client =
                    HttpClients.custom()
                            .useSystemProperties()
                            .setConnectionManager(manager)
                            .setRequestExecutor(new CbsStatLineRequestExecutor())
                            .disableRedirectHandling()
                            .disableAutomaticRetries()
                            .disableCookieManagement()
                            .setDefaultRequestConfig(
                                    RequestConfig.custom()
                                            .setConnectTimeout(HTTP_TIMEOUT_MS)
                                            .setSocketTimeout(HTTP_TIMEOUT_MS)
                                            .setConnectionRequestTimeout(HTTP_TIMEOUT_MS)
                                            .build())
                            .build();
            worker =
                    Executors.newSingleThreadExecutor(
                            task -> {
                                Thread thread = new Thread(task, "cbs-statline-source-fetch");
                                thread.setDaemon(true);
                                return thread;
                            });
            timer =
                    Executors.newSingleThreadScheduledExecutor(
                            task -> {
                                Thread thread = new Thread(task, "cbs-statline-source-deadline");
                                thread.setDaemon(true);
                                return thread;
                            });
            cursor =
                    new CbsStatLineCursor(
                            client, start, dataset, timer, cancelled, inFlight, limits);
        } catch (RuntimeException error) {
            manager.close();
            try {
                close();
            } catch (IOException cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
    }

    /**
     * Never waits on remote work and accepts at most one row per call. Network/parser ownership
     * remains on the worker while only collection and local completion use the checkpoint monitor.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        if (cancelled.get() || noMoreSplits) {
            return;
        }
        Future<SeaTunnelRow> operation = pending;
        if (operation == null) {
            try {
                pending = worker.submit(cursor::next);
            } catch (RejectedExecutionException error) {
                if (cancelled.get()) {
                    return;
                }
                throw new IOException("CBS OData fetch worker is unavailable");
            }
            if (cancelled.get()) {
                pending.cancel(true);
            }
            return;
        }
        if (!operation.isDone()) {
            return;
        }
        SeaTunnelRow row;
        try {
            row = operation.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("CBS OData reader interrupted");
        } catch (CancellationException error) {
            if (cancelled.get()) {
                return;
            }
            throw new IOException("CBS OData read unexpectedly canceled");
        } catch (ExecutionException error) {
            if (cancelled.get()) {
                return;
            }
            Throwable cause = error.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("CBS OData read failed (details withheld)");
        }
        pending = null;
        if (cancelled.get()) {
            return;
        }
        synchronized (output.getCheckpointLock()) {
            if (cancelled.get()) {
                return;
            }
            if (row != null) {
                output.collect(row);
                emitted++;
            }
            noMoreSplits = row == null || maxRows > 0 && emitted >= maxRows;
        }
        if (noMoreSplits && !cancelled.get()) {
            // Abort before closing unread entity data; max_rows never drains/validates its suffix.
            cursor.close();
            context.signalNoMoreElement();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws IOException {
        pollNext(output);
    }

    /**
     * Cancellation never waits for the collector monitor. Sticky cancellation and request abort
     * suppress late response delivery even when the queued worker has not dispatched yet.
     */
    @Override
    public void cancel() {
        cancelled.set(true);
        HttpGet request = inFlight.get();
        if (request != null) {
            request.abort();
        }
        Future<SeaTunnelRow> operation = pending;
        if (operation != null) {
            operation.cancel(true);
        }
    }

    /**
     * Releases all owned resources after abort, preserving a primary cleanup failure and still
     * stopping both executors. Active parser cleanup remains on its worker.
     */
    @Override
    public void close() throws IOException {
        // Reverse resource-close order keeps abort/parser/client cleanup before executor shutdown.
        try (Closeable stopTimer =
                        () -> {
                            if (timer != null) {
                                timer.shutdownNow();
                            }
                        };
                Closeable stopWorker =
                        () -> {
                            if (worker != null) {
                                worker.shutdownNow();
                            }
                        };
                Closeable closeClient =
                        () -> {
                            if (client != null) {
                                client.close();
                            }
                        };
                Closeable closeCursor =
                        () -> {
                            if (cursor != null) {
                                cursor.close();
                            }
                        }) {
            cancel();
        }
    }

    /*
     * Resolves opaque continuation links, including query-only references, without decoding their cursors.
     * The target must retain the current scheme, host, port and dataset path, without user info or fragments.
     */
    static URI continuation(URI current, String value) throws IOException {
        final URI link;
        try {
            URI reference = URI.create(value);
            // URI.resolve treats query-only references like relative path segments.
            link =
                    value.startsWith("?")
                            ? URI.create(
                                    current.getScheme()
                                            + "://"
                                            + current.getRawAuthority()
                                            + current.getRawPath()
                                            + value)
                            : current.resolve(reference);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid CBS OData continuation URL");
        }
        if (value.isEmpty()
                || !current.getScheme().equalsIgnoreCase(link.getScheme())
                || !current.getHost().equalsIgnoreCase(link.getHost())
                || port(current) != port(link)
                || link.getRawUserInfo() != null
                || link.getRawFragment() != null
                || !current.getRawPath().equals(link.getRawPath())) {
            throw new IOException("CBS OData continuation must target the same dataset and origin");
        }
        return link;
    }

    private static int port(URI uri) {
        return uri.getPort() < 0
                ? ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80)
                : uri.getPort();
    }
}
