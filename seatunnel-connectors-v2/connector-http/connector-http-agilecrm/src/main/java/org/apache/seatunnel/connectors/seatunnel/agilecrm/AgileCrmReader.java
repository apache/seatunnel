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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmClient.fail;

/** Polls one remote page at a time without holding the checkpoint lock during I/O. */
final class AgileCrmReader implements SourceReader<SeaTunnelRow, AgileCrmTraversal> {
    private final AgileCrmSettings config;
    private final Context context;
    private final String tableId;
    private final AgileCrmClient http;
    private final Object traversalLock = new Object();
    private final ExecutorService requests =
            Executors.newSingleThreadExecutor(
                    work -> {
                        Thread thread = new Thread(work, "seatunnel-agilecrm-http");
                        thread.setDaemon(true);
                        return thread;
                    });

    private AgileCrmTraversal traversal;
    private volatile Future<AgileCrmPage> inFlight;
    private AgileCrmResource requestResource;
    private long until;
    private volatile boolean stopping;
    private boolean endReported;

    AgileCrmReader(AgileCrmSettings config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new AgileCrmClient(config);
    }

    @Override
    public void open() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (stopping || endReported) {
            return;
        }
        boolean missing;
        boolean completed;
        synchronized (traversalLock) {
            missing = traversal == null;
            completed = !missing && traversal.finished;
            if (!missing && !completed && inFlight == null) {
                submit(traversal.nextRequest(config), traversal.parentId(), traversal.discovery);
                return;
            }
        }
        if (completed) {
            endReported = true;
            context.signalNoMoreElement();
        } else if (!missing && inFlight.isDone()) {
            collectPage(output);
        } else {
            if (!missing && System.nanoTime() - until >= 0L) {
                cancel();
                throw fail(requestResource, "request deadline exceeded");
            }
            Thread.sleep(10);
        }
    }

    private void submit(AgileCrmResource.Request request, Long parent, boolean discovering)
            throws AgileCrmClient.ReadFailure {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.timeoutMillis);
        until = deadline;
        requestResource = request.resource;
        try {
            inFlight =
                    requests.submit(
                            () -> {
                                AgileCrmPage page =
                                        AgileCrmPage.parse(
                                                config,
                                                request.resource,
                                                parent,
                                                discovering,
                                                tableId,
                                                http.retrieve(request, deadline));
                                AgileCrmClient.timeRemaining(request.resource, deadline);
                                return page;
                            });
        } catch (RejectedExecutionException e) {
            if (!stopping) {
                throw fail(request.resource, "could not schedule a request");
            }
        }
    }

    private void collectPage(Collector<SeaTunnelRow> output) throws Exception {
        AgileCrmPage page;
        try {
            page = inFlight.get();
        } catch (CancellationException e) {
            if (stopping) {
                return;
            }
            throw fail(requestResource, "request cancelled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AgileCrmClient.ReadFailure) {
                throw (AgileCrmClient.ReadFailure) e.getCause();
            }
            throw fail(requestResource, "request failed");
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (traversalLock) {
                if (!stopping) {
                    String digest = traversal.checkNext(requestResource, page.next());
                    if (traversal.discovery) {
                        traversal.discover(requestResource, page.parents);
                    } else {
                        page.rows.forEach(output::collect);
                    }
                    traversal.advance(config.resource, page.next(), digest);
                    inFlight = null;
                }
            }
        }
    }

    @Override
    public List<AgileCrmTraversal> snapshotState(long checkpointId) {
        synchronized (traversalLock) {
            return traversal == null
                    ? Collections.emptyList()
                    : Collections.singletonList(traversal.snapshot());
        }
    }

    @Override
    public void addSplits(List<AgileCrmTraversal> splits) {
        synchronized (traversalLock) {
            for (AgileCrmTraversal restored : splits) {
                if (traversal != null || !config.stateKey().equals(restored.stateKey)) {
                    throw AgileCrmSettings.invalid(
                            "received an incompatible or duplicate scan split");
                }
                traversal = restored;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // A checkpoint with no assigned split must still wait. An assigned terminal split ends.
    }

    @Override
    public void cancel() {
        stopping = true;
        http.close();
        Future<AgileCrmPage> request = inFlight;
        if (request != null) {
            request.cancel(true);
        }
        requests.shutdownNow();
    }

    @Override
    public void close() {
        cancel();
    }
}
