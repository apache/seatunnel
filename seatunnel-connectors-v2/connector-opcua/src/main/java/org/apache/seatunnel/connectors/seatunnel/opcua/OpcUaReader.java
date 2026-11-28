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

package org.apache.seatunnel.connectors.seatunnel.opcua;

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

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaFailure.problem;

/** Collects only on the engine thread. Slow SDK calls run without holding the checkpoint lock. */
final class OpcUaReader implements SourceReader<SeaTunnelRow, OpcUaSplit> {
    private final Context context;
    private final OpcUaSettings settings;
    private final OpcUaAcquisition acquisition;
    private final Object stateLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(OpcUaResources.threads("opcua-reader"));
    private volatile Future<OpcUaAcquisition.Batch> pending;
    private volatile boolean stopped;
    private OpcUaSplit split;
    private long deadline;
    private long nextRequest;
    private boolean signaled;

    OpcUaReader(Context context, OpcUaSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        acquisition = new OpcUaAcquisition(settings, tableId);
    }

    /** Network work starts after split assignment and stays on the acquisition worker. */
    @Override
    public void open() {}

    /** The engine thread emits completed batches and leaves pending UA requests cancellable. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws Exception {
        if (stopped || signaled) {
            return;
        }
        OpcUaSplit position;
        synchronized (stateLock) {
            position = split;
        }
        if (position == null) {
            Thread.sleep(10L);
            return;
        }
        if (position.finished) {
            signaled = true;
            context.signalNoMoreElement();
            return;
        }
        if (pending == null) {
            if (nextRequest != 0L && System.nanoTime() - nextRequest < 0L) {
                Thread.sleep(10L);
                return;
            }
            submit(position);
        } else if (pending.isDone()) {
            publish(collector);
        } else if (System.nanoTime() - deadline >= 0L) {
            cancel();
            throw problem("operation deadline exceeded");
        } else {
            Thread.sleep(10L);
        }
    }

    private void submit(OpcUaSplit position) throws OpcUaFailure {
        long due = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.requestTimeout);
        deadline = due;
        try {
            pending =
                    worker.submit(
                            () -> {
                                OpcUaAcquisition.Batch batch =
                                        acquisition.next(position.bindings, due);
                                if (System.nanoTime() - due >= 0L) {
                                    throw problem("operation deadline exceeded");
                                }
                                return batch;
                            });
        } catch (RejectedExecutionException e) {
            if (!stopped) {
                throw problem("could not schedule acquisition");
            }
        }
    }

    private void publish(Collector<SeaTunnelRow> collector) throws Exception {
        OpcUaAcquisition.Batch batch;
        try {
            batch = pending.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof OpcUaFailure) {
                throw (OpcUaFailure) e.getCause();
            }
            throw problem("acquisition failed");
        } catch (CancellationException e) {
            if (stopped) {
                return;
            }
            throw problem("acquisition was cancelled");
        }
        synchronized (collector.getCheckpointLock()) {
            synchronized (stateLock) {
                if (!stopped) {
                    int count = batch.rows.size();
                    long emitted = split.emitted;
                    if (settings.maxRecords > 0L) {
                        count = (int) Math.min(count, settings.maxRecords - emitted);
                        emitted += count;
                    }
                    for (int i = 0; i < count; i++) {
                        collector.collect(batch.rows.get(i));
                    }
                    boolean finished =
                            settings.mode == OpcUaSettings.ReadMode.BROWSE_NODES
                                    || (settings.maxRecords > 0L && emitted == settings.maxRecords);
                    split = new OpcUaSplit(settings.stateKey, batch.binding, emitted, finished);
                }
            }
        }
        pending = null;
        int pause = settings.mode == OpcUaSettings.ReadMode.POLLING ? settings.interval : 10;
        nextRequest = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pause);
    }

    /** Capture published progress and namespace bindings from the same reader transition. */
    @Override
    public List<OpcUaSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return split == null ? Collections.emptyList() : Collections.singletonList(split);
        }
    }

    @Override
    public void addSplits(List<OpcUaSplit> splits) {
        synchronized (stateLock) {
            for (OpcUaSplit restored : splits) {
                if (split != null || !settings.stateKey.equals(restored.stateKey)) {
                    throw OpcUaSettings.invalid("received an unexpected or duplicate OPC UA split");
                }
                split = restored;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // Empty before assignment and the terminal OPC UA split are distinct states.
    }

    /** Cancel pending protocol work without waiting for the engine checkpoint lock. */
    @Override
    public void cancel() {
        stopped = true;
        Future<OpcUaAcquisition.Batch> fetch = pending;
        if (fetch != null) {
            fetch.cancel(true);
        }
        worker.shutdownNow();
        acquisition.close();
    }

    /** Release the reader-owned worker and channel resources after completion or failure. */
    @Override
    public void close() {
        cancel();
    }
}
