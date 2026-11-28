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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustReportClient.problem;

/** Runs at most one report day at a time and emits its rows with its checkpoint transition. */
final class AdjustReader implements SourceReader<SeaTunnelRow, AdjustScan> {
    private final Context context;
    private final AdjustReportPlan plan;
    private final AdjustReportClient client;
    private final AdjustReportRows rows;
    private final Object checkpoint = new Object();
    private final ExecutorService reports =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-adjust-report");
                        thread.setDaemon(true);
                        return thread;
                    });

    private volatile boolean cancelled;
    private volatile Future<List<SeaTunnelRow>> pending;
    private AdjustScan scan;
    private long deadline;
    private boolean finished;

    AdjustReader(Context context, AdjustReportPlan plan, String tableId) {
        this.context = context;
        this.plan = plan;
        client = new AdjustReportClient(plan);
        rows = new AdjustReportRows(plan, tableId);
    }

    @Override
    public void open() {}

    /** Holds no network wait inside the collector's checkpoint lock. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws Exception {
        if (cancelled || finished) {
            return;
        }
        AdjustScan position;
        synchronized (checkpoint) {
            position = scan;
        }
        if (position == null) {
            Thread.sleep(10);
        } else if (position.finished()) {
            finished = true;
            context.signalNoMoreElement();
        } else if (pending == null) {
            request(position.next);
        } else if (pending.isDone()) {
            publish(collector);
        } else {
            if (System.nanoTime() - deadline >= 0) {
                cancel();
                throw problem("report day deadline exceeded");
            }
            Thread.sleep(10);
        }
    }

    private void request(LocalDate day) throws AdjustReportClient.ReportFailure {
        long due = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(plan.timeoutMillis);
        deadline = due;
        try {
            pending =
                    reports.submit(
                            () -> {
                                List<SeaTunnelRow> result =
                                        rows.convert(day, client.fetch(day, due));
                                AdjustReportClient.remainingMillis(due);
                                return result;
                            });
        } catch (RejectedExecutionException e) {
            if (!cancelled) {
                throw problem("could not start report request");
            }
        }
    }

    private void publish(Collector<SeaTunnelRow> collector) throws Exception {
        List<SeaTunnelRow> records;
        try {
            records = pending.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AdjustReportClient.ReportFailure) {
                throw (AdjustReportClient.ReportFailure) e.getCause();
            }
            throw problem("report request failed");
        } catch (java.util.concurrent.CancellationException e) {
            if (cancelled) {
                return;
            }
            throw problem("report request was cancelled");
        }
        synchronized (collector.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!cancelled) {
                    records.forEach(collector::collect);
                    scan = scan.advance();
                    pending = null;
                }
            }
        }
    }

    @Override
    public List<AdjustScan> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return scan == null ? Collections.emptyList() : Collections.singletonList(scan);
        }
    }

    @Override
    public void addSplits(List<AdjustScan> splits) {
        synchronized (checkpoint) {
            for (AdjustScan restored : splits) {
                if (scan != null || !plan.signature().equals(restored.signature)) {
                    throw AdjustReportPlan.invalid("received an incompatible report scan");
                }
                scan = restored;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // The assigned split, including a restored terminal split, defines the scan's end.
    }

    /** Interrupts the worker and closes its connection independently of collector locks. */
    @Override
    public void cancel() {
        cancelled = true;
        client.close();
        Future<List<SeaTunnelRow>> request = pending;
        if (request != null) {
            request.cancel(true);
        }
        reports.shutdownNow();
    }

    @Override
    public void close() {
        cancel();
    }
}
