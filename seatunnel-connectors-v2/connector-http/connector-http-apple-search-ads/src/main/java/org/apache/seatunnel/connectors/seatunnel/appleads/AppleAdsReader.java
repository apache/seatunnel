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
package org.apache.seatunnel.connectors.seatunnel.appleads;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.time.LocalDate;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Advances one bounded account scan across all selected resources.
 *
 * <p>Independent parent pages remain open while their children are visited. HTTPS runs off the
 * engine poll thread, and a collector barrier atomically orders output and saved progress.
 */
final class AppleAdsReader implements SourceReader<SeaTunnelRow, AppleAdsSplit> {
    private final AppleAdsConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private final Map<AppleAdsSplit.Frame, AppleAdsApi.Page> pages = new IdentityHashMap<>();

    // Cancel may run from an engine thread while pollNext or an HTTPS request is active.
    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean finished;
    private AppleAdsSplit progress;
    private AppleAdsHttp http;
    private AppleAdsApi api;
    private ExecutorService worker;
    private Future<AppleAdsApi.Page> pending;
    private long pendingUntil;

    AppleAdsReader(AppleAdsConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Allocates transport only for the reader that can receive the account split.
     *
     * <p>A pre-open cancellation is sticky and prevents creating any network workers.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && worker == null && context.getIndexOfSubtask() == 0) {
                http = new AppleAdsHttp(config);
                api = new AppleAdsApi(config, http);
                worker =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "apple-ads-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Resolves one page, handles a parent transition, or emits the next accepted row.
     *
     * <p>A waiting network worker does not hold the engine's checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || finished) {
                return;
            }
            if (progress == null ? noMoreSplits : progress.finished(config)) {
                finished = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        AppleAdsResource resource = config.resources.get(progress.resource);
        if (progress.stack.isEmpty()) {
            commit(
                    output,
                    () -> {
                        if (resource.report() && progress.first.isAfter(progress.last)) {
                            ++progress.resource;
                            return;
                        }
                        AppleAdsResource stage = resource.firstStage();
                        progress.stack.add(
                                new AppleAdsSplit.Frame(
                                        stage, null, null, stage.report() ? progress.first : null));
                    });
            return;
        }
        AppleAdsSplit.Frame frame = progress.stack.get(progress.stack.size() - 1);
        AppleAdsApi.Window window = frame.stage.report() ? window(frame.date) : null;
        AppleAdsApi.Page page = pages.get(frame);
        if (page == null) {
            page =
                    fetch(
                            frame.stage,
                            () ->
                                    api.page(
                                            frame.stage,
                                            frame.campaign,
                                            frame.group,
                                            window,
                                            frame.offset));
            if (page == null) {
                return;
            }
            verify(frame, page);
            pages.put(frame, page);
        }
        AppleAdsApi.Page ready = page;
        if (frame.row < ready.rows.size()) {
            AppleAdsApi.Row row = ready.rows.get(frame.row);
            commit(
                    output,
                    () -> {
                        if (frame.stage == resource) {
                            emit(output, resource, row);
                        } else {
                            AppleAdsResource child = resource.after(frame.stage);
                            progress.stack.add(
                                    new AppleAdsSplit.Frame(
                                            child,
                                            row.campaign,
                                            row.group,
                                            child.report() ? progress.first : null));
                        }
                        frame.pageDigest = ready.digest;
                        frame.total = ready.total;
                        frame.row++;
                    });
            return;
        }
        commit(
                output,
                () -> {
                    pages.remove(frame);
                    if (ready.next >= 0) {
                        frame.advance(ready, config.maxPages);
                    } else if (window != null && window.last.isBefore(progress.last)) {
                        frame.advanceWindow(window.last.plusDays(1));
                    } else {
                        progress.stack.remove(progress.stack.size() - 1);
                        if (progress.stack.isEmpty()) {
                            ++progress.resource;
                        }
                    }
                });
    }

    private AppleAdsApi.Window window(LocalDate first) {
        LocalDate last = first.plusDays(config.window - 1L);
        return new AppleAdsApi.Window(first, last.isAfter(progress.last) ? progress.last : last);
    }

    private static void verify(AppleAdsSplit.Frame frame, AppleAdsApi.Page page) {
        if (frame.total >= 0 && frame.total != page.total) {
            throw frame.stage.fail("pagination total changed within the parent or report window");
        }
        if (frame.row > page.rows.size()
                || frame.pageDigest != null && !frame.pageDigest.equals(page.digest)) {
            throw frame.stage.fail("saved page changed since checkpoint");
        }
        if (page.digest.equals(frame.precedingDigest)) {
            throw frame.stage.fail("pagination repeated provider rows");
        }
    }

    private void emit(
            Collector<SeaTunnelRow> output, AppleAdsResource resource, AppleAdsApi.Row row) {
        SeaTunnelRow event =
                new SeaTunnelRow(
                        new Object[] {
                            resource.label(),
                            config.mode.name(),
                            config.accountId,
                            String.valueOf(row.id),
                            text(row.campaign),
                            text(row.group),
                            text(row.day),
                            row.country,
                            row.json(http)
                        });
        event.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(event);
    }

    private static String text(Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * Fetches one operation off the poll thread and sanitizes unexpected worker failures.
     *
     * <p>At most one request may be pending, owned by the frame that was active when it began.
     */
    private AppleAdsApi.Page fetch(AppleAdsResource resource, Callable<AppleAdsApi.Page> task)
            throws Exception {
        Future<AppleAdsApi.Page> current;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                pendingUntil = http.deadline();
                pending = worker.submit(task);
            }
            current = pending;
        }
        try {
            AppleAdsApi.Page result = current.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : result;
            }
        } catch (TimeoutException ex) {
            if (!closed && pendingUntil - System.nanoTime() <= 0) {
                cancel();
                throw resource.fail("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.fail("read canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AppleAdsResource.Failure) {
                throw (AppleAdsResource.Failure) ex.getCause();
            }
            throw resource.fail("provider request could not be completed");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable change) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    change.run();
                }
            }
        }
    }

    /**
     * Captures the complete active traversal after the latest row accepted by the collector.
     *
     * <p>An in-flight request has no effect on the saved position until its result is consumed.
     */
    @Override
    public List<AppleAdsSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? Collections.emptyList() : List.of(progress.copy());
        }
    }

    /**
     * Installs the account split after checking mode, account and date configuration identity.
     *
     * <p>Assignment has no provider side effects and the recovered cutoffs remain fixed.
     */
    @Override
    public void addSplits(List<AppleAdsSplit> incoming) {
        synchronized (checkpoint) {
            if (incoming.isEmpty()) {
                return;
            }
            if (incoming.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid AppleAds split assignment");
            }
            AppleAdsSplit assigned = incoming.get(0);
            assigned.validate(config);
            progress = assigned.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Cancels a pending read and closes its active TLS connection.
     *
     * <p>The hook is idempotent and does not acquire the engine's checkpoint lock.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (http != null) {
                http.close();
            }
            if (pending != null) {
                pending.cancel(true);
            }
            if (worker != null) {
                worker.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }
}
