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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Traverses the fixed profile scope with one bounded request in flight. The checkpoint lock covers
 * emission and progress only, including an independent report-ID commit before a download can
 * start.
 */
final class AmazonAdsReader implements SourceReader<SeaTunnelRow, AmazonAdsSplit> {
    private static final int CHUNK = 16;
    private final Context context;
    private final AmazonAdsConfig config;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private boolean idle;
    private boolean done;
    private AmazonAdsSplit position;
    private AmazonAdsTransport transport;
    private AmazonAdsDirect direct;
    private AmazonAdsReports reports;
    private ExecutorService worker;
    private Future<Object> pending;
    private long deadline;
    private AmazonAdsDirect.Page profiles;
    private AmazonAdsDirect.Page directPage;
    private AmazonAdsDirect.Page recommendationPage;
    private AmazonAdsReportSpool reportSpool;

    AmazonAdsReader(AmazonAdsConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Start product-owned network workers only for the active split reader.
     *
     * <p>Other reader subtasks remain idle and do not establish AWS or token connections.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (closed || worker != null || context.getIndexOfSubtask() != 0) {
                return;
            }
            transport = new AmazonAdsTransport(config);
            direct = new AmazonAdsDirect(config, transport);
            reports = new AmazonAdsReports(config, transport);
            worker =
                    Executors.newSingleThreadExecutor(
                            task -> AmazonAdsTransport.daemon(task, "amazonads-reader"));
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || done) {
                return;
            }
            if (position == null && idle || position != null && position.finished) {
                done = true;
                context.signalNoMoreElement();
                return;
            }
            if (position == null) {
                return;
            }
        }
        if (profiles == null) {
            AmazonAdsResource resource = AmazonAdsResource.PROFILES;
            profiles =
                    fetch(
                            resource,
                            config.requestMs,
                            AmazonAdsDirect.Page.class,
                            limit ->
                                    direct.fetch(
                                            resource,
                                            null,
                                            null,
                                            position.cutoff,
                                            new AmazonAdsSplit.Cursor(),
                                            limit));
            if (profiles == null) {
                return;
            }
            if (position.profile > profiles.rows.size()
                    || position.profileDigest != null
                            && !position.profileDigest.equals(profiles.digest)) {
                throw resource.fail("saved profile discovery changed");
            }
        }
        if (position.profile == profiles.rows.size()) {
            commit(
                    output,
                    () -> {
                        position.profileDigest = profiles.digest;
                        position.finished = true;
                    });
            return;
        }
        AmazonAdsRecord profileRow = profiles.rows.get(position.profile);
        AmazonAdsPartition profile = new AmazonAdsPartition(config, profileRow);
        if (!position.profileEmitted) {
            commit(
                    output,
                    () -> {
                        position.profileDigest = profiles.digest;
                        if (config.selected.contains(AmazonAdsResource.PROFILES)) {
                            emit(
                                    output,
                                    AmazonAdsResource.PROFILES,
                                    profileRow.id,
                                    profile.profile,
                                    null,
                                    null,
                                    profileRow.value.toString());
                        }
                        position.profileEmitted = true;
                    });
            return;
        }
        if (!profile.selected || position.resource == config.profileResources.size()) {
            commit(output, position::nextProfile);
            return;
        }
        AmazonAdsResource resource = config.profileResources.get(position.resource);
        if (resource.kind == AmazonAdsResource.Kind.REPORT) {
            readReport(output, profile, resource);
        } else {
            readDirect(output, profile, resource);
        }
    }

    private void readDirect(
            Collector<SeaTunnelRow> output, AmazonAdsPartition profile, AmazonAdsResource resource)
            throws Exception {
        boolean nested =
                resource == AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS
                        && !config.adGroupResources.isEmpty();
        if (directPage == null) {
            directPage =
                    fetch(
                            resource,
                            config.requestMs,
                            AmazonAdsDirect.Page.class,
                            limit ->
                                    direct.fetch(
                                            resource,
                                            profile,
                                            null,
                                            position.cutoff,
                                            position.page,
                                            limit));
            if (directPage == null) {
                return;
            }
            verifyPage(resource, directPage, position.page);
            if (nested
                    && (position.group > directPage.rows.size()
                            || position.groupEmitted && position.group == directPage.rows.size()
                            || position.groupDigest != null
                                    && !position.groupDigest.equals(directPage.digest))) {
                throw resource.fail("saved ad-group page changed");
            }
        }
        if (nested
                && position.groupEmitted
                && position.recommendation < config.adGroupResources.size()
                && recommendationPage == null) {
            AmazonAdsRecord group = directPage.rows.get(position.group);
            AmazonAdsResource child = config.adGroupResources.get(position.recommendation);
            recommendationPage =
                    fetch(
                            child,
                            config.requestMs,
                            AmazonAdsDirect.Page.class,
                            limit ->
                                    direct.fetch(
                                            child,
                                            profile,
                                            group,
                                            position.cutoff,
                                            position.child,
                                            limit));
            if (recommendationPage == null) {
                return;
            }
            verifyPage(child, recommendationPage, position.child);
        }
        commit(output, () -> consumeDirect(output, profile, resource, nested));
    }

    private void consumeDirect(
            Collector<SeaTunnelRow> output,
            AmazonAdsPartition profile,
            AmazonAdsResource resource,
            boolean nested) {
        if (nested) {
            collectAdGroup(output, profile);
            if (position.group < directPage.rows.size()) {
                return;
            }
        } else {
            emitPage(output, profile, resource, directPage, position.page, null);
            if (position.page.row < directPage.rows.size()) {
                return;
            }
        }
        if (!closed) {
            if (directPage.more) {
                position.page.advance(directPage);
                position.clearAdGroupPage();
            } else {
                position.nextResource();
            }
            directPage = null;
        }
    }

    private void collectAdGroup(Collector<SeaTunnelRow> output, AmazonAdsPartition profile) {
        if (position.group == directPage.rows.size()) {
            return;
        }
        AmazonAdsRecord group = directPage.rows.get(position.group);
        if (!position.groupEmitted) {
            position.groupDigest = directPage.digest;
            if (config.selected.contains(AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS)) {
                emit(
                        output,
                        AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS,
                        group.id,
                        profile.profile,
                        null,
                        null,
                        group.value.toString());
            }
            position.groupEmitted = true;
            return;
        }
        if (position.recommendation == config.adGroupResources.size()) {
            position.group++;
            position.clearAdGroup();
            return;
        }
        AmazonAdsResource resource = config.adGroupResources.get(position.recommendation);
        emitPage(output, profile, resource, recommendationPage, position.child, group.id);
        if (!closed && position.child.row == recommendationPage.rows.size()) {
            if (recommendationPage.more) {
                position.child.advance(recommendationPage);
            } else {
                position.recommendation++;
                position.child = new AmazonAdsSplit.Cursor();
            }
            recommendationPage = null;
        }
    }

    private void emitPage(
            Collector<SeaTunnelRow> output,
            AmazonAdsPartition profile,
            AmazonAdsResource resource,
            AmazonAdsDirect.Page page,
            AmazonAdsSplit.Cursor cursor,
            String parent) {
        int end = Math.min(page.rows.size(), cursor.row + CHUNK);
        AmazonAdsPartition.Window window =
                resource.kind == AmazonAdsResource.Kind.ATTRIBUTION
                        ? profile.attribution(config, resource, position.cutoff)
                        : null;
        while (!closed && cursor.row < end) {
            AmazonAdsRecord item = page.rows.get(cursor.row);
            emit(output, resource, item.id, profile.profile, parent, window, item.value.toString());
            cursor.digest = page.digest;
            cursor.row++;
        }
    }

    private void readReport(
            Collector<SeaTunnelRow> output, AmazonAdsPartition profile, AmazonAdsResource resource)
            throws Exception {
        AmazonAdsPartition.Window window =
                AmazonAdsPartition.firstReport(
                        config, resource, position.cutoff, position.reportFrom);
        if (window == null) {
            commit(output, position::nextResource);
            return;
        }
        ObjectNode request = reports.request(resource, window);
        String fingerprint = AmazonAdsReports.fingerprint(resource, profile.profile, request);
        if (position.reportId == null) {
            long creationBudget =
                    Math.min(
                            config.reportMs,
                            (long) config.requestMs * (2 * config.attempts)
                                    + (long) config.duplicateMs * (config.attempts - 1));
            String created =
                    fetch(
                            resource,
                            creationBudget,
                            String.class,
                            limit -> reports.create(resource, profile.profile, request, limit));
            if (created != null) {
                commit(
                        output,
                        () -> {
                            position.reportFrom = window.from;
                            position.reportId = created;
                            position.reportFingerprint = fingerprint;
                            position.reportStarted = System.currentTimeMillis();
                        });
            }
            return;
        }
        if (!fingerprint.equals(position.reportFingerprint)) {
            throw resource.fail("saved report definition no longer matches the request");
        }
        if (reportSpool == null) {
            long budget = (long) config.reportMs + config.downloadMs + config.requestMs;
            reportSpool =
                    fetch(
                            resource,
                            budget,
                            AmazonAdsReportSpool.class,
                            limit ->
                                    reports.awaitDownload(
                                            resource,
                                            profile.profile,
                                            window,
                                            request,
                                            position.reportId,
                                            position.reportStarted,
                                            limit));
            if (reportSpool == null) {
                return;
            }
            if (position.reportRows > reportSpool.rows
                    || position.reportDigest != null
                            && !position.reportDigest.equals(reportSpool.digest)) {
                throw resource.fail("saved report content changed");
            }
            reportSpool.seek(position.reportOffset);
        }
        commit(
                output,
                () -> {
                    int stop = Math.min(reportSpool.rows, position.reportRows + CHUNK);
                    while (!closed && position.reportRows < stop) {
                        byte[] row = reportSpool.nextRow();
                        emit(
                                output,
                                resource,
                                fingerprint + ":" + position.reportRows,
                                profile.profile,
                                null,
                                window,
                                new String(row, StandardCharsets.UTF_8));
                        position.reportRows++;
                        position.reportOffset = reportSpool.offset();
                        position.reportDigest = reportSpool.digest;
                    }
                    if (!closed && position.reportRows == reportSpool.rows) {
                        transport.release(reportSpool);
                        reportSpool = null;
                        position.reportFrom = window.to.plusDays(1);
                        position.clearReport();
                    }
                });
    }

    private <T> T fetch(
            AmazonAdsResource resource, long milliseconds, Class<T> type, NetworkTask<T> task)
            throws Exception {
        Future<Object> inFlight;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(milliseconds);
                deadline = expires;
                pending = worker.submit(() -> task.run(expires));
            }
            inFlight = pending;
        }
        try {
            Object result = inFlight.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
            }
            return closed ? null : type.cast(result);
        } catch (TimeoutException ex) {
            if (!closed && deadline - System.nanoTime() <= 0) {
                cancel();
                throw resource.fail("operation deadline exceeded");
            }
            return null;
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AmazonAdsResource.Failure) {
                throw (AmazonAdsResource.Failure) ex.getCause();
            }
            throw resource.fail("request could not be completed");
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.fail("request canceled");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable advance) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    advance.run();
                }
            }
        }
    }

    private static void verifyPage(
            AmazonAdsResource resource, AmazonAdsDirect.Page page, AmazonAdsSplit.Cursor saved) {
        if (saved.row > page.rows.size()
                || saved.digest != null && !saved.digest.equals(page.digest)) {
            throw resource.fail("saved page changed");
        }
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            AmazonAdsResource resource,
            String id,
            String profile,
            String parent,
            AmazonAdsPartition.Window window,
            String data) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            resource.label(),
                            id,
                            profile,
                            parent,
                            window == null ? null : window.from.toString(),
                            window == null ? null : window.to.toString(),
                            data
                        });
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    @Override
    public List<AmazonAdsSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    @Override
    public void addSplits(List<AmazonAdsSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid AmazonAds reader assignment");
            }
            AmazonAdsSplit incoming = splits.get(0);
            incoming.validate(config);
            position = incoming.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            idle = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Interrupt cancellation independently of the emission lock and close any socket or spool.
     *
     * <p>The engine can cancel a blocked network read without waiting for a checkpoint barrier.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (transport != null) {
                transport.close();
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

    /**
     * Represents a single bounded network phase whose result is committed by the engine reader.
     *
     * <p>Its executor never advances a report or page checkpoint on its own.
     */
    private interface NetworkTask<T> {
        T run(long deadline) throws InterruptedException;
    }
}
