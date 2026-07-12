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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileCompareMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileDiscoveryMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FilePostSyncAction;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileStartMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSyncMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileUpdateStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceOperationState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A continuous split enumerator that keeps scanning the source path and assigns new/changed files
 * to readers at runtime.
 *
 * <p>This enumerator is designed to reuse the existing {@code sync_mode=update} semantics for
 * incremental/dedup behavior, and does not maintain an unbounded "seen" state.
 */
@Slf4j
public class ContinuousMultipleTableFileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final int DEFAULT_ASSIGN_BATCH_SIZE = 32;
    private static final String METRIC_POST_SYNC_SUBMITTED = "post_sync_operations_submitted";
    private static final String METRIC_POST_SYNC_SUCCEEDED = "post_sync_operations_succeeded";
    private static final String METRIC_POST_SYNC_FAILED = "post_sync_operations_failed";
    private static final String METRIC_POST_SYNC_STALE_SKIPPED =
            "post_sync_operations_stale_skipped";
    private static final String METRIC_RETENTION_DELETED = "retention_deleted_files";
    private static final String METRIC_RETENTION_FAILED = "retention_failed_operations";
    private static final Pattern BACKUP_VERSION_SUFFIX_PATTERN =
            Pattern.compile("^.+\\.v\\d+_\\d+$");

    private final Context<FileSourceSplit> context;
    private final List<TableScanContext> tableScanContexts;
    private final FileStartMode startMode;
    private final Duration scanInterval;
    private final long jobStartTimeMillis;

    private final Object lock = new Object();
    private final Deque<FileSourceSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> pendingSplitIds = new HashSet<>();
    private final Map<String, SplitVersion> pendingSplitVersions = new HashMap<>();
    private final Set<Integer> readersAwaitingSplit = new HashSet<>();
    // Tracks the latest queued/completed source file version to prevent duplicate re-queue
    // before the target side catches up (e.g. short scan interval with distcp update mode).
    private final Map<String, SplitVersion> knownSplitVersions = new HashMap<>();
    private final Map<String, InFlightSplitContext> inFlightSplitContexts = new HashMap<>();
    private final List<FileSourceOperationState> finishedAwaitingCheckpoint = new ArrayList<>();
    private final NavigableMap<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint =
            new TreeMap<>();
    private final Map<String, Long> retentionLastRunMillisByPath = new HashMap<>();
    private Set<FileSourceSplit> inFlightSplits;

    private final Counter postSyncSubmittedCounter;
    private final Counter postSyncSucceededCounter;
    private final Counter postSyncFailedCounter;
    private final Counter postSyncStaleSkippedCounter;
    private final Counter retentionDeletedCounter;
    private final Counter retentionFailedCounter;

    private ScheduledExecutorService scheduler;
    private volatile boolean closed;

    public ContinuousMultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy) {
        this(
                context,
                multipleTableFileSourceConfig,
                fileSplitStrategy,
                new FileSourceState(new HashSet<>()));
    }

    public ContinuousMultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy,
            FileSourceState checkpointState) {
        this.context = context;
        MetricsContext metricsContext = null;
        try {
            metricsContext = context.getMetricsContext();
        } catch (Exception e) {
            log.warn("Unable to initialize metrics context for file source enumerator.", e);
        }
        this.postSyncSubmittedCounter = initCounter(metricsContext, METRIC_POST_SYNC_SUBMITTED);
        this.postSyncSucceededCounter = initCounter(metricsContext, METRIC_POST_SYNC_SUCCEEDED);
        this.postSyncFailedCounter = initCounter(metricsContext, METRIC_POST_SYNC_FAILED);
        this.postSyncStaleSkippedCounter =
                initCounter(metricsContext, METRIC_POST_SYNC_STALE_SKIPPED);
        this.retentionDeletedCounter = initCounter(metricsContext, METRIC_RETENTION_DELETED);
        this.retentionFailedCounter = initCounter(metricsContext, METRIC_RETENTION_FAILED);

        this.jobStartTimeMillis =
                checkpointState.getDiscoveryStartTimeMillis() > 0
                        ? checkpointState.getDiscoveryStartTimeMillis()
                        : System.currentTimeMillis();
        Set<FileSourceSplit> restoredInFlightSplits =
                new HashSet<>(checkpointState.getAssignedSplit());
        this.inFlightSplits = new HashSet<>();

        List<BaseFileSourceConfig> fileSourceConfigs =
                multipleTableFileSourceConfig.getFileSourceConfigs();
        validateContinuousDiscoveryConfig(fileSourceConfigs);

        this.scanInterval =
                resolveGlobalOption(fileSourceConfigs, FileBaseSourceOptions.SCAN_INTERVAL);
        this.startMode = resolveGlobalOption(fileSourceConfigs, FileBaseSourceOptions.START_MODE);

        this.tableScanContexts = new ArrayList<>(fileSourceConfigs.size());
        for (BaseFileSourceConfig cfg : fileSourceConfigs) {
            this.tableScanContexts.add(new TableScanContext(cfg, fileSplitStrategy));
        }

        recoverSplitsFromCheckpoint(restoredInFlightSplits);
        restorePendingOpsFromCheckpoint(checkpointState.getPendingOpsByCheckpoint());
        restoreRetentionCursor(checkpointState.getRetentionLastRunMillisByPath());
    }

    @Override
    public void open() {
        log.info(
                "Continuous discovery enabled: interval={}, start_mode={}, parallelism={}",
                scanInterval,
                startMode.name().toLowerCase(Locale.ROOT),
                context.currentParallelism());

        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "file-source-scan");
                            thread.setDaemon(true);
                            return thread;
                        });
        scheduler.scheduleWithFixedDelay(
                this::safeScanOnce,
                0L,
                Math.max(1L, scanInterval.toMillis()),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        // Assign splits on demand via handleSplitRequest.
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Continuous discovery scheduler does not terminate in 5 seconds.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (TableScanContext ctx : tableScanContexts) {
            ctx.close();
        }
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (FileSourceSplit split : splits) {
                inFlightSplits.remove(split);
                InFlightSplitContext inFlightSplitContext =
                        inFlightSplitContexts.remove(split.splitId());
                SplitVersion splitVersion =
                        inFlightSplitContext == null
                                ? knownSplitVersions.get(split.splitId())
                                : inFlightSplitContext.splitVersion;
                enqueueSplitIfAbsent(split, splitVersion);
            }
        }
        handleSplitRequest(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (lock) {
            return pendingSplits.size();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        List<FileSourceSplit> assign = new ArrayList<>(DEFAULT_ASSIGN_BATCH_SIZE);
        synchronized (lock) {
            while (assign.size() < DEFAULT_ASSIGN_BATCH_SIZE && !pendingSplits.isEmpty()) {
                FileSourceSplit split = pendingSplits.pollFirst();
                if (split == null) {
                    break;
                }
                pendingSplitIds.remove(split.splitId());
                inFlightSplits.add(split);
                SplitVersion splitVersion = pendingSplitVersions.remove(split.splitId());
                inFlightSplitContexts.put(
                        split.splitId(), new InFlightSplitContext(split, splitVersion));
                assign.add(split);
            }
            if (assign.isEmpty()) {
                readersAwaitingSplit.add(subtaskId);
            } else {
                readersAwaitingSplit.remove(subtaskId);
            }
        }
        if (!assign.isEmpty()) {
            context.assignSplit(subtaskId, assign);
            if (log.isDebugEnabled()) {
                log.debug("Assigned {} splits to reader {}", assign.size(), subtaskId);
            }
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        // Try to assign immediately in case splits are already discovered.
        handleSplitRequest(subtaskId);
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            if (!finishedAwaitingCheckpoint.isEmpty()) {
                pendingOpsByCheckpoint
                        .computeIfAbsent(checkpointId, key -> new ArrayList<>())
                        .addAll(copyOperationStates(finishedAwaitingCheckpoint));
                finishedAwaitingCheckpoint.clear();
            }
            // Store in-flight splits only to avoid unbounded state growth.
            return new FileSourceState(
                    new HashSet<>(inFlightSplits),
                    jobStartTimeMillis,
                    copyPendingOpsByCheckpoint(),
                    new HashMap<>(retentionLastRunMillisByPath));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        commitPostSyncOperations(checkpointId);
        runRetentionIfNeeded(checkpointId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof FileSplitFinishedEvent)) {
            return;
        }
        String splitId = ((FileSplitFinishedEvent) sourceEvent).getSplitId();
        InFlightSplitContext finishedContext;
        synchronized (lock) {
            finishedContext = inFlightSplitContexts.remove(splitId);
            inFlightSplits.removeIf(s -> Objects.equals(s.splitId(), splitId));
        }
        if (finishedContext == null) {
            return;
        }
        Optional<TableScanContext> tableCtxOpt =
                findTableScanContext(finishedContext.split.getTableId());
        if (!tableCtxOpt.isPresent()) {
            log.warn(
                    "Skip post-sync staging because table context is not found. splitId={}, tableId={}",
                    splitId,
                    finishedContext.split.getTableId());
            return;
        }
        TableScanContext tableScanContext = tableCtxOpt.get();
        if (tableScanContext.postSyncAction == FilePostSyncAction.NONE) {
            return;
        }
        FileSourceOperationState opState =
                buildOperationStateFromFinishedSplit(tableScanContext, finishedContext);
        if (opState == null) {
            return;
        }
        synchronized (lock) {
            finishedAwaitingCheckpoint.add(opState);
        }
        incCounter(postSyncSubmittedCounter);
        if (log.isDebugEnabled()) {
            log.debug(
                    "Staged post-sync operation: action={}, splitId={}, source={}",
                    opState.getAction(),
                    opState.getSplitId(),
                    maskUriUserInfo(opState.getSourcePath()));
        }
    }

    private void safeScanOnce() {
        if (closed) {
            return;
        }
        try {
            scanOnce();
        } catch (Exception e) {
            log.warn("Continuous discovery scan failed, will retry in next interval.", e);
        }
    }

    @VisibleForTesting
    void scanOnceForTest() throws IOException {
        scanOnce();
    }

    private void scanOnce() throws IOException {
        int scanned = 0;
        int queued = 0;
        Set<String> activeKnownSplitIds = new HashSet<>();
        for (TableScanContext ctx : tableScanContexts) {
            List<FileStatus> files = ctx.listFiles(ctx.rootPath);
            scanned += files.size();
            for (FileStatus fileStatus : files) {
                if (!ctx.shouldProcess(fileStatus, jobStartTimeMillis, startMode)) {
                    clearKnownVersionIfPresent(ctx.tableId, fileStatus.getPath().toString());
                    continue;
                }
                SplitVersion splitVersion = SplitVersion.fromFileStatus(fileStatus);
                for (FileSourceSplit split : ctx.toSplits(fileStatus)) {
                    activeKnownSplitIds.add(split.splitId());
                    if (enqueueSplitIfAbsent(split, splitVersion)) {
                        queued++;
                    }
                }
            }
        }
        cleanupStaleKnownVersions(activeKnownSplitIds);
        if (queued > 0) {
            log.info(
                    "Continuous discovery scan finished: scanned={}, queued={}, pending={}, inflight={}",
                    scanned,
                    queued,
                    currentUnassignedSplitSize(),
                    inFlightSplits.size());
        } else if (log.isDebugEnabled()) {
            log.debug(
                    "Continuous discovery scan finished: scanned={}, queued=0, pending={}, inflight={}",
                    scanned,
                    currentUnassignedSplitSize(),
                    inFlightSplits.size());
        }

        assignSplitsToAwaitingReaders();
    }

    private void assignSplitsToAwaitingReaders() {
        if (currentUnassignedSplitSize() <= 0) {
            return;
        }
        Set<Integer> registeredReaders = context.registeredReaders();
        if (registeredReaders == null || registeredReaders.isEmpty()) {
            return;
        }

        Set<Integer> awaitingReaders;
        synchronized (lock) {
            if (readersAwaitingSplit.isEmpty()) {
                return;
            }
            awaitingReaders = new HashSet<>(readersAwaitingSplit);
        }

        for (int readerId : awaitingReaders) {
            if (!registeredReaders.contains(readerId)) {
                continue;
            }
            if (currentUnassignedSplitSize() <= 0) {
                return;
            }
            handleSplitRequest(readerId);
        }
    }

    private boolean enqueueSplitIfAbsent(FileSourceSplit split) {
        return enqueueSplitIfAbsent(split, null);
    }

    private boolean enqueueSplitIfAbsent(FileSourceSplit split, SplitVersion splitVersion) {
        String splitId = split.splitId();
        synchronized (lock) {
            if (splitVersion != null && splitVersion.equals(knownSplitVersions.get(splitId))) {
                return false;
            }
            if (pendingSplitIds.contains(splitId)) {
                return false;
            }
            for (FileSourceSplit inFlight : inFlightSplits) {
                if (Objects.equals(inFlight.splitId(), splitId)) {
                    return false;
                }
            }
            pendingSplits.addLast(split);
            pendingSplitIds.add(splitId);
            if (splitVersion != null) {
                pendingSplitVersions.put(splitId, splitVersion);
            } else {
                pendingSplitVersions.remove(splitId);
            }
            if (splitVersion != null) {
                knownSplitVersions.put(splitId, splitVersion);
            }
            return true;
        }
    }

    private void recoverSplitsFromCheckpoint(Set<FileSourceSplit> restoredInFlightSplits) {
        if (restoredInFlightSplits == null || restoredInFlightSplits.isEmpty()) {
            return;
        }
        int recovered = 0;
        int skipped = 0;
        for (FileSourceSplit split : restoredInFlightSplits) {
            Optional<TableScanContext> contextOpt = findTableScanContext(split.getTableId());
            if (!contextOpt.isPresent()) {
                skipped++;
                continue;
            }
            TableScanContext context = contextOpt.get();
            FileStatus sourceStatus;
            try {
                sourceStatus = context.sourceFs.getFileStatus(split.getFilePath());
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Skip recovering split because source file status cannot be resolved: {}",
                            maskUriUserInfo(split.getFilePath()),
                            e);
                }
                skipped++;
                continue;
            }

            boolean shouldProcess;
            try {
                shouldProcess = context.shouldProcess(sourceStatus, jobStartTimeMillis, startMode);
            } catch (IOException e) {
                log.warn(
                        "Failed to evaluate recovered split {}, re-enqueue it conservatively.",
                        maskUriUserInfo(split.getFilePath()),
                        e);
                shouldProcess = true;
            }
            if (!shouldProcess) {
                skipped++;
                continue;
            }

            synchronized (lock) {
                pendingSplits.addLast(split);
                pendingSplitIds.add(split.splitId());
                SplitVersion splitVersion = SplitVersion.fromFileStatus(sourceStatus);
                pendingSplitVersions.put(split.splitId(), splitVersion);
                knownSplitVersions.put(split.splitId(), splitVersion);
            }
            recovered++;
        }
        log.info(
                "Recovered in-flight splits from checkpoint: total={}, re-enqueued={}, skipped={}.",
                restoredInFlightSplits.size(),
                recovered,
                skipped);
    }

    private void cleanupStaleKnownVersions(Set<String> activeKnownSplitIds) {
        synchronized (lock) {
            if (knownSplitVersions.isEmpty()) {
                return;
            }
            Set<String> retainedSplitIds = new HashSet<>(activeKnownSplitIds);
            retainedSplitIds.addAll(pendingSplitIds);
            for (FileSourceSplit split : inFlightSplits) {
                retainedSplitIds.add(split.splitId());
            }
            knownSplitVersions.keySet().removeIf(splitId -> !retainedSplitIds.contains(splitId));
        }
    }

    private Optional<TableScanContext> findTableScanContext(String tableId) {
        for (TableScanContext ctx : tableScanContexts) {
            if (!Objects.equals(ctx.tableId, tableId)) {
                continue;
            }
            return Optional.of(ctx);
        }
        return Optional.empty();
    }

    private void clearKnownVersionIfPresent(String tableId, String filePath) {
        // Continuous mode currently supports binary only, so split id is stable as
        // tableId+filePath.
        String splitId = new FileSourceSplit(tableId, filePath).splitId();
        synchronized (lock) {
            knownSplitVersions.remove(splitId);
        }
    }

    private void restorePendingOpsFromCheckpoint(
            Map<Long, List<FileSourceOperationState>> checkpointOpsByCheckpoint) {
        if (checkpointOpsByCheckpoint == null || checkpointOpsByCheckpoint.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (Map.Entry<Long, List<FileSourceOperationState>> entry :
                    checkpointOpsByCheckpoint.entrySet()) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }
                pendingOpsByCheckpoint.put(entry.getKey(), copyOperationStates(entry.getValue()));
            }
        }
    }

    private void restoreRetentionCursor(Map<String, Long> retentionCursorByPath) {
        if (retentionCursorByPath == null || retentionCursorByPath.isEmpty()) {
            return;
        }
        synchronized (lock) {
            retentionLastRunMillisByPath.putAll(retentionCursorByPath);
        }
    }

    private void commitPostSyncOperations(long checkpointId) {
        Map<Long, List<FileSourceOperationState>> toCommit = new TreeMap<>();
        synchronized (lock) {
            for (Map.Entry<Long, List<FileSourceOperationState>> entry :
                    pendingOpsByCheckpoint.headMap(checkpointId, true).entrySet()) {
                toCommit.put(entry.getKey(), copyOperationStates(entry.getValue()));
            }
        }
        if (toCommit.isEmpty()) {
            return;
        }

        long attempted = 0L;
        long succeeded = 0L;
        long failed = 0L;
        long staleSkipped = 0L;
        Map<Long, List<FileSourceOperationState>> remainingByCheckpoint = new TreeMap<>();

        for (Map.Entry<Long, List<FileSourceOperationState>> entry : toCommit.entrySet()) {
            List<FileSourceOperationState> remaining = new ArrayList<>();
            for (FileSourceOperationState op : entry.getValue()) {
                attempted++;
                OpCommitResult result = commitSingleOperation(op);
                if (result == OpCommitResult.SUCCESS) {
                    succeeded++;
                    incCounter(postSyncSucceededCounter);
                } else if (result == OpCommitResult.STALE_SKIPPED) {
                    staleSkipped++;
                    incCounter(postSyncStaleSkippedCounter);
                } else {
                    failed++;
                    incCounter(postSyncFailedCounter);
                    op.increaseRetryCount();
                    remaining.add(op);
                }
            }
            if (!remaining.isEmpty()) {
                remainingByCheckpoint.put(entry.getKey(), remaining);
            }
        }

        synchronized (lock) {
            for (Long cp : toCommit.keySet()) {
                pendingOpsByCheckpoint.remove(cp);
            }
            for (Map.Entry<Long, List<FileSourceOperationState>> entry :
                    remainingByCheckpoint.entrySet()) {
                pendingOpsByCheckpoint.put(entry.getKey(), entry.getValue());
            }
        }

        log.info(
                "Post-sync commit finished for checkpoint {}: attempted={}, success={}, stale_skipped={}, failed={}, remaining_checkpoints={}",
                checkpointId,
                attempted,
                succeeded,
                staleSkipped,
                failed,
                remainingByCheckpoint.size());
    }

    private OpCommitResult commitSingleOperation(FileSourceOperationState op) {
        Optional<TableScanContext> tableContextOpt = findTableScanContext(op.getTableId());
        if (!tableContextOpt.isPresent()) {
            log.warn(
                    "Post-sync operation failed: table context not found, tableId={}, splitId={}",
                    op.getTableId(),
                    op.getSplitId());
            return OpCommitResult.FAILED_RETRYABLE;
        }

        try {
            if (op.getAction() == FilePostSyncAction.DELETE) {
                return commitDeleteOperation(tableContextOpt.get(), op);
            }
            if (op.getAction() == FilePostSyncAction.BACKUP) {
                return commitBackupOperation(tableContextOpt.get(), op);
            }
            return OpCommitResult.SUCCESS;
        } catch (Exception e) {
            log.warn(
                    "Post-sync operation failed and will be retried: action={}, splitId={}, source={}, retryCount={}",
                    op.getAction(),
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()),
                    op.getRetryCount(),
                    e);
            return OpCommitResult.FAILED_RETRYABLE;
        }
    }

    private OpCommitResult commitDeleteOperation(TableScanContext ctx, FileSourceOperationState op)
            throws IOException {
        FileStatus sourceStatus;
        try {
            sourceStatus = ctx.sourceFs.getFileStatus(op.getSourcePath());
        } catch (java.io.FileNotFoundException e) {
            return OpCommitResult.SUCCESS;
        }
        if (!isVersionMatched(sourceStatus, op)) {
            log.warn(
                    "Post-sync delete skipped due to stale version: splitId={}, source={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()));
            return OpCommitResult.STALE_SKIPPED;
        }
        ctx.sourceFs.deleteFile(op.getSourcePath());
        return OpCommitResult.SUCCESS;
    }

    private OpCommitResult commitBackupOperation(TableScanContext ctx, FileSourceOperationState op)
            throws IOException {
        if (StringUtils.isBlank(op.getBackupTargetPath())) {
            log.warn(
                    "Post-sync backup failed: backup target path is empty, splitId={}, source={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()));
            return OpCommitResult.FAILED_RETRYABLE;
        }

        boolean targetExists = ctx.sourceFs.fileExist(op.getBackupTargetPath());
        FileStatus sourceStatus = null;
        boolean sourceExists = true;
        try {
            sourceStatus = ctx.sourceFs.getFileStatus(op.getSourcePath());
        } catch (java.io.FileNotFoundException e) {
            sourceExists = false;
        }

        if (!sourceExists) {
            if (targetExists) {
                return OpCommitResult.SUCCESS;
            }
            return OpCommitResult.SUCCESS;
        }

        if (!isVersionMatched(sourceStatus, op)) {
            log.warn(
                    "Post-sync backup skipped due to stale version: splitId={}, source={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()));
            return OpCommitResult.STALE_SKIPPED;
        }

        if (targetExists) {
            // Target already exists for this version suffix, delete source to finish idempotently.
            ctx.sourceFs.deleteFile(op.getSourcePath());
            return OpCommitResult.SUCCESS;
        }

        ctx.sourceFs.renameFile(op.getSourcePath(), op.getBackupTargetPath(), false);
        return OpCommitResult.SUCCESS;
    }

    private boolean isVersionMatched(FileStatus status, FileSourceOperationState op) {
        return status.getLen() == op.getSourceLength()
                && status.getModificationTime() == op.getSourceModificationTime();
    }

    private void runRetentionIfNeeded(long checkpointId) {
        long now = System.currentTimeMillis();
        boolean hasAnyRetentionEnabled = false;
        long deleted = 0L;
        long failed = 0L;
        for (TableScanContext ctx : tableScanContexts) {
            if (!ctx.retentionEnabled()) {
                continue;
            }
            hasAnyRetentionEnabled = true;
            long lastRun;
            synchronized (lock) {
                lastRun = retentionLastRunMillisByPath.getOrDefault(ctx.backupPath, 0L);
                if (now - lastRun < ctx.retentionCheckInterval.toMillis()) {
                    continue;
                }
                retentionLastRunMillisByPath.put(ctx.backupPath, now);
            }
            RetentionResult result = runRetentionOnce(ctx, now);
            deleted += result.deletedFiles;
            failed += result.failedOperations;
        }

        if (hasAnyRetentionEnabled) {
            log.info(
                    "Retention scan finished at checkpoint {}: deleted_files={}, failed_operations={}.",
                    checkpointId,
                    deleted,
                    failed);
        }
    }

    private RetentionResult runRetentionOnce(TableScanContext ctx, long nowMillis) {
        RetentionResult result = new RetentionResult();
        long expireBefore = nowMillis - ctx.retentionMaxAge.toMillis();
        try {
            cleanupRetentionRecursively(ctx.sourceFs, ctx.backupPath, expireBefore, result);
        } catch (Exception e) {
            result.failedOperations++;
            incCounter(retentionFailedCounter);
            log.warn(
                    "Retention scan failed: backupPath={}, maxAge={}, interval={}",
                    maskUriUserInfo(ctx.backupPath),
                    ctx.retentionMaxAge,
                    ctx.retentionCheckInterval,
                    e);
        }
        return result;
    }

    private void cleanupRetentionRecursively(
            HadoopFileSystemProxy fs, String path, long expireBefore, RetentionResult result)
            throws IOException {
        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(path);
        } catch (java.io.FileNotFoundException e) {
            return;
        }
        if (statuses == null || statuses.length == 0) {
            return;
        }
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                cleanupRetentionRecursively(fs, status.getPath().toString(), expireBefore, result);
                continue;
            }
            if (!status.isFile()) {
                continue;
            }
            if (!BACKUP_VERSION_SUFFIX_PATTERN.matcher(status.getPath().getName()).matches()) {
                continue;
            }
            if (status.getModificationTime() > expireBefore) {
                continue;
            }
            try {
                fs.deleteFile(status.getPath().toString());
                result.deletedFiles++;
                incCounter(retentionDeletedCounter);
            } catch (Exception e) {
                result.failedOperations++;
                incCounter(retentionFailedCounter);
                log.warn(
                        "Retention delete failed: file={}, expireBefore={}",
                        maskUriUserInfo(status.getPath().toString()),
                        expireBefore,
                        e);
            }
        }
    }

    private FileSourceOperationState buildOperationStateFromFinishedSplit(
            TableScanContext tableScanContext, InFlightSplitContext inFlightSplitContext) {
        FileSourceSplit split = inFlightSplitContext.split;
        SplitVersion splitVersion = inFlightSplitContext.splitVersion;
        if (splitVersion == null) {
            splitVersion = resolveSplitVersion(tableScanContext, split);
        }
        if (splitVersion == null) {
            log.warn(
                    "Skip post-sync staging because split version cannot be resolved: splitId={}, source={}",
                    split.splitId(),
                    maskUriUserInfo(split.getFilePath()));
            return null;
        }

        String backupTargetPath = null;
        if (tableScanContext.postSyncAction == FilePostSyncAction.BACKUP) {
            String relativePath =
                    resolveRelativePath(tableScanContext.rootPath, split.getFilePath());
            String versionedRelativePath =
                    relativePath + ".v" + splitVersion.length + "_" + splitVersion.modificationTime;
            backupTargetPath =
                    buildTargetFilePath(tableScanContext.backupPath, versionedRelativePath);
        }

        return new FileSourceOperationState(
                split.getTableId(),
                split.splitId(),
                split.getFilePath(),
                splitVersion.length,
                splitVersion.modificationTime,
                tableScanContext.postSyncAction,
                backupTargetPath);
    }

    private SplitVersion resolveSplitVersion(
            TableScanContext tableScanContext, FileSourceSplit split) {
        try {
            FileStatus fileStatus = tableScanContext.sourceFs.getFileStatus(split.getFilePath());
            return SplitVersion.fromFileStatus(fileStatus);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Failed to resolve split version from file status, splitId={}, source={}",
                        split.splitId(),
                        maskUriUserInfo(split.getFilePath()),
                        e);
            }
            return null;
        }
    }

    private static List<FileSourceOperationState> copyOperationStates(
            List<FileSourceOperationState> operationStates) {
        if (operationStates == null || operationStates.isEmpty()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(operationStates);
    }

    private Map<Long, List<FileSourceOperationState>> copyPendingOpsByCheckpoint() {
        Map<Long, List<FileSourceOperationState>> copied = new TreeMap<>();
        for (Map.Entry<Long, List<FileSourceOperationState>> entry :
                pendingOpsByCheckpoint.entrySet()) {
            copied.put(entry.getKey(), copyOperationStates(entry.getValue()));
        }
        return copied;
    }

    private static Counter initCounter(MetricsContext metricsContext, String name) {
        if (metricsContext == null) {
            return null;
        }
        try {
            return metricsContext.counter(name);
        } catch (Exception e) {
            return null;
        }
    }

    private static void incCounter(Counter counter) {
        if (counter != null) {
            counter.inc();
        }
    }

    private static void validateContinuousDiscoveryConfig(List<BaseFileSourceConfig> configs) {
        FileDiscoveryMode mode = resolveGlobalOption(configs, FileBaseSourceOptions.DISCOVERY_MODE);
        if (mode != FileDiscoveryMode.CONTINUOUS) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Continuous enumerator can only be used when discovery_mode=continuous.");
        }
        for (BaseFileSourceConfig cfg : configs) {
            ReadonlyConfig c = cfg.getBaseFileSourceConfig();
            FileSyncMode syncMode = c.get(FileBaseSourceOptions.SYNC_MODE);
            if (syncMode != FileSyncMode.UPDATE) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "discovery_mode=continuous currently requires sync_mode=update.");
            }
            FileFormat fileFormat = c.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
            if (fileFormat != FileFormat.BINARY) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "discovery_mode=continuous currently only supports file_format_type=binary.");
            }
            Duration interval = c.get(FileBaseSourceOptions.SCAN_INTERVAL);
            if (interval.isZero() || interval.isNegative()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "discovery_mode=continuous requires scan_interval > 0, but got "
                                + interval
                                + ".");
            }
            validatePostSyncConfig(cfg);
        }
    }

    private static void validatePostSyncConfig(BaseFileSourceConfig baseFileSourceConfig) {
        ReadonlyConfig config = baseFileSourceConfig.getBaseFileSourceConfig();
        FilePostSyncAction action = config.get(FileBaseSourceOptions.POST_SYNC_ACTION);
        Optional<String> backupPath = config.getOptional(FileBaseSourceOptions.BACKUP_PATH);
        Optional<Duration> retentionMaxAge =
                config.getOptional(FileBaseSourceOptions.RETENTION_MAX_AGE);
        Duration retentionCheckInterval =
                config.get(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL);

        if (action == FilePostSyncAction.BACKUP && StringUtils.isBlank(backupPath.orElse(null))) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "post_sync_action=backup requires backup_path.");
        }
        if (action == FilePostSyncAction.BACKUP) {
            validateBackupPath(baseFileSourceConfig, backupPath.get());
        }

        if (action != FilePostSyncAction.BACKUP && backupPath.isPresent()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "backup_path is only valid when post_sync_action=backup.");
        }

        if (retentionMaxAge.isPresent()) {
            if (action != FilePostSyncAction.BACKUP) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "retention_max_age is only valid when post_sync_action=backup.");
            }
            if (retentionMaxAge.get().isZero() || retentionMaxAge.get().isNegative()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "retention_max_age must be greater than 0, but got "
                                + retentionMaxAge.get()
                                + ".");
            }
            if (retentionCheckInterval.isZero() || retentionCheckInterval.isNegative()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "retention_check_interval must be greater than 0, but got "
                                + retentionCheckInterval
                                + ".");
            }
        }
    }

    private static void validateBackupPath(
            BaseFileSourceConfig baseFileSourceConfig, String backupPath) {
        ReadonlyConfig config = baseFileSourceConfig.getBaseFileSourceConfig();
        String sourcePath = config.get(FileBaseSourceOptions.FILE_PATH);
        HadoopConf hadoopConf = baseFileSourceConfig.getHadoopConfig();
        String defaultFsIdentity =
                normalizeFsIdentity(hadoopConf == null ? null : hadoopConf.getHdfsNameKey());
        String sourceFsIdentity = resolveFsIdentity(sourcePath, defaultFsIdentity);
        String backupFsIdentity = resolveFsIdentity(backupPath, defaultFsIdentity);
        if (Objects.equals(sourceFsIdentity, backupFsIdentity)) {
            if (isPathOverlapped(sourcePath, backupPath)) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "backup_path must not overlap with path. Please configure backup_path outside the scanned path tree.");
            }
            return;
        }
        throw new FileConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "post_sync_action=backup currently only supports same-filesystem backup in phase-1. "
                        + "Please configure backup_path with the same scheme and authority as path.");
    }

    private static String resolveFsIdentity(String path, String defaultFsIdentity) {
        if (StringUtils.isBlank(path)) {
            return defaultFsIdentity;
        }
        try {
            java.net.URI uri = new Path(path).toUri();
            if (StringUtils.isBlank(uri.getScheme())) {
                return defaultFsIdentity;
            }
            return normalizeFsIdentity(uri);
        } catch (Exception e) {
            return defaultFsIdentity;
        }
    }

    private static String normalizeFsIdentity(String rawFs) {
        if (StringUtils.isBlank(rawFs)) {
            return "";
        }
        try {
            return normalizeFsIdentity(new Path(rawFs).toUri());
        } catch (Exception e) {
            return rawFs.trim().toLowerCase(Locale.ROOT);
        }
    }

    private static String normalizeFsIdentity(java.net.URI uri) {
        if (uri == null || StringUtils.isBlank(uri.getScheme())) {
            return "";
        }
        String authority = uri.getAuthority();
        if (authority != null && uri.getUserInfo() != null) {
            authority = authority.replace(uri.getUserInfo() + "@", "");
        }
        return uri.getScheme().toLowerCase(Locale.ROOT)
                + "://"
                + StringUtils.defaultString(authority).toLowerCase(Locale.ROOT);
    }

    private static boolean isPathOverlapped(String sourcePath, String backupPath) {
        String normalizedSourcePath = normalizePathForOverlap(sourcePath);
        String normalizedBackupPath = normalizePathForOverlap(backupPath);
        if (StringUtils.isBlank(normalizedSourcePath)
                || StringUtils.isBlank(normalizedBackupPath)) {
            return false;
        }
        return Objects.equals(normalizedSourcePath, normalizedBackupPath)
                || isParentPath(normalizedSourcePath, normalizedBackupPath)
                || isParentPath(normalizedBackupPath, normalizedSourcePath);
    }

    private static boolean isParentPath(String parentPath, String childPath) {
        String parentPrefix = parentPath.endsWith("/") ? parentPath : parentPath + "/";
        return childPath.startsWith(parentPrefix);
    }

    private static String normalizePathForOverlap(String rawPath) {
        if (StringUtils.isBlank(rawPath)) {
            return rawPath;
        }
        try {
            return trimTrailingPathSeparator(new Path(rawPath).toUri().normalize().getPath());
        } catch (Exception e) {
            return trimTrailingPathSeparator(rawPath);
        }
    }

    private static String trimTrailingPathSeparator(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }
        String normalized = path.replace('\\', '/');
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static <T> T resolveGlobalOption(
            List<BaseFileSourceConfig> configs,
            org.apache.seatunnel.api.configuration.Option<T> option) {
        Set<T> values = new HashSet<>();
        for (BaseFileSourceConfig cfg : configs) {
            ReadonlyConfig c = cfg.getBaseFileSourceConfig();
            Optional<T> v = c.getOptional(option);
            v.ifPresent(values::add);
        }
        if (values.size() > 1) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "In multi-table mode, option '"
                            + option.key()
                            + "' must be consistent across tables.");
        }
        return values.isEmpty() ? option.defaultValue() : values.iterator().next();
    }

    private static final class TableScanContext implements AutoCloseable {
        private final String tableId;
        private final ReadonlyConfig config;
        private final String rootPath;
        private final HadoopConf hadoopConf;
        private final HadoopFileSystemProxy sourceFs;
        private final HadoopFileSystemProxy targetFs;
        private final boolean shareTargetFs;
        private final FileUpdateStrategy updateStrategy;
        private final FileCompareMode compareMode;
        private final FilePostSyncAction postSyncAction;
        private final String backupPath;
        private final Duration retentionMaxAge;
        private final Duration retentionCheckInterval;
        private boolean checksumUnavailableWarned;
        private final boolean recursiveFileScan;

        private final Pattern pattern;
        private final String fileBasePath;
        private final Date modifiedStart;
        private final Date modifiedEnd;
        private final String filenameExtension;
        private final List<String> readPartitions;

        private final FileSplitStrategy fileSplitStrategy;

        private TableScanContext(
                BaseFileSourceConfig baseFileSourceConfig, FileSplitStrategy fileSplitStrategy) {
            this.tableId =
                    baseFileSourceConfig.getCatalogTable().getTableId().toTablePath().toString();
            this.config = baseFileSourceConfig.getBaseFileSourceConfig();
            this.rootPath = config.get(FileBaseSourceOptions.FILE_PATH);
            this.hadoopConf = baseFileSourceConfig.getHadoopConfig();
            this.sourceFs = new HadoopFileSystemProxy(hadoopConf);

            String filterPattern =
                    config.getOptional(FileBaseSourceOptions.FILE_FILTER_PATTERN).orElse(null);
            this.pattern =
                    StringUtils.isBlank(filterPattern) ? null : Pattern.compile(filterPattern);
            this.fileBasePath = config.getOptional(FileBaseSourceOptions.FILE_PATH).orElse(null);

            this.modifiedStart =
                    parseModifiedDate(
                            config.getOptional(FileBaseSourceOptions.FILE_FILTER_MODIFIED_START)
                                    .orElse(null));
            this.modifiedEnd =
                    parseModifiedDate(
                            config.getOptional(FileBaseSourceOptions.FILE_FILTER_MODIFIED_END)
                                    .orElse(null));

            this.filenameExtension =
                    config.getOptional(FileBaseSourceOptions.FILENAME_EXTENSION).orElse(null);
            this.readPartitions =
                    config.getOptional(FileBaseSourceOptions.READ_PARTITIONS)
                            .orElse(Collections.emptyList());

            this.updateStrategy = config.get(FileBaseSourceOptions.UPDATE_STRATEGY);
            this.compareMode = config.get(FileBaseSourceOptions.COMPARE_MODE);
            this.postSyncAction = config.get(FileBaseSourceOptions.POST_SYNC_ACTION);
            this.backupPath =
                    config.getOptional(FileBaseSourceOptions.BACKUP_PATH)
                            .map(sourceFs::makeQualifiedPath)
                            .orElse(null);
            this.retentionMaxAge =
                    config.getOptional(FileBaseSourceOptions.RETENTION_MAX_AGE).orElse(null);
            this.retentionCheckInterval =
                    config.get(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL);
            this.recursiveFileScan = config.get(FileBaseSourceOptions.RECURSIVE_FILE_SCAN);

            String targetPath = config.get(FileBaseSourceOptions.TARGET_PATH);
            Map<String, String> targetHadoopConf =
                    config.getOptional(FileBaseSourceOptions.TARGET_HADOOP_CONF).orElse(null);
            HadoopConf targetConf = buildTargetHadoopConf(hadoopConf, targetPath, targetHadoopConf);
            if (targetConf == hadoopConf) {
                this.targetFs = this.sourceFs;
                this.shareTargetFs = true;
            } else {
                this.targetFs = new HadoopFileSystemProxy(targetConf);
                this.shareTargetFs = false;
            }

            this.fileSplitStrategy = fileSplitStrategy;
        }

        private boolean retentionEnabled() {
            return postSyncAction == FilePostSyncAction.BACKUP
                    && StringUtils.isNotBlank(backupPath)
                    && retentionMaxAge != null
                    && retentionMaxAge.toMillis() > 0;
        }

        private List<FileSourceSplit> toSplits(FileStatus fileStatus) {
            return fileSplitStrategy.split(tableId, fileStatus.getPath().toString());
        }

        private List<FileStatus> listFiles(String path) throws IOException {
            List<FileStatus> files = new ArrayList<>();
            FileStatus[] statuses = sourceFs.listStatus(path);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    String name = status.getPath().getName();
                    if (recursiveFileScan && !name.startsWith(".")) {
                        files.addAll(listFiles(status.getPath().toString()));
                    }
                    continue;
                }
                if (!status.isFile()) {
                    continue;
                }
                if (status.getLen() <= 0) {
                    continue;
                }
                String name = status.getPath().getName();
                if ("_SUCCESS".equals(name) || name.startsWith(".")) {
                    continue;
                }
                if (!filterByPattern(status)) {
                    continue;
                }
                if (!filterByModifiedDate(status)) {
                    continue;
                }
                if (StringUtils.isNotBlank(filenameExtension)
                        && !name.endsWith(filenameExtension)) {
                    continue;
                }
                if (!readPartitions.isEmpty()) {
                    String filePath = status.getPath().toString();
                    boolean matched = false;
                    for (String p : readPartitions) {
                        if (filePath.contains(p)) {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        continue;
                    }
                }
                files.add(status);
            }
            return files;
        }

        private boolean shouldProcess(
                FileStatus sourceFileStatus, long baselineStartMillis, FileStartMode startMode)
                throws IOException {
            if (startMode == FileStartMode.LATEST
                    && sourceFileStatus.getModificationTime() <= baselineStartMillis) {
                return false;
            }
            return shouldSyncInUpdateMode(sourceFileStatus);
        }

        private boolean shouldSyncInUpdateMode(FileStatus sourceFileStatus) throws IOException {
            String sourceFilePath = sourceFileStatus.getPath().toString();
            String relativePath = resolveRelativePath(rootPath, sourceFilePath);
            String targetPath = config.get(FileBaseSourceOptions.TARGET_PATH);
            String targetFilePath = buildTargetFilePath(targetPath, relativePath);

            FileStatus targetFileStatus;
            try {
                targetFileStatus = targetFs.getFileStatus(targetFilePath);
            } catch (java.io.FileNotFoundException e) {
                return true;
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Update mode compare failed when getting target file status, fallback to COPY. source={}, target={}",
                            maskUriUserInfo(sourceFilePath),
                            maskUriUserInfo(targetFilePath),
                            e);
                }
                return true;
            }

            long sourceLen = sourceFileStatus.getLen();
            long targetLen = targetFileStatus.getLen();
            if (sourceLen != targetLen) {
                return true;
            }

            long sourceMtime = sourceFileStatus.getModificationTime();
            long targetMtime = targetFileStatus.getModificationTime();

            if (updateStrategy == FileUpdateStrategy.DISTCP) {
                if (sourceMtime > targetMtime) {
                    return true;
                }
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Update sync mode skipped file: source={}, target={}, reason={}",
                            maskUriUserInfo(sourceFilePath),
                            maskUriUserInfo(targetFilePath),
                            "distcp: target newer or same");
                }
                return false;
            }

            if (updateStrategy == FileUpdateStrategy.STRICT) {
                if (compareMode == FileCompareMode.LEN_MTIME) {
                    if (sourceMtime != targetMtime) {
                        return true;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Update sync mode skipped file: source={}, target={}, reason={}",
                                maskUriUserInfo(sourceFilePath),
                                maskUriUserInfo(targetFilePath),
                                "strict len_mtime: len and mtime equal");
                    }
                    return false;
                }
                if (compareMode == FileCompareMode.CHECKSUM) {
                    FileChecksum sourceChecksum = null;
                    FileChecksum targetChecksum = null;
                    Exception checksumException = null;
                    try {
                        sourceChecksum = sourceFs.getFileChecksum(sourceFilePath);
                        targetChecksum = targetFs.getFileChecksum(targetFilePath);
                    } catch (Exception e) {
                        checksumException = e;
                    }

                    if (checksumException != null
                            || sourceChecksum == null
                            || targetChecksum == null) {
                        warnChecksumUnavailableOnce(
                                sourceFilePath, targetFilePath, checksumException);
                        try {
                            boolean sameContent = fileContentEquals(sourceFilePath, targetFilePath);
                            if (sameContent && log.isDebugEnabled()) {
                                log.debug(
                                        "Update sync mode skipped file: source={}, target={}, reason={}",
                                        maskUriUserInfo(sourceFilePath),
                                        maskUriUserInfo(targetFilePath),
                                        "strict checksum: content equal (checksum unavailable)");
                            }
                            return !sameContent;
                        } catch (Exception e) {
                            log.warn(
                                    "Fallback content comparison failed, fallback to COPY. source={}, target={}",
                                    maskUriUserInfo(sourceFilePath),
                                    maskUriUserInfo(targetFilePath),
                                    e);
                            return true;
                        }
                    }
                    if (checksumEquals(sourceChecksum, targetChecksum)) {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Update sync mode skipped file: source={}, target={}, reason={}",
                                    maskUriUserInfo(sourceFilePath),
                                    maskUriUserInfo(targetFilePath),
                                    "strict checksum: checksum equal");
                        }
                        return false;
                    }
                    return true;
                }
            }

            return true;
        }

        private boolean fileContentEquals(String sourceFilePath, String targetFilePath)
                throws IOException {
            try (InputStream sourceIn = sourceFs.getInputStream(sourceFilePath);
                    InputStream targetIn = targetFs.getInputStream(targetFilePath)) {
                byte[] sourceBuffer = new byte[8 * 1024];
                byte[] targetBuffer = new byte[8 * 1024];

                while (true) {
                    int sourceRead = sourceIn.read(sourceBuffer);
                    int targetRead = targetIn.read(targetBuffer);
                    if (sourceRead != targetRead) {
                        return false;
                    }
                    if (sourceRead == -1) {
                        return true;
                    }
                    for (int i = 0; i < sourceRead; i++) {
                        if (sourceBuffer[i] != targetBuffer[i]) {
                            return false;
                        }
                    }
                }
            }
        }

        private void warnChecksumUnavailableOnce(
                String sourceFilePath, String targetFilePath, Exception checksumException) {
            if (checksumUnavailableWarned) {
                return;
            }
            if (checksumException == null) {
                log.warn(
                        "File checksum is not available, fallback to content comparison. source={}, target={}",
                        maskUriUserInfo(sourceFilePath),
                        maskUriUserInfo(targetFilePath));
            } else {
                log.warn(
                        "File checksum is not available, fallback to content comparison. source={}, target={}",
                        maskUriUserInfo(sourceFilePath),
                        maskUriUserInfo(targetFilePath),
                        checksumException);
            }
            checksumUnavailableWarned = true;
        }

        private boolean filterByPattern(FileStatus fileStatus) {
            if (pattern == null || fileBasePath == null) {
                return true;
            }
            if (pattern.pattern().startsWith(fileBasePath)) {
                String absPath = fileStatus.getPath().toUri().getPath();
                return pattern.matcher(absPath.substring(absPath.indexOf(fileBasePath))).matches();
            }
            return pattern.matcher(fileStatus.getPath().getName()).matches();
        }

        private boolean filterByModifiedDate(FileStatus fileStatus) {
            long fileModifiedTime = fileStatus.getModificationTime();
            if (modifiedStart != null && modifiedEnd != null) {
                return fileModifiedTime >= modifiedStart.getTime()
                        && fileModifiedTime < modifiedEnd.getTime();
            }
            if (modifiedStart != null) {
                return fileModifiedTime >= modifiedStart.getTime();
            }
            if (modifiedEnd != null) {
                return fileModifiedTime < modifiedEnd.getTime();
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            if (!shareTargetFs && targetFs != null) {
                targetFs.close();
            }
            if (sourceFs != null) {
                sourceFs.close();
            }
        }

        private static Date parseModifiedDate(String modifiedDate) {
            if (modifiedDate == null) {
                return null;
            }
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                return dateFormat.parse(modifiedDate);
            } catch (ParseException e) {
                throw new IllegalArgumentException(
                        "Failed to parse file modified date format: yyyy-MM-dd HH:mm:ss, please check file_filter_modified_start or file_filter_modified_end format.");
            }
        }
    }

    private static HadoopConf buildTargetHadoopConf(
            HadoopConf sourceConf, String targetPath, Map<String, String> targetHadoopConf) {
        Map<String, String> extraOptions =
                targetHadoopConf == null
                        ? new LinkedHashMap<>()
                        : new LinkedHashMap<>(targetHadoopConf);

        String fsDefaultNameKey = sourceConf.getFsDefaultNameKey();
        String targetDefaultFs = extraOptions.remove(fsDefaultNameKey);

        if (StringUtils.isBlank(targetDefaultFs)) {
            targetDefaultFs = tryDeriveDefaultFsFromPath(targetPath);
        }
        if (StringUtils.isBlank(targetDefaultFs)) {
            targetDefaultFs = sourceConf.getHdfsNameKey();
        }

        boolean needNewConf =
                !extraOptions.isEmpty()
                        || !Objects.equals(targetDefaultFs, sourceConf.getHdfsNameKey());
        if (!needNewConf) {
            return sourceConf;
        }

        HadoopConf conf = new HadoopConf(targetDefaultFs);
        conf.setHdfsSitePath(sourceConf.getHdfsSitePath());
        conf.setRemoteUser(sourceConf.getRemoteUser());
        conf.setKrb5Path(sourceConf.getKrb5Path());
        conf.setKerberosPrincipal(sourceConf.getKerberosPrincipal());
        conf.setKerberosKeytabPath(sourceConf.getKerberosKeytabPath());
        conf.setExtraOptions(extraOptions);
        return conf;
    }

    private static String tryDeriveDefaultFsFromPath(String basePath) {
        if (StringUtils.isBlank(basePath)) {
            return null;
        }
        try {
            Path path = new Path(basePath);
            if (path.toUri().getScheme() == null) {
                return null;
            }
            if (path.toUri().getAuthority() == null) {
                return null;
            }
            return path.toUri().getScheme() + "://" + path.toUri().getAuthority();
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean checksumEquals(FileChecksum source, FileChecksum target) {
        if (source == null || target == null) {
            return false;
        }
        return Objects.equals(source.getAlgorithmName(), target.getAlgorithmName())
                && source.getLength() == target.getLength()
                && java.util.Arrays.equals(source.getBytes(), target.getBytes());
    }

    private static String buildTargetFilePath(String targetBasePath, String relativePath) {
        String cleanRelativePath =
                StringUtils.isBlank(relativePath)
                        ? ""
                        : (relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
        return new Path(targetBasePath, cleanRelativePath).toString();
    }

    private static String resolveRelativePath(String basePath, String fullFilePath) {
        String base = normalizePathPart(basePath);
        String file = normalizePathPart(fullFilePath);
        if (StringUtils.isBlank(file)) {
            return "";
        }
        if (StringUtils.isBlank(base)) {
            return new Path(file).getName();
        }
        if (Objects.equals(base, file)) {
            return new Path(file).getName();
        }
        String basePrefix = base.endsWith("/") ? base : base + "/";
        if (file.startsWith(basePrefix)) {
            return file.substring(basePrefix.length());
        }
        int idx = file.indexOf(basePrefix);
        if (idx >= 0) {
            return file.substring(idx + basePrefix.length());
        }
        return new Path(file).getName();
    }

    private static String normalizePathPart(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }
        try {
            return new Path(path).toUri().getPath();
        } catch (Exception e) {
            return path;
        }
    }

    private static String maskUriUserInfo(String rawPath) {
        if (StringUtils.isBlank(rawPath)) {
            return rawPath;
        }
        try {
            java.net.URI uri = new Path(rawPath).toUri();
            if (uri.getUserInfo() == null || uri.getAuthority() == null) {
                return rawPath;
            }
            String maskedAuthority = uri.getAuthority().replace(uri.getUserInfo() + "@", "***@");
            return uri.getScheme()
                    + "://"
                    + maskedAuthority
                    + (uri.getPath() == null ? "" : uri.getPath());
        } catch (Exception e) {
            return rawPath;
        }
    }

    private enum OpCommitResult {
        SUCCESS,
        STALE_SKIPPED,
        FAILED_RETRYABLE
    }

    private static final class RetentionResult {
        private long deletedFiles;
        private long failedOperations;
    }

    private static final class InFlightSplitContext {
        private final FileSourceSplit split;
        private final SplitVersion splitVersion;

        private InFlightSplitContext(FileSourceSplit split, SplitVersion splitVersion) {
            this.split = split;
            this.splitVersion = splitVersion;
        }
    }

    private static final class SplitVersion {
        private final long length;
        private final long modificationTime;

        private SplitVersion(long length, long modificationTime) {
            this.length = length;
            this.modificationTime = modificationTime;
        }

        private static SplitVersion fromFileStatus(FileStatus fileStatus) {
            return new SplitVersion(fileStatus.getLen(), fileStatus.getModificationTime());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitVersion that = (SplitVersion) o;
            return length == that.length && modificationTime == that.modificationTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(length, modificationTime);
        }
    }
}
