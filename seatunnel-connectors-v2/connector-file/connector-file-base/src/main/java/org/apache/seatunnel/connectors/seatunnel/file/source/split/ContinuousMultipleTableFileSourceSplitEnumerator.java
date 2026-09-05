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
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileCompareMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileDiscoveryMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FilePostSyncAction;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileStartMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSyncMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileUpdateStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.LocalFileIdentity;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceOperationState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileTailState;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 * <p>Binary discovery reuses the existing {@code sync_mode=update} semantics. Local text tailing
 * checkpoints one committed byte offset per active file.
 */
@Slf4j
public class ContinuousMultipleTableFileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final int DEFAULT_ASSIGN_BATCH_SIZE = 32;
    private static final int TAIL_STATE_MISSING_SCAN_GRACE = 3;
    private static final String METRIC_POST_SYNC_SUBMITTED = "post_sync_operations_submitted";
    private static final String METRIC_POST_SYNC_SUCCEEDED = "post_sync_operations_succeeded";
    private static final String METRIC_POST_SYNC_FAILED = "post_sync_operations_failed";
    private static final String METRIC_POST_SYNC_STALE_SKIPPED =
            "post_sync_operations_stale_skipped";
    private static final String METRIC_RETENTION_DELETED = "retention_deleted_files";
    private static final String METRIC_RETENTION_FAILED = "retention_failed_operations";
    private static final Pattern BACKUP_VERSION_SUFFIX_PATTERN =
            Pattern.compile("^.+\\.v(\\d+)_(\\d+)(?:_(\\d+))?$");

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
    private final Map<String, Long> legacyProcessedFileOffsets = new HashMap<>();
    private final Map<String, FileTailState> fileTailStates = new HashMap<>();
    private long tailScanGeneration;
    private boolean textTailingInitialScanComplete;
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
        this.legacyProcessedFileOffsets.putAll(checkpointState.getProcessedFileOffsets());
        this.fileTailStates.putAll(checkpointState.getFileTailStates());
        this.tailScanGeneration =
                fileTailStates.values().stream()
                        .mapToLong(FileTailState::getLastSeenScanGeneration)
                        .max()
                        .orElse(0L);
        this.textTailingInitialScanComplete = checkpointState.isTextTailingInitialScanComplete();
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
                    new HashMap<>(retentionLastRunMillisByPath),
                    new HashMap<>(legacyProcessedFileOffsets),
                    new HashMap<>(fileTailStates),
                    textTailingInitialScanComplete);
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
        FileSplitFinishedEvent fileSplitFinishedEvent = (FileSplitFinishedEvent) sourceEvent;
        String splitId = fileSplitFinishedEvent.getSplitId();
        InFlightSplitContext finishedContext;
        synchronized (lock) {
            finishedContext = inFlightSplitContexts.get(splitId);
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
            completeInFlightSplit(splitId, finishedContext, null);
            return;
        }
        TableScanContext tableScanContext = tableCtxOpt.get();
        if (tableScanContext.textTailing) {
            synchronized (lock) {
                long processedBytes = fileSplitFinishedEvent.getProcessedBytes();
                if (processedBytes < 0L) {
                    processedBytes = finishedContext.split.getLength();
                }
                String fileIdentity = finishedContext.split.getFileIdentity();
                FileTailState tailState =
                        fileTailStates.get(
                                tailingFileKey(finishedContext.split.getTableId(), fileIdentity));
                if (processedBytes == finishedContext.split.getLength()
                        && tailState != null
                        && tailState.getCommittedOffset() == finishedContext.split.getStart()) {
                    long committedOffset =
                            finishedContext.split.getStart() + finishedContext.split.getLength();
                    fileTailStates.put(
                            tailingFileKey(finishedContext.split.getTableId(), fileIdentity),
                            new FileTailState(
                                    tailState.getTableId(),
                                    tailState.getFileIdentity(),
                                    tailState.getFilePath(),
                                    committedOffset,
                                    finishedContext.split.getEndContentAnchor(),
                                    false,
                                    tailState.getLastSeenScanGeneration()));
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Committed local text tail offset. file={}, offset={}",
                                maskUriUserInfo(tailState.getFilePath()),
                                committedOffset);
                    }
                } else if (processedBytes != finishedContext.split.getLength()) {
                    log.warn(
                            "Local text tail split ended before its planned range; the committed offset is unchanged. file={}, expectedBytes={}, processedBytes={}",
                            maskUriUserInfo(finishedContext.split.getFilePath()),
                            finishedContext.split.getLength(),
                            processedBytes);
                }
            }
            completeInFlightSplit(splitId, finishedContext, null);
            return;
        }
        if (tableScanContext.postSyncAction == FilePostSyncAction.NONE) {
            completeInFlightSplit(splitId, finishedContext, null);
            return;
        }
        FileSourceOperationState opState =
                buildOperationStateFromFinishedSplit(
                        tableScanContext,
                        finishedContext,
                        fileSplitFinishedEvent.getContentFingerprint());
        if (opState == null) {
            return;
        }
        if (!completeInFlightSplit(splitId, finishedContext, opState)) {
            return;
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

    /**
     * Removes a completed split and stages its post-sync operation in one critical section, so a
     * concurrent checkpoint observes either the in-flight split or the fully built operation.
     */
    private boolean completeInFlightSplit(
            String splitId,
            InFlightSplitContext expectedContext,
            FileSourceOperationState operationState) {
        synchronized (lock) {
            if (inFlightSplitContexts.get(splitId) != expectedContext) {
                return false;
            }
            inFlightSplitContexts.remove(splitId);
            inFlightSplits.removeIf(s -> Objects.equals(s.splitId(), splitId));
            if (operationState != null) {
                finishedAwaitingCheckpoint.add(operationState);
            }
            return true;
        }
    }

    @VisibleForTesting
    void safeScanOnce() {
        if (closed) {
            return;
        }
        try {
            scanOnce();
        } catch (IOException e) {
            log.warn("Continuous discovery scan failed, will retry in next interval.", e);
        } catch (RuntimeException e) {
            log.error(
                    "Continuous discovery scan failed unexpectedly, will retry in next interval.",
                    e);
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
        Set<String> observedTailStateKeys = new HashSet<>();
        boolean tailScanComplete = true;
        long currentTailScanGeneration;
        synchronized (lock) {
            currentTailScanGeneration = ++tailScanGeneration;
        }
        for (TableScanContext ctx : tableScanContexts) {
            List<FileStatus> files = ctx.listFiles(ctx.rootPath);
            scanned += files.size();
            for (FileStatus fileStatus : files) {
                if (ctx.textTailing) {
                    try {
                        if (enqueueTextTailSplit(
                                ctx,
                                fileStatus,
                                currentTailScanGeneration,
                                observedTailStateKeys)) {
                            queued++;
                        }
                    } catch (IOException e) {
                        tailScanComplete = false;
                        log.warn(
                                "Failed to inspect local text file during continuous discovery; "
                                        + "other files will continue to be scanned. file={}",
                                maskUriUserInfo(fileStatus.getPath().toString()),
                                e);
                    } catch (RuntimeException e) {
                        tailScanComplete = false;
                        log.error(
                                "Unexpected failure while inspecting local text file during continuous discovery; "
                                        + "other files will continue to be scanned. file={}",
                                maskUriUserInfo(fileStatus.getPath().toString()),
                                e);
                    }
                    continue;
                }
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
        if (tailScanComplete) {
            synchronized (lock) {
                textTailingInitialScanComplete = true;
            }
            cleanupStaleTailStates(currentTailScanGeneration, observedTailStateKeys);
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

    private boolean enqueueTextTailSplit(
            TableScanContext ctx,
            FileStatus fileStatus,
            long scanGeneration,
            Set<String> observedTailStateKeys)
            throws IOException {
        String filePath = fileStatus.getPath().toString();
        String fileIdentity = LocalFileIdentity.read(filePath);
        String fileKey = tailingFileKey(ctx.tableId, fileIdentity);
        observedTailStateKeys.add(fileKey);
        FileTailState tailState;
        synchronized (lock) {
            tailState = fileTailStates.get(fileKey);
            if (tailState != null) {
                tailState =
                        new FileTailState(
                                tailState.getTableId(),
                                tailState.getFileIdentity(),
                                filePath,
                                tailState.getCommittedOffset(),
                                tailState.getContentAnchor(),
                                tailState.isDiscardUntilDelimiter(),
                                scanGeneration);
                fileTailStates.put(fileKey, tailState);
            }
            if (hasOutstandingTailSplit(ctx.tableId, fileIdentity)) {
                return false;
            }
        }
        if (tailState == null) {
            Long legacyOffset;
            synchronized (lock) {
                legacyOffset =
                        legacyProcessedFileOffsets.remove(tailingFileKey(ctx.tableId, filePath));
            }
            boolean initialLatest;
            synchronized (lock) {
                initialLatest =
                        !textTailingInitialScanComplete && startMode == FileStartMode.LATEST;
            }
            long initialOffset =
                    legacyOffset != null
                            ? legacyOffset
                            : initialLatest
                                    ? fileStatus.getLen()
                                    : ctx.findInitialRowOffset(filePath, fileStatus.getLen());
            if (initialOffset < 0L) {
                return false;
            }
            boolean discardUntilDelimiter =
                    initialLatest
                            && initialOffset > 0L
                            && !ctx.endsWithDelimiter(filePath, initialOffset);
            tailState =
                    new FileTailState(
                            ctx.tableId,
                            fileIdentity,
                            filePath,
                            initialOffset,
                            ctx.contentAnchor(filePath, initialOffset),
                            discardUntilDelimiter,
                            scanGeneration);
            synchronized (lock) {
                FileTailState existing = fileTailStates.putIfAbsent(fileKey, tailState);
                if (existing != null) {
                    tailState = existing;
                }
            }
        }

        long committedOffset = tailState.getCommittedOffset();
        if (fileStatus.getLen() < committedOffset
                || !Objects.equals(
                        tailState.getContentAnchor(),
                        ctx.contentAnchor(filePath, committedOffset))) {
            log.warn(
                    "Local text file content changed before the committed offset; restarting from the first configured row boundary. file={}",
                    maskUriUserInfo(filePath));
            committedOffset = ctx.findInitialRowOffset(filePath, fileStatus.getLen());
            if (committedOffset < 0L) {
                return false;
            }
            tailState =
                    new FileTailState(
                            ctx.tableId,
                            fileIdentity,
                            filePath,
                            committedOffset,
                            ctx.contentAnchor(filePath, committedOffset),
                            false,
                            scanGeneration);
            synchronized (lock) {
                fileTailStates.put(fileKey, tailState);
            }
        }

        if (tailState.isDiscardUntilDelimiter()) {
            long firstDelimiterEnd =
                    ctx.findFirstDelimiterEnd(filePath, committedOffset, fileStatus.getLen());
            long discardEnd = firstDelimiterEnd < 0L ? fileStatus.getLen() : firstDelimiterEnd;
            tailState =
                    new FileTailState(
                            ctx.tableId,
                            fileIdentity,
                            filePath,
                            discardEnd,
                            ctx.contentAnchor(filePath, discardEnd),
                            firstDelimiterEnd < 0L,
                            scanGeneration);
            synchronized (lock) {
                fileTailStates.put(fileKey, tailState);
            }
            committedOffset = discardEnd;
            if (firstDelimiterEnd < 0L) {
                return false;
            }
        }

        long completeEnd =
                ctx.findLastCompleteRowEnd(filePath, committedOffset, fileStatus.getLen());
        if (completeEnd <= committedOffset) {
            return false;
        }
        if (!fileIdentity.equals(LocalFileIdentity.read(filePath))) {
            return false;
        }
        return enqueueSplitIfAbsent(
                new FileSourceSplit(
                        ctx.tableId,
                        filePath,
                        committedOffset,
                        completeEnd - committedOffset,
                        fileIdentity,
                        ctx.contentAnchor(filePath, completeEnd)));
    }

    private boolean hasOutstandingTailSplit(String tableId, String fileIdentity) {
        for (FileSourceSplit split : pendingSplits) {
            if (Objects.equals(tableId, split.getTableId())
                    && Objects.equals(fileIdentity, split.getFileIdentity())) {
                return true;
            }
        }
        for (FileSourceSplit split : inFlightSplits) {
            if (Objects.equals(tableId, split.getTableId())
                    && Objects.equals(fileIdentity, split.getFileIdentity())) {
                return true;
            }
        }
        return false;
    }

    private void cleanupStaleTailStates(long scanGeneration, Set<String> observedTailStateKeys) {
        synchronized (lock) {
            fileTailStates
                    .entrySet()
                    .removeIf(
                            entry ->
                                    !observedTailStateKeys.contains(entry.getKey())
                                            && scanGeneration
                                                            - entry.getValue()
                                                                    .getLastSeenScanGeneration()
                                                    >= TAIL_STATE_MISSING_SCAN_GRACE
                                            && !hasOutstandingTailSplit(
                                                    entry.getValue().getTableId(),
                                                    entry.getValue().getFileIdentity()));
        }
    }

    private static String tailingFileKey(String tableId, String fileIdentity) {
        return tableId + "\u0000" + fileIdentity;
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
        // Binary continuous splits use a stable tableId+filePath id.
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
                List<FileSourceOperationState> restoredOperations =
                        copyOperationStates(entry.getValue());
                pendingOpsByCheckpoint.put(entry.getKey(), restoredOperations);
                for (FileSourceOperationState operation : restoredOperations) {
                    knownSplitVersions.put(
                            operation.getSplitId(),
                            new SplitVersion(
                                    operation.getSourceLength(),
                                    operation.getSourceModificationTime()));
                }
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
                OpCommitResult result = commitSingleOperation(op, entry.getKey());
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

    private OpCommitResult commitSingleOperation(FileSourceOperationState op, long checkpointId) {
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
                return commitDeleteOperation(tableContextOpt.get(), op, checkpointId);
            }
            if (op.getAction() == FilePostSyncAction.BACKUP) {
                return commitBackupOperation(tableContextOpt.get(), op, checkpointId);
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

    private OpCommitResult commitDeleteOperation(
            TableScanContext ctx, FileSourceOperationState op, long checkpointId)
            throws IOException {
        String trashPath = buildDeleteStagingPath(op, checkpointId);
        FileStatus trashedStatus = getFileStatusIfPresent(ctx.sourceFs, trashPath);
        if (trashedStatus == null
                && getFileStatusIfPresent(ctx.sourceFs, op.getSourcePath()) == null) {
            log.info(
                    "Post-sync delete dropped: source and staged file are absent, source={}, "
                            + "trash={}, checkpointId={}",
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(trashPath),
                    checkpointId);
            return OpCommitResult.SUCCESS;
        }

        if (trashedStatus == null) {
            try {
                ctx.sourceFs.renameFile(op.getSourcePath(), trashPath, false);
            } catch (Exception e) {
                log.warn(
                        "Post-sync delete: rename-to-trash failed, will retry: source={}",
                        maskUriUserInfo(op.getSourcePath()),
                        e);
                return OpCommitResult.FAILED_RETRYABLE;
            }
            trashedStatus = getFileStatusIfPresent(ctx.sourceFs, trashPath);
        }

        if (trashedStatus == null) {
            // Another actor removed the staged file after rename. There is no source-side data
            // left for this operation to protect.
            log.info(
                    "Post-sync delete completed externally after staging: source={}, trash={}, "
                            + "checkpointId={}",
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(trashPath),
                    checkpointId);
            return OpCommitResult.SUCCESS;
        }

        if (!isOperationContentMatched(ctx, op, trashPath, trashedStatus)) {
            return handleStaleStagedOperation(
                    ctx, op, checkpointId, trashPath, "delete", trashedStatus);
        }

        if (!isSinkTargetCommitted(ctx, op, checkpointId, trashPath)) {
            return handleRetryableStagedOperation(
                    ctx, op, checkpointId, trashPath, "delete", trashedStatus);
        }

        ctx.sourceFs.deleteFile(trashPath);
        log.info(
                "Post-sync delete completed: source={}, trash={}, checkpointId={}, capturedLen={}, "
                        + "capturedMtime={}",
                maskUriUserInfo(op.getSourcePath()),
                maskUriUserInfo(trashPath),
                checkpointId,
                op.getSourceLength(),
                op.getSourceModificationTime());
        return OpCommitResult.SUCCESS;
    }

    static String buildDeleteStagingPath(FileSourceOperationState op, long checkpointId) {
        Path sourcePath = new Path(op.getSourcePath());
        // Split IDs can contain qualified URIs, so keep the staging file name path-safe.
        String trashFileName = ".st_trash." + checkpointId + "." + sha256Hex(op.getSplitId());
        Path parent = sourcePath.getParent();
        return parent == null ? trashFileName : new Path(parent, trashFileName).toString();
    }

    private static String sha256Hex(String value) {
        try {
            return sha256Hex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not supported by this JVM", e);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int current = bytes[i] & 0xff;
            encoded[i * 2] = digits[current >>> 4];
            encoded[i * 2 + 1] = digits[current & 0x0f];
        }
        return new String(encoded);
    }

    private OpCommitResult commitBackupOperation(
            TableScanContext ctx, FileSourceOperationState op, long checkpointId)
            throws IOException {
        if (StringUtils.isBlank(op.getBackupTargetPath())) {
            log.warn(
                    "Post-sync backup failed: backup target path is empty, splitId={}, source={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()));
            return OpCommitResult.FAILED_RETRYABLE;
        }

        String stagingPath = buildBackupStagingPath(op);
        FileStatus sourceStatus = getFileStatusIfPresent(ctx.sourceFs, op.getSourcePath());
        FileStatus targetStatus = getFileStatusIfPresent(ctx.sourceFs, op.getBackupTargetPath());
        FileStatus stagingStatus = getFileStatusIfPresent(ctx.sourceFs, stagingPath);

        if (sourceStatus == null) {
            if (targetStatus != null) {
                if (isOperationContentMatched(ctx, op, op.getBackupTargetPath(), targetStatus)) {
                    log.info(
                            "Post-sync backup completed during a previous attempt: source={}, target={}, "
                                    + "checkpointId={}, capturedLen={}, capturedMtime={}",
                            maskUriUserInfo(op.getSourcePath()),
                            maskUriUserInfo(op.getBackupTargetPath()),
                            checkpointId,
                            op.getSourceLength(),
                            op.getSourceModificationTime());
                    return OpCommitResult.SUCCESS;
                }
                log.error(
                        "Post-sync backup recovery found an inconsistent target; operation will be "
                                + "retried without deleting data: splitId={}, source={}, target={}, "
                                + "capturedLen={}, capturedMtime={}, actualLen={}, actualMtime={}",
                        op.getSplitId(),
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(op.getBackupTargetPath()),
                        op.getSourceLength(),
                        op.getSourceModificationTime(),
                        targetStatus.getLen(),
                        targetStatus.getModificationTime());
                return OpCommitResult.FAILED_RETRYABLE;
            }
            if (stagingStatus == null) {
                log.warn(
                        "Post-sync backup cannot determine completion because source, staging, and "
                                + "backup target are absent; operation will be retried: splitId={}, "
                                + "source={}, target={}, checkpointId={}",
                        op.getSplitId(),
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(op.getBackupTargetPath()),
                        checkpointId);
                return OpCommitResult.FAILED_RETRYABLE;
            }
        }

        if (targetStatus != null && sourceStatus != null) {
            // Never use an existing target as proof that this source can be deleted: it may belong
            // to a previous attempt while a writer has recreated the source path.
            log.warn(
                    "Post-sync backup skipped because target already exists; source is retained: "
                            + "splitId={}, source={}, target={}, checkpointId={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(op.getBackupTargetPath()),
                    checkpointId);
            return OpCommitResult.STALE_SKIPPED;
        }

        if (sourceStatus != null
                && stagingStatus == null
                && !isSinkTargetCommitted(ctx, op, checkpointId, op.getSourcePath())) {
            // Keep the source visible until the sink target reaches its final committed location.
            // This avoids unnecessary source-side rename/restore churn on file systems such as FTP
            // where a staged move can temporarily hide the discovery root before the sink commit is
            // actually durable.
            return OpCommitResult.FAILED_RETRYABLE;
        }

        if (stagingStatus == null) {
            try {
                ctx.sourceFs.renameFile(op.getSourcePath(), stagingPath, false);
            } catch (Exception e) {
                log.warn(
                        "Post-sync backup: rename-to-staging failed, will retry: source={}, staging={}",
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(stagingPath),
                        e);
                return OpCommitResult.FAILED_RETRYABLE;
            }
            stagingStatus = getFileStatusIfPresent(ctx.sourceFs, stagingPath);
        }

        if (stagingStatus == null) {
            log.warn(
                    "Post-sync backup staging disappeared before verification; operation will be retried: "
                            + "splitId={}, source={}, staging={}, checkpointId={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(stagingPath),
                    checkpointId);
            return OpCommitResult.FAILED_RETRYABLE;
        }

        if (!isOperationContentMatched(ctx, op, stagingPath, stagingStatus)) {
            return handleStaleStagedOperation(
                    ctx, op, checkpointId, stagingPath, "backup", stagingStatus);
        }

        if (!isSinkTargetCommitted(ctx, op, checkpointId, stagingPath)) {
            return handleRetryableStagedOperation(
                    ctx, op, checkpointId, stagingPath, "backup", stagingStatus);
        }

        ctx.sourceFs.renameFile(stagingPath, op.getBackupTargetPath(), false);
        targetStatus = getFileStatusIfPresent(ctx.sourceFs, op.getBackupTargetPath());
        if (targetStatus == null) {
            log.warn(
                    "Post-sync backup promotion target is absent after rename; operation will be retried: "
                            + "splitId={}, source={}, target={}, checkpointId={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(op.getBackupTargetPath()),
                    checkpointId);
            return OpCommitResult.FAILED_RETRYABLE;
        }
        if (!isOperationContentMatched(ctx, op, op.getBackupTargetPath(), targetStatus)) {
            log.warn(
                    "Post-sync backup promoted an unexpected target version; operation will be retried: "
                            + "splitId={}, source={}, target={}, checkpointId={}",
                    op.getSplitId(),
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(op.getBackupTargetPath()),
                    checkpointId);
            return OpCommitResult.FAILED_RETRYABLE;
        }

        log.info(
                "Post-sync backup completed: source={}, target={}, checkpointId={}, "
                        + "capturedLen={}, capturedMtime={}",
                maskUriUserInfo(op.getSourcePath()),
                maskUriUserInfo(op.getBackupTargetPath()),
                checkpointId,
                op.getSourceLength(),
                op.getSourceModificationTime());
        return OpCommitResult.SUCCESS;
    }

    /**
     * Verify the final sink object before mutating its source counterpart.
     *
     * <p>Source and sink checkpoint completion callbacks are independent. A source enumerator can
     * therefore observe the completed checkpoint before the sink committer has renamed its
     * temporary file. Checking both length and the checkpoint-captured content prevents post-sync
     * delete or backup from racing ahead of that final sink commit, including when an older
     * same-length target already exists.
     */
    private boolean isSinkTargetCommitted(
            TableScanContext ctx,
            FileSourceOperationState op,
            long checkpointId,
            String sourcePathToCompareWhenFingerprintMissing) {
        String targetPath = ctx.targetFilePath(op.getSourcePath());
        FileStatus targetStatus;
        try {
            targetStatus = getFileStatusIfPresent(ctx.targetFs, targetPath);
            if (targetStatus == null || targetStatus.getLen() != op.getSourceLength()) {
                log.info(
                        "Post-sync operation is waiting for sink target: action={}, source={}, "
                                + "target={}, checkpointId={}, expectedLen={}, actualLen={}",
                        op.getAction(),
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(targetPath),
                        checkpointId,
                        op.getSourceLength(),
                        targetStatus == null ? null : targetStatus.getLen());
                return false;
            }
            if (!isSinkTargetContentMatched(
                    ctx, op, targetPath, sourcePathToCompareWhenFingerprintMissing)) {
                log.info(
                        "Post-sync operation is waiting for sink target content: action={}, "
                                + "source={}, target={}, checkpointId={}",
                        op.getAction(),
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(targetPath),
                        checkpointId);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn(
                    "Post-sync operation cannot verify sink target and will be retried: action={}, "
                            + "source={}, target={}, checkpointId={}",
                    op.getAction(),
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(targetPath),
                    checkpointId,
                    e);
            return false;
        }
    }

    private boolean isSinkTargetContentMatched(
            TableScanContext ctx,
            FileSourceOperationState op,
            String targetPath,
            String sourcePathToCompareWhenFingerprintMissing)
            throws IOException {
        if (StringUtils.isNotBlank(op.getSourceContentFingerprint())) {
            return Objects.equals(
                    op.getSourceContentFingerprint(),
                    calculateContentFingerprint(ctx.targetFs, targetPath));
        }
        if (StringUtils.isBlank(sourcePathToCompareWhenFingerprintMissing)) {
            return false;
        }
        return ctx.fileContentEquals(sourcePathToCompareWhenFingerprintMissing, targetPath);
    }

    private boolean isOperationContentMatched(
            TableScanContext ctx,
            FileSourceOperationState op,
            String candidatePath,
            FileStatus candidateStatus)
            throws IOException {
        if (candidateStatus == null) {
            return false;
        }
        if (StringUtils.isNotBlank(op.getSourceContentFingerprint())) {
            return Objects.equals(
                    op.getSourceContentFingerprint(),
                    calculateContentFingerprint(ctx.sourceFs, candidatePath));
        }
        return isVersionMatched(candidateStatus, op);
    }

    private FileStatus getFileStatusIfPresent(HadoopFileSystemProxy sourceFs, String path)
            throws IOException {
        try {
            return sourceFs.getFileStatus(path);
        } catch (java.io.FileNotFoundException e) {
            return null;
        }
    }

    private boolean isVersionMatched(FileStatus status, FileSourceOperationState op) {
        return status.getLen() == op.getSourceLength()
                && status.getModificationTime() == op.getSourceModificationTime();
    }

    private OpCommitResult handleStaleStagedOperation(
            TableScanContext ctx,
            FileSourceOperationState op,
            long checkpointId,
            String stagedPath,
            String actionLabel,
            FileStatus stagedStatus)
            throws IOException {
        RestoreStagedFileResult restoreResult =
                restoreStagedSource(ctx, op, stagedPath, stagedStatus, actionLabel);
        if (restoreResult == RestoreStagedFileResult.FAILED) {
            log.error(
                    "Post-sync {}: failed to restore staged file after stale-content detection; "
                            + "operation will be retried: source={}, staging={}, checkpointId={}",
                    actionLabel,
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(stagedPath),
                    checkpointId);
            return OpCommitResult.FAILED_RETRYABLE;
        }
        log.warn(
                "Post-sync {} skipped due to stale staged content: splitId={}, source={}, staging={}, "
                        + "checkpointId={}",
                actionLabel,
                op.getSplitId(),
                maskUriUserInfo(op.getSourcePath()),
                maskUriUserInfo(stagedPath),
                checkpointId);
        return OpCommitResult.STALE_SKIPPED;
    }

    private OpCommitResult handleRetryableStagedOperation(
            TableScanContext ctx,
            FileSourceOperationState op,
            long checkpointId,
            String stagedPath,
            String actionLabel,
            FileStatus stagedStatus)
            throws IOException {
        RestoreStagedFileResult restoreResult =
                restoreStagedSource(ctx, op, stagedPath, stagedStatus, actionLabel);
        if (restoreResult == RestoreStagedFileResult.FAILED) {
            log.warn(
                    "Post-sync {} cannot restore staged source while waiting for sink target; "
                            + "operation will be retried with staged file intact: source={}, staging={}, "
                            + "checkpointId={}",
                    actionLabel,
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(stagedPath),
                    checkpointId);
        }
        return OpCommitResult.FAILED_RETRYABLE;
    }

    private RestoreStagedFileResult restoreStagedSource(
            TableScanContext ctx,
            FileSourceOperationState op,
            String stagedPath,
            FileStatus stagedStatus,
            String actionLabel)
            throws IOException {
        FileStatus currentSourceStatus = getFileStatusIfPresent(ctx.sourceFs, op.getSourcePath());
        if (currentSourceStatus != null) {
            if (ctx.fileContentEquals(stagedPath, op.getSourcePath())) {
                ctx.sourceFs.deleteFile(stagedPath);
                log.info(
                        "Post-sync {} found identical source content already restored; deleted staged file: "
                                + "source={}, staging={}",
                        actionLabel,
                        maskUriUserInfo(op.getSourcePath()),
                        maskUriUserInfo(stagedPath));
                return RestoreStagedFileResult.ALREADY_VISIBLE;
            }
            return RestoreStagedFileResult.FAILED;
        }

        try {
            ctx.sourceFs.renameFile(stagedPath, op.getSourcePath(), false);
            return RestoreStagedFileResult.RESTORED;
        } catch (Exception restoreEx) {
            log.debug(
                    "Post-sync {} restore failed: source={}, staging={}",
                    actionLabel,
                    maskUriUserInfo(op.getSourcePath()),
                    maskUriUserInfo(stagedPath),
                    restoreEx);
            return RestoreStagedFileResult.FAILED;
        }
    }

    private static String buildBackupStagingPath(FileSourceOperationState op) {
        return op.getBackupTargetPath() + ".staging";
    }

    private String calculateContentFingerprint(HadoopFileSystemProxy fs, String filePath)
            throws IOException {
        try (InputStream inputStream = fs.getInputStream(filePath)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8 * 1024];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return sha256Hex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is not supported by this JVM", e);
        }
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
            if (resolveBackupCreatedTimeMillis(status) > expireBefore) {
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

    private long resolveBackupCreatedTimeMillis(FileStatus status) {
        java.util.regex.Matcher matcher =
                BACKUP_VERSION_SUFFIX_PATTERN.matcher(status.getPath().getName());
        if (matcher.matches() && matcher.group(3) != null) {
            try {
                return Long.parseLong(matcher.group(3));
            } catch (NumberFormatException ignored) {
                // Fall through to filesystem mtime for malformed legacy file names.
            }
        }
        return status.getModificationTime();
    }

    private FileSourceOperationState buildOperationStateFromFinishedSplit(
            TableScanContext tableScanContext,
            InFlightSplitContext inFlightSplitContext,
            String sourceContentFingerprint) {
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
            long backupCreatedTimeMillis = System.currentTimeMillis();
            String versionedRelativePath =
                    relativePath
                            + ".v"
                            + splitVersion.length
                            + "_"
                            + splitVersion.modificationTime
                            + "_"
                            + backupCreatedTimeMillis;
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
                backupTargetPath,
                sourceContentFingerprint);
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
            FileFormat fileFormat = c.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
            boolean localTextTailing = isLocalTextTailing(cfg);
            if (localTextTailing) {
                String sourcePath = c.get(FileBaseSourceOptions.FILE_PATH);
                try {
                    LocalFileIdentity.read(sourcePath);
                } catch (NoSuchFileException e) {
                    throw new FileConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            "LocalFile continuous text tailing path does not exist: "
                                    + maskUriUserInfo(sourcePath),
                            e);
                } catch (IOException e) {
                    throw new FileConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            "LocalFile continuous text tailing requires a filesystem that exposes a stable file key for the configured path.",
                            e);
                }
            }
            if (localTextTailing && syncMode != FileSyncMode.FULL) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "LocalFile continuous text tailing requires sync_mode=full.");
            }
            if (localTextTailing
                    && (c.get(FileBaseSourceOptions.COMPRESS_CODEC) != CompressFormat.NONE
                            || c.get(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC)
                                    != ArchiveCompressFormat.NONE)) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "LocalFile continuous text tailing does not support compressed files.");
            }
            if (localTextTailing
                    && c.get(FileBaseSourceOptions.POST_SYNC_ACTION) != FilePostSyncAction.NONE) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "LocalFile continuous text tailing requires post_sync_action=none.");
            }
            if (localTextTailing
                    && !StandardCharsets.UTF_8
                            .name()
                            .equalsIgnoreCase(c.get(FileBaseSourceOptions.ENCODING))) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "LocalFile continuous text tailing currently requires encoding=UTF-8.");
            }
            if (localTextTailing && c.get(FileBaseSourceOptions.ROW_DELIMITER).isEmpty()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "LocalFile continuous text tailing requires a non-empty row_delimiter.");
            }
            if (!localTextTailing && syncMode != FileSyncMode.UPDATE) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "discovery_mode=continuous currently requires sync_mode=update.");
            }
            if (!localTextTailing && fileFormat != FileFormat.BINARY) {
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

    private static boolean isLocalTextTailing(BaseFileSourceConfig config) {
        ReadonlyConfig readonlyConfig = config.getBaseFileSourceConfig();
        return readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE) == FileFormat.TEXT
                && FileSystemType.LOCAL.getFileSystemPluginName().equals(config.getPluginName());
    }

    private static void validatePostSyncConfig(BaseFileSourceConfig baseFileSourceConfig) {
        ReadonlyConfig config = baseFileSourceConfig.getBaseFileSourceConfig();
        FilePostSyncAction action = config.get(FileBaseSourceOptions.POST_SYNC_ACTION);
        if (action == FilePostSyncAction.NONE) {
            return;
        }
        Optional<String> backupPath = config.getOptional(FileBaseSourceOptions.BACKUP_PATH);
        Optional<Duration> retentionMaxAge =
                config.getOptional(FileBaseSourceOptions.RETENTION_MAX_AGE);
        Duration retentionCheckInterval =
                config.get(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL);

        if (action != FilePostSyncAction.NONE) {
            validatePostSyncPathSafety(
                    config.get(FileBaseSourceOptions.FILE_PATH),
                    baseFileSourceConfig.getHadoopConfig());
        }
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
            try (HadoopFileSystemProxy fs = new HadoopFileSystemProxy(hadoopConf)) {
                String qualifiedSource = fs.makeQualifiedPath(sourcePath);
                String qualifiedBackup = fs.makeQualifiedPath(backupPath);
                if (isPathOverlappedQualified(qualifiedSource, qualifiedBackup)) {
                    throw new FileConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            "backup_path must not overlap with path. Please configure backup_path outside the scanned path tree.");
                }
                return;
            } catch (FileConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Cannot validate backup_path against path using canonical filesystem paths. "
                                + "Refusing to enable post_sync_action=backup until the path relationship "
                                + "can be verified.",
                        e);
            }
        }
        throw new FileConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "post_sync_action=backup currently only supports same-filesystem backup in phase-1. "
                        + "Please configure backup_path with the same scheme and authority as path.");
    }

    /**
     * Compares two fully-qualified path URIs for overlap. Local filesystem paths are resolved with
     * {@link java.nio.file.Path#toRealPath(java.nio.file.LinkOption...)} so symlink aliases cannot
     * bypass the backup/source tree boundary.
     */
    private static boolean isPathOverlappedQualified(String qualifiedSource, String qualifiedBackup)
            throws IOException {
        Path sourcePath = new Path(qualifiedSource);
        Path backupPath = new Path(qualifiedBackup);

        String sourceScheme = sourcePath.toUri().getScheme();
        String sourcePathStr;
        String backupPathStr;
        if (sourceScheme != null && "file".equalsIgnoreCase(sourceScheme)) {
            sourcePathStr = resolveLocalPathForOverlap(sourcePath);
            backupPathStr = resolveLocalPathForOverlap(backupPath);
        } else {
            sourcePathStr = trimTrailingPathSeparator(sourcePath.toUri().getPath());
            backupPathStr = trimTrailingPathSeparator(backupPath.toUri().getPath());
        }

        if (StringUtils.isBlank(sourcePathStr) || StringUtils.isBlank(backupPathStr)) {
            return false;
        }
        return Objects.equals(sourcePathStr, backupPathStr)
                || isParentPathQualified(sourcePathStr, backupPathStr)
                || isParentPathQualified(backupPathStr, sourcePathStr);
    }

    /**
     * Resolves the existing prefix through real paths, then appends non-existing descendants. This
     * supports a new backup directory while still detecting symlink aliases in its parent path.
     */
    private static String resolveLocalPathForOverlap(Path path) throws IOException {
        java.nio.file.Path requestedPath =
                java.nio.file.Paths.get(path.toUri()).toAbsolutePath().normalize();
        Deque<java.nio.file.Path> missingSegments = new ArrayDeque<>();
        java.nio.file.Path existingAncestor = requestedPath;
        while (!java.nio.file.Files.exists(existingAncestor)) {
            java.nio.file.Path fileName = existingAncestor.getFileName();
            if (fileName == null || existingAncestor.getParent() == null) {
                throw new IOException(
                        "No existing ancestor found while resolving local path " + requestedPath);
            }
            missingSegments.push(fileName);
            existingAncestor = existingAncestor.getParent();
        }

        java.nio.file.Path resolvedPath = existingAncestor.toRealPath();
        while (!missingSegments.isEmpty()) {
            resolvedPath = resolvedPath.resolve(missingSegments.pop());
        }
        return trimTrailingPathSeparator(resolvedPath.normalize().toString());
    }

    private static boolean isParentPathQualified(String parentPath, String childPath) {
        return childPath.startsWith(parentPath + "/");
    }

    /**
     * Rejects post_sync_action=delete|backup when the normalized path resolves to the filesystem
     * root, to prevent mass deletion of source data.
     */
    private static void validatePostSyncPathSafety(String path, HadoopConf hadoopConf) {
        if (StringUtils.isBlank(path)) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "post_sync_action=delete|backup requires a non-empty path.");
        }
        try (HadoopFileSystemProxy fs = new HadoopFileSystemProxy(hadoopConf)) {
            String qualified = fs.makeQualifiedPath(path);
            String pathComponent = new Path(qualified).toUri().getPath();
            if ("/".equals(pathComponent) || pathComponent.isEmpty()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "post_sync_action=delete|backup requires path to be a non-root directory. "
                                + "Refusing to operate on filesystem root to prevent mass deletion.");
            }
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            // Best-effort: if filesystem not initialized, check raw path
            String pathPart = new Path(path).toUri().getPath();
            if (pathPart == null || "/".equals(pathPart) || pathPart.isEmpty()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "post_sync_action=delete|backup requires path to be a non-root directory. "
                                + "Refusing to operate on filesystem root to prevent mass deletion.");
            }
            log.warn(
                    "Cannot qualify path for safety check, using raw path validation: {}", path, e);
        }
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
        private final boolean textTailing;
        private final byte[] rowDelimiterBytes;
        private final int[] rowDelimiterPrefix;
        private final long skipHeaderRowNumber;

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
            this.textTailing = isLocalTextTailing(baseFileSourceConfig);
            this.rowDelimiterBytes =
                    textTailing
                            ? config.get(FileBaseSourceOptions.ROW_DELIMITER)
                                    .getBytes(
                                            Charset.forName(
                                                    config.get(FileBaseSourceOptions.ENCODING)))
                            : new byte[0];
            this.rowDelimiterPrefix = buildPrefixTable(rowDelimiterBytes);
            this.skipHeaderRowNumber =
                    textTailing ? config.get(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER) : 0L;

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
            if (postSyncAction == FilePostSyncAction.BACKUP) {
                this.backupPath =
                        config.getOptional(FileBaseSourceOptions.BACKUP_PATH)
                                .map(sourceFs::makeQualifiedPath)
                                .orElse(null);
                this.retentionMaxAge =
                        config.getOptional(FileBaseSourceOptions.RETENTION_MAX_AGE).orElse(null);
                this.retentionCheckInterval =
                        config.get(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL);
            } else {
                this.backupPath = null;
                this.retentionMaxAge = null;
                this.retentionCheckInterval =
                        FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.defaultValue();
            }
            this.recursiveFileScan = config.get(FileBaseSourceOptions.RECURSIVE_FILE_SCAN);

            if (textTailing) {
                this.targetFs = null;
                this.shareTargetFs = false;
            } else {
                String targetPath = config.get(FileBaseSourceOptions.TARGET_PATH);
                Map<String, String> targetHadoopConf =
                        config.getOptional(FileBaseSourceOptions.TARGET_HADOOP_CONF).orElse(null);
                HadoopConf targetConf =
                        buildTargetHadoopConf(hadoopConf, targetPath, targetHadoopConf);
                if (targetConf == hadoopConf) {
                    this.targetFs = this.sourceFs;
                    this.shareTargetFs = true;
                } else {
                    this.targetFs = new HadoopFileSystemProxy(targetConf);
                    this.shareTargetFs = false;
                }
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

        private long findLastCompleteRowEnd(String filePath, long start, long fileSize)
                throws IOException {
            return scanDelimiterEnd(filePath, start, fileSize, false);
        }

        private long findFirstDelimiterEnd(String filePath, long start, long fileSize)
                throws IOException {
            return scanDelimiterEnd(filePath, start, fileSize, true);
        }

        private long scanDelimiterEnd(
                String filePath, long start, long fileSize, boolean returnFirst)
                throws IOException {
            if (start >= fileSize) {
                return returnFirst ? -1L : start;
            }
            try (FSDataInputStream input = sourceFs.getInputStream(filePath)) {
                input.seek(start);
                byte[] buffer = new byte[64 * 1024];
                long position = start;
                long matchedEnd = returnFirst ? -1L : start;
                int delimiterIndex = 0;
                int read;
                while (position < fileSize
                        && (read =
                                        input.read(
                                                buffer,
                                                0,
                                                (int) Math.min(buffer.length, fileSize - position)))
                                != -1) {
                    for (int i = 0; i < read; i++) {
                        byte current = buffer[i];
                        position++;
                        while (delimiterIndex > 0 && current != rowDelimiterBytes[delimiterIndex]) {
                            delimiterIndex = rowDelimiterPrefix[delimiterIndex - 1];
                        }
                        if (current == rowDelimiterBytes[delimiterIndex]) {
                            delimiterIndex++;
                            if (delimiterIndex == rowDelimiterBytes.length) {
                                matchedEnd = position;
                                if (returnFirst) {
                                    return matchedEnd;
                                }
                                delimiterIndex = rowDelimiterPrefix[delimiterIndex - 1];
                            }
                        }
                    }
                }
                return matchedEnd;
            }
        }

        private long findInitialRowOffset(String filePath, long fileSize) throws IOException {
            if (skipHeaderRowNumber <= 0L) {
                return 0L;
            }
            long position = 0L;
            for (long completedRows = 0L; completedRows < skipHeaderRowNumber; completedRows++) {
                position = findFirstDelimiterEnd(filePath, position, fileSize);
                if (position < 0L) {
                    return -1L;
                }
            }
            return position;
        }

        private boolean endsWithDelimiter(String filePath, long fileSize) throws IOException {
            if (fileSize < rowDelimiterBytes.length) {
                return false;
            }
            byte[] suffix = new byte[rowDelimiterBytes.length];
            try (FSDataInputStream input = sourceFs.getInputStream(filePath)) {
                input.seek(fileSize - rowDelimiterBytes.length);
                input.readFully(suffix);
            }
            return java.util.Arrays.equals(suffix, rowDelimiterBytes);
        }

        private String contentAnchor(String filePath, long offset) throws IOException {
            int prefixLength = (int) Math.min(2048L, offset);
            int suffixLength = (int) Math.min(2048L, Math.max(0L, offset - prefixLength));
            byte[] anchor = new byte[prefixLength + suffixLength];
            try (FSDataInputStream input = sourceFs.getInputStream(filePath)) {
                if (prefixLength > 0) {
                    input.readFully(anchor, 0, prefixLength);
                }
                if (suffixLength > 0) {
                    input.seek(offset - suffixLength);
                    input.readFully(anchor, prefixLength, suffixLength);
                }
            }
            return sha256Hex(anchor);
        }

        private static int[] buildPrefixTable(byte[] delimiter) {
            int[] prefix = new int[delimiter.length];
            int matched = 0;
            for (int i = 1; i < delimiter.length; i++) {
                while (matched > 0 && delimiter[i] != delimiter[matched]) {
                    matched = prefix[matched - 1];
                }
                if (delimiter[i] == delimiter[matched]) {
                    matched++;
                    prefix[i] = matched;
                }
            }
            return prefix;
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
                if (status.getLen() <= 0 && !textTailing) {
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
            if (textTailing) {
                return true;
            }
            if (startMode == FileStartMode.LATEST
                    && sourceFileStatus.getModificationTime() <= baselineStartMillis) {
                return false;
            }
            return shouldSyncInUpdateMode(sourceFileStatus);
        }

        private boolean shouldSyncInUpdateMode(FileStatus sourceFileStatus) throws IOException {
            String sourceFilePath = sourceFileStatus.getPath().toString();
            String targetFilePath = targetFilePath(sourceFilePath);

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

        private String targetFilePath(String sourceFilePath) {
            String relativePath = resolveRelativePath(rootPath, sourceFilePath);
            String targetPath = config.get(FileBaseSourceOptions.TARGET_PATH);
            return buildTargetFilePath(targetPath, relativePath);
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

    private enum RestoreStagedFileResult {
        RESTORED,
        ALREADY_VISIBLE,
        FAILED
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
