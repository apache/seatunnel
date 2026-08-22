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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.apache.seatunnel.engine.common.config.server.TelemetryLogsConfig;
import org.apache.seatunnel.engine.common.utils.LogUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the local lifecycle of job log files for one engine node.
 *
 * <p>The service intentionally splits retention into two independent code paths so that the
 * decision about WHEN to remove a file does not conflict with the decision about WHICH files belong
 * to a job:
 *
 * <ul>
 *   <li>{@link #clean(long)} is the terminal-state remover. It is invoked from {@code
 *       JobHistoryService.JobInfoExpiredListener} after the history entry expires, and removes
 *       every remaining file (active, rolled, and unclassified) that belongs exactly to the given
 *       job id.
 *   <li>{@link #pruneRolledJobLogSegments(Duration)} is the running-state remover. It is scheduled
 *       by {@link #startPrune(long)} and only deletes rolled segments whose last-modified age
 *       exceeds the configured retention window. Active and unclassified files are NEVER touched
 *       here so that long-running jobs keep their current log file intact while the local node
 *       sheds old rolled segments.
 * </ul>
 */
@Slf4j
public class TaskLogManagerService {

    /** Mark the prune executor as started exactly once. */
    private final AtomicBoolean pruneStarted = new AtomicBoolean(false);

    /**
     * Dedicated single-thread executor for prune work; isolated from the master monitoring loop.
     */
    private volatile ScheduledExecutorService pruneExecutor;

    private String path;

    public TaskLogManagerService(TelemetryLogsConfig log) {}

    public void initClean() {
        try {
            path = LogUtil.getLogPath();
        } catch (IllegalArgumentException e) {
            // When log4j appender is not configured (e.g. local example / custom log config), avoid
            // polluting logs with stack traces
            log.debug(
                    "The corresponding log file path is not properly configured, please check the log configuration file. {}",
                    e.getMessage());
        } catch (Exception e) {
            log.debug(
                    "The corresponding log file path is not properly configured, please check the log configuration file.",
                    e);
        }
    }

    /**
     * Remove every log file that exactly belongs to the supplied job id. Invoked by {@code
     * JobHistoryService} after the history entry expires; idempotent because each call goes through
     * {@code Files.deleteIfExists}.
     */
    public void clean(long jobId) {
        if (path == null || jobId <= 0) {
            return;
        }
        log.info("Cleaning logs for jobId: {} , path : {}", jobId, path);
        String[] logFiles = getLogFiles(jobId, path);
        for (String logFile : logFiles) {
            try {
                Files.deleteIfExists(resolveLogPath(path, logFile));
            } catch (IOException e) {
                log.warn("Failed to delete log file: {}", logFile, e);
            }
        }
    }

    /**
     * Periodically remove rolled job log segments whose last-modified age exceeds {@code ttl}.
     *
     * <p>Only files matching {@link JobLogFileNameMatcher#isRolledSegment(String)} are removed.
     * Active files ({@code job-<id>.log}) and unclassified sidecars ({@code
     * job-<id>.log.unclassified}) are intentionally preserved because they are owned by the active
     * lifecycle and must only be removed at job terminal state by {@link #clean(long)}.
     *
     * @param pruneIntervalMinutes how often to scan, in minutes. Must be positive; non-positive
     *     values disable the scheduled prune.
     * @param ttl age after which a rolled segment is eligible for deletion
     */
    public void startPrune(long pruneIntervalMinutes, Duration ttl) {
        if (pruneIntervalMinutes <= 0) {
            log.info("Job log prune is disabled (intervalMinutes={})", pruneIntervalMinutes);
            return;
        }
        if (!pruneStarted.compareAndSet(false, true)) {
            return;
        }
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            log.warn(
                    "Invalid job log prune TTL {}, skipping scheduled prune.", String.valueOf(ttl));
            pruneStarted.set(false);
            return;
        }
        pruneExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "task-log-prune");
                            thread.setDaemon(true);
                            return thread;
                        });
        pruneExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        pruneRolledJobLogSegments(ttl);
                    } catch (Throwable t) {
                        log.warn("Job log prune iteration failed", t);
                    }
                },
                pruneIntervalMinutes,
                pruneIntervalMinutes,
                TimeUnit.MINUTES);
        log.info("Started job log prune: every {} minutes, TTL={}", pruneIntervalMinutes, ttl);
    }

    /**
     * Stop the prune executor. Safe to call multiple times. Subsequent {@link #startPrune(long,
     * Duration)} calls become no-ops because the {@link #pruneStarted} flag remains set; this
     * mirrors the engine's lifecycle where the log manager is constructed once per node.
     */
    public void shutdown() {
        ScheduledExecutorService executor = pruneExecutor;
        if (executor != null) {
            executor.shutdownNow();
            pruneExecutor = null;
        }
    }

    /**
     * Remove rolled job log segments older than {@code ttl}. The active log file is never touched;
     * terminal removal of the active file is delegated to {@link #clean(long)} which runs when the
     * owning job leaves the running state.
     *
     * @param ttl maximum allowed age for a rolled segment
     */
    void pruneRolledJobLogSegments(Duration ttl) {
        if (path == null) {
            return;
        }
        File logDir = new File(path);
        if (!logDir.exists() || !logDir.isDirectory()) {
            return;
        }
        File[] candidates = logDir.listFiles();
        if (candidates == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long ttlMillis = ttl.toMillis();
        int deleted = 0;
        int failed = 0;
        for (File file : candidates) {
            if (!file.isFile()) {
                continue;
            }
            String name = file.getName();
            if (!JobLogFileNameMatcher.isRolledSegment(name)) {
                continue;
            }
            long age = now - file.lastModified();
            if (age < ttlMillis) {
                continue;
            }
            try {
                Files.deleteIfExists(file.toPath());
                deleted++;
            } catch (IOException e) {
                failed++;
                log.warn("Failed to prune rolled job log segment: {}", name, e);
            }
        }
        if (deleted > 0 || failed > 0) {
            log.info(
                    "Pruned rolled job log segments under {}: deleted={}, failed={}",
                    path,
                    deleted,
                    failed);
        }
    }

    private String[] getLogFiles(long jobId, String path) {
        File logDir = new File(path);
        if (!logDir.exists() || !logDir.isDirectory()) {
            return new String[0];
        }

        String[] logFiles =
                logDir.list((dir, name) -> JobLogFileNameMatcher.isJobLogFile(name, jobId));
        return logFiles == null ? new String[0] : logFiles;
    }

    private Path resolveLogPath(String path, String logFile) {
        return Paths.get(path).resolve(logFile);
    }
}
