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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An explicitly selected SPI demonstration collector that counts records without formatting their
 * payloads. It emits one payload-free count summary when closed and at most one nonfatal threshold
 * notice per collector instance.
 */
@Slf4j
public class CountingDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private DistributedCounter dirtyRecordCounter = new LocalAtomicCounter();
    private long threshold = -1;
    private boolean failOnThreshold = false;
    private final AtomicBoolean thresholdNoticeLogged = new AtomicBoolean();
    private final AtomicBoolean closeSummaryLogged = new AtomicBoolean();

    @Override
    public void init(Config config) {
        if (config.hasPath(LogDirtyRecordCollector.THRESHOLD.key())) {
            this.threshold = config.getLong(LogDirtyRecordCollector.THRESHOLD.key());
        }
        if (config.hasPath(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key())) {
            this.failOnThreshold =
                    config.getBoolean(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key());
        }
        log.info(
                "[CountingCollector] initialized, threshold={}, failOnThreshold={}",
                threshold,
                failOnThreshold);
    }

    @Override
    public void setDistributedCounter(DistributedCounter counter) {
        this.dirtyRecordCounter = counter != null ? counter : new LocalAtomicCounter();
        log.debug(
                "[CountingCollector] distributed counter set: {}",
                dirtyRecordCounter.getClass().getName());
    }

    @Override
    public void incrementDistributedCounter() {
        dirtyRecordCounter.add(1L);
    }

    @Override
    public void collect(
            int subTaskIndex,
            Object dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {
        incrementDistributedCounter();
        checkThresholdRuntime();
    }

    @Override
    public long getDirtyRecordCount() {
        return dirtyRecordCounter.value();
    }

    @Override
    public void checkThreshold() throws Exception {
        if (threshold <= 0) {
            return;
        }

        long count = dirtyRecordCounter.value();
        if (count >= threshold) {
            String message =
                    String.format(
                            "[CountingCollector] threshold exceeded: %d >= %d", count, threshold);
            if (failOnThreshold) {
                throw new RuntimeException(message);
            } else if (thresholdNoticeLogged.compareAndSet(false, true)) {
                log.warn(message);
            }
        }
    }

    /**
     * Emits a payload-free count once when this collector instance closes.
     *
     * <p>The summary reflects this instance's counter; it is not a durable job-wide total across
     * worker restarts or separate planning instances.
     */
    @Override
    public void close() {
        if (closeSummaryLogged.compareAndSet(false, true)) {
            log.info("[CountingCollector] total dirty records={}", getDirtyRecordCount());
        }
    }

    private void checkThresholdRuntime() {
        try {
            checkThreshold();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
