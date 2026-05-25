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

/**
 * A sample custom dirty record collector discovered via SPI. Logs each dirty record with a
 * distinctive {@code [CountingCollector]} prefix so integration tests can verify that the custom
 * SPI-based collector is actually being used.
 */
@Slf4j
public class CountingDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private DistributedCounter dirtyRecordCounter = new LocalAtomicCounter();
    private long threshold = -1;
    private boolean failOnThreshold = false;

    @Override
    public void init(Config config) {
        if (config.hasPath("threshold")) {
            this.threshold = config.getLong("threshold");
        }
        if (config.hasPath("fail_on_threshold")) {
            this.failOnThreshold = config.getBoolean("fail_on_threshold");
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
        long n = dirtyRecordCounter.value();
        log.error(
                "[CountingCollector] dirty record #{}: SubTask={}, Record={}, Error={}",
                n,
                subTaskIndex,
                dirtyRecord != null ? dirtyRecord.toString() : "null",
                errorMessage != null ? errorMessage : "");
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
            } else {
                log.warn(message);
            }
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
