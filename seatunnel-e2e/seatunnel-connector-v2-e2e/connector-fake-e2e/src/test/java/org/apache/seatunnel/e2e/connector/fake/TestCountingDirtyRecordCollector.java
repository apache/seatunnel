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

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.LogDirtyRecordCollector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Top-level test-only collector that counts dirty rows and logs one summary on close without
 * depending on any enclosing test-harness class.
 */
@Slf4j
public class TestCountingDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private final AtomicLong dirtyRecordCount = new AtomicLong();
    private final AtomicBoolean closeSummaryLogged = new AtomicBoolean();
    private long threshold = -1;
    private boolean failOnThreshold = false;

    @Override
    public void init(Config config) {
        if (config.hasPath(LogDirtyRecordCollector.THRESHOLD.key())) {
            this.threshold = config.getLong(LogDirtyRecordCollector.THRESHOLD.key());
        }
        if (config.hasPath(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key())) {
            this.failOnThreshold =
                    config.getBoolean(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key());
        }
    }

    @Override
    public void collect(
            int subTaskIndex,
            Object dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {
        long count = dirtyRecordCount.incrementAndGet();
        if (threshold > 0 && count >= threshold && failOnThreshold) {
            throw new RuntimeException(
                    String.format(
                            "[CountingCollector] threshold exceeded: %d >= %d", count, threshold));
        }
    }

    @Override
    public long getDirtyRecordCount() {
        return dirtyRecordCount.get();
    }

    @Override
    public void close() {
        if (closeSummaryLogged.compareAndSet(false, true)) {
            log.info("[CountingCollector] total dirty records={}", getDirtyRecordCount());
        }
    }
}
