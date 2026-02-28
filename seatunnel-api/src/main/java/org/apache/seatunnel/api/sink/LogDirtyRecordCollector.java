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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/** A dirty record collector that logs dirty data information. */
@Slf4j
public class LogDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private String logLevel = "ERROR";
    private boolean includeStackTrace = true;
    // -1 means no threshold
    private long threshold = -1;
    private boolean failOnThreshold = false;
    private final AtomicLong dirtyRecordCount = new AtomicLong(0);
    private transient Object distributedCounter;

    @Override
    public void setDistributedCounter(Object counter) {
        this.distributedCounter = counter;
        log.debug(
                "[LogCollector] distributed counter set: {}",
                counter != null ? counter.getClass().getName() : "null");
    }

    @Override
    public void incrementDistributedCounter() {
        if (distributedCounter == null) {
            return;
        }
        try {
            if (distributedCounter.getClass().getName().contains("LongAccumulator")) {
                // spark LongAccumulator
                distributedCounter
                        .getClass()
                        .getMethod("add", long.class)
                        .invoke(distributedCounter, 1L);
            } else {
                // flink LongCounter or seaTunnel Counter
                try {
                    distributedCounter.getClass().getMethod("inc").invoke(distributedCounter);
                } catch (NoSuchMethodException e) {
                    distributedCounter
                            .getClass()
                            .getMethod("add", long.class)
                            .invoke(distributedCounter, 1L);
                }
            }
        } catch (Exception e) {
            log.trace("Failed to increment distributed counter", e);
        }
    }

    private long getDistributedCount() {
        if (distributedCounter == null) {
            return dirtyRecordCount.get();
        }
        try {
            if (distributedCounter.getClass().getName().contains("LongAccumulator")) {
                // spark LongAccumulator
                return (Long)
                        distributedCounter.getClass().getMethod("value").invoke(distributedCounter);
            } else {
                // flink or seaTunnel Counter
                return (Long)
                        distributedCounter
                                .getClass()
                                .getMethod("getCount")
                                .invoke(distributedCounter);
            }
        } catch (Exception e) {
            log.trace("Failed to get distributed count, using local count", e);
            return dirtyRecordCount.get();
        }
    }

    @Override
    public void init(Config config) {
        if (config.hasPath("log_level")) {
            this.logLevel = config.getString("log_level").toUpperCase();
        }
        if (config.hasPath("include_stack_trace")) {
            this.includeStackTrace = config.getBoolean("include_stack_trace");
        }
        if (config.hasPath("threshold")) {
            this.threshold = config.getLong("threshold");
        }
        if (config.hasPath("fail_on_threshold")) {
            this.failOnThreshold = config.getBoolean("fail_on_threshold");
        }
    }

    @Override
    public void init(Config config, CatalogTable catalogTable) throws Exception {
        if (config != null && config.hasPath("dirty.collector")) {
            init(config.getConfig("dirty.collector"));
        } else if (config != null) {
            init(config);
        } else {
            DirtyRecordCollector.super.init(config, catalogTable);
        }
    }

    @Override
    public void collect(
            int subTaskIndex,
            SeaTunnelRow dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {

        long currentCount = dirtyRecordCount.incrementAndGet();
        incrementDistributedCounter();

        String tableInfo = formatTableInfo(catalogTable);

        String logMessage =
                String.format(
                        "Dirty record collected (exception) - SubTask: %d, %s, Count: %d, Record: %s, Error: %s",
                        subTaskIndex,
                        tableInfo,
                        currentCount,
                        dirtyRecord != null ? dirtyRecord.toString() : "null",
                        errorMessage != null ? errorMessage : "");

        if (includeStackTrace && exception != null) {
            logMessage += "\nException: " + ExceptionUtils.getMessage(exception);
        }

        doLog(logMessage);
        checkThresholdRuntime();
    }

    @Override
    public void collectFromUserRule(
            int subTaskIndex, SeaTunnelRow record, String errorMessage, CatalogTable catalogTable) {

        long currentCount = dirtyRecordCount.incrementAndGet();
        incrementDistributedCounter();

        String tableInfo = formatTableInfo(catalogTable);
        String logMessage =
                String.format(
                        "Dirty record collected (user rule) - SubTask: %d, %s, Count: %d, Record: %s, Reason: %s",
                        subTaskIndex,
                        tableInfo,
                        currentCount,
                        record != null ? record.toString() : "null",
                        errorMessage != null ? errorMessage : "");

        doLog(logMessage);
        checkThresholdRuntime();
    }

    private static String formatTableInfo(CatalogTable catalogTable) {
        return catalogTable != null
                ? String.format(
                        "Table: %s.%s.%s",
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName())
                : "Table: unknown";
    }

    private void doLog(String logMessage) {
        switch (logLevel) {
            case "ERROR":
                log.error(logMessage);
                break;
            case "WARN":
                log.warn(logMessage);
                break;
            case "INFO":
                log.info(logMessage);
                break;
            case "DEBUG":
                log.debug(logMessage);
                break;
            default:
                log.error(logMessage);
        }
    }

    private void checkThresholdRuntime() {
        try {
            checkThreshold();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getDirtyRecordCount() {
        return dirtyRecordCount.get();
    }

    @Override
    public void checkThreshold() throws Exception {
        if (threshold <= 0) {
            return;
        }

        long count = getDistributedCount();
        if (count >= threshold) {
            String message =
                    String.format(
                            "Dirty record threshold exceeded: %d >= %d (distributed=%s)",
                            count, threshold, distributedCounter != null);

            if (failOnThreshold) {
                log.error(message + " - Task will be failed!");
                throw new RuntimeException(message);
            } else {
                log.warn(message + " - Threshold reached but task continues");
            }
        }
    }
}
