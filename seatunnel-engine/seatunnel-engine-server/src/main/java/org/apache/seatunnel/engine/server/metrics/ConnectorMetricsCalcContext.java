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
package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;

public class ConnectorMetricsCalcContext {

    private final MetricsContext metricsContext;

    private final PluginType type;

    // Real-time (attempt) metrics
    private Counter count;

    private final Map<String, Counter> countPerTable = new ConcurrentHashMap<>();

    private Meter QPS;

    private final Map<String, Meter> QPSPerTable = new ConcurrentHashMap<>();

    private Counter bytes;

    private final Map<String, Counter> bytesPerTable = new ConcurrentHashMap<>();

    private Meter bytesPerSeconds;

    private final Map<String, Meter> bytesPerSecondsPerTable = new ConcurrentHashMap<>();

    // Committed metrics
    private Counter committedCount;

    private final Map<String, Counter> committedCountPerTable = new ConcurrentHashMap<>();

    private Meter committedQPS;

    private final Map<String, Meter> committedQPSPerTable = new ConcurrentHashMap<>();

    private Counter committedBytes;

    private final Map<String, Counter> committedBytesPerTable = new ConcurrentHashMap<>();

    private Meter committedBytesPerSeconds;

    private final Map<String, Meter> committedBytesPerSecondsPerTable = new ConcurrentHashMap<>();

    private PendingMetrics currentPendingMetrics;

    private final Map<Long, PendingMetrics> pendingMetricsByCheckpoint = new ConcurrentHashMap<>();

    private final Map<String, String> tableNameCache = new ConcurrentHashMap<>();

    public ConnectorMetricsCalcContext(
            MetricsContext metricsContext,
            PluginType type,
            boolean isMulti,
            List<TablePath> tables) {
        this.metricsContext = metricsContext;
        this.type = type;
        initializeMetrics(isMulti, tables);
    }

    private void initializeMetrics(boolean isMulti, List<TablePath> tables) {
        if (type.equals(PluginType.SINK)) {
            initializeAttemptMetrics(
                    isMulti,
                    tables,
                    SINK_WRITE_COUNT,
                    SINK_WRITE_QPS,
                    SINK_WRITE_BYTES,
                    SINK_WRITE_BYTES_PER_SECONDS);
            initializeCommittedMetrics(isMulti, tables);
            currentPendingMetrics = new PendingMetrics();
        } else if (type.equals(PluginType.SOURCE)) {
            initializeAttemptMetrics(
                    isMulti,
                    tables,
                    SOURCE_RECEIVED_COUNT,
                    SOURCE_RECEIVED_QPS,
                    SOURCE_RECEIVED_BYTES,
                    SOURCE_RECEIVED_BYTES_PER_SECONDS);
        }
    }

    private void initializeAttemptMetrics(
            boolean isMulti,
            List<TablePath> tables,
            String countName,
            String qpsName,
            String bytesName,
            String bytesPerSecondsName) {
        count = metricsContext.counter(countName);
        QPS = metricsContext.meter(qpsName);
        bytes = metricsContext.counter(bytesName);
        bytesPerSeconds = metricsContext.meter(bytesPerSecondsName);
        if (isMulti) {
            tables.forEach(
                    tablePath -> {
                        String fullName = tablePath.getFullName();
                        countPerTable.put(
                                fullName, metricsContext.counter(countName + "#" + fullName));
                        QPSPerTable.put(fullName, metricsContext.meter(qpsName + "#" + fullName));
                        bytesPerTable.put(
                                fullName, metricsContext.counter(bytesName + "#" + fullName));
                        bytesPerSecondsPerTable.put(
                                fullName,
                                metricsContext.meter(bytesPerSecondsName + "#" + fullName));
                    });
        }
    }

    private void initializeCommittedMetrics(boolean isMulti, List<TablePath> tables) {
        committedCount = metricsContext.counter(SINK_COMMITTED_COUNT);
        committedQPS = metricsContext.meter(SINK_COMMITTED_QPS);
        committedBytes = metricsContext.counter(SINK_COMMITTED_BYTES);
        committedBytesPerSeconds = metricsContext.meter(SINK_COMMITTED_BYTES_PER_SECONDS);
        if (isMulti) {
            tables.forEach(
                    tablePath -> {
                        String fullName = tablePath.getFullName();
                        committedCountPerTable.put(
                                fullName,
                                metricsContext.counter(SINK_COMMITTED_COUNT + "#" + fullName));
                        committedQPSPerTable.put(
                                fullName,
                                metricsContext.meter(SINK_COMMITTED_QPS + "#" + fullName));
                        committedBytesPerTable.put(
                                fullName,
                                metricsContext.counter(SINK_COMMITTED_BYTES + "#" + fullName));
                        committedBytesPerSecondsPerTable.put(
                                fullName,
                                metricsContext.meter(
                                        SINK_COMMITTED_BYTES_PER_SECONDS + "#" + fullName));
                    });
        }
    }

    public void updateMetrics(Object data, String tableId) {
        count.inc();
        QPS.markEvent();
        if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            long rowBytes = row.getBytesSize();
            bytes.inc(rowBytes);
            bytesPerSeconds.markEvent(rowBytes);

            String normalizedTableName =
                    StringUtils.isNotBlank(tableId) ? normalizeTableName(tableId) : null;
            if (PluginType.SINK.equals(type)) {
                recordPendingMetrics(normalizedTableName, rowBytes);
            }

            if (StringUtils.isNotBlank(normalizedTableName)) {
                processMetrics(
                        countPerTable,
                        Counter.class,
                        normalizedTableName,
                        SINK_WRITE_COUNT,
                        SOURCE_RECEIVED_COUNT,
                        Counter::inc);

                processMetrics(
                        bytesPerTable,
                        Counter.class,
                        normalizedTableName,
                        SINK_WRITE_BYTES,
                        SOURCE_RECEIVED_BYTES,
                        counter -> counter.inc(rowBytes));

                processMetrics(
                        QPSPerTable,
                        Meter.class,
                        normalizedTableName,
                        SINK_WRITE_QPS,
                        SOURCE_RECEIVED_QPS,
                        Meter::markEvent);

                processMetrics(
                        bytesPerSecondsPerTable,
                        Meter.class,
                        normalizedTableName,
                        SINK_WRITE_BYTES_PER_SECONDS,
                        SOURCE_RECEIVED_BYTES_PER_SECONDS,
                        meter -> meter.markEvent(rowBytes));
            }
        }
    }

    public void sealCheckpointMetrics(long checkpointId) {
        if (!PluginType.SINK.equals(type)) {
            return;
        }
        PendingMetrics pendingToSeal = currentPendingMetrics;
        currentPendingMetrics = new PendingMetrics();
        if (pendingToSeal.isEmpty()) {
            return;
        }
        pendingMetricsByCheckpoint
                .computeIfAbsent(checkpointId, key -> new PendingMetrics())
                .merge(pendingToSeal);
    }

    public void commitPendingMetrics(long checkpointId) {
        if (!PluginType.SINK.equals(type)) {
            return;
        }
        PendingMetrics pending = pendingMetricsByCheckpoint.remove(checkpointId);
        if (pending == null || pending.isEmpty()) {
            return;
        }
        committedCount.inc(pending.getCount());
        committedQPS.markEvent(pending.getCount());
        committedBytes.inc(pending.getBytes());
        committedBytesPerSeconds.markEvent(pending.getBytes());
        pending.getTableMetrics()
                .forEach(
                        (table, metrics) -> {
                            processMetrics(
                                    committedCountPerTable,
                                    Counter.class,
                                    table,
                                    SINK_COMMITTED_COUNT,
                                    SOURCE_RECEIVED_COUNT,
                                    counter -> counter.inc(metrics.count));
                            processMetrics(
                                    committedBytesPerTable,
                                    Counter.class,
                                    table,
                                    SINK_COMMITTED_BYTES,
                                    SOURCE_RECEIVED_BYTES,
                                    counter -> counter.inc(metrics.bytes));
                            processMetrics(
                                    committedQPSPerTable,
                                    Meter.class,
                                    table,
                                    SINK_COMMITTED_QPS,
                                    SOURCE_RECEIVED_QPS,
                                    meter -> meter.markEvent(metrics.count));
                            processMetrics(
                                    committedBytesPerSecondsPerTable,
                                    Meter.class,
                                    table,
                                    SINK_COMMITTED_BYTES_PER_SECONDS,
                                    SOURCE_RECEIVED_BYTES_PER_SECONDS,
                                    meter -> meter.markEvent(metrics.bytes));
                        });
    }

    public void abortPendingMetrics(long checkpointId) {
        if (!PluginType.SINK.equals(type)) {
            return;
        }
        pendingMetricsByCheckpoint.remove(checkpointId);
    }

    private void recordPendingMetrics(String normalizedTableName, long rowBytes) {
        if (currentPendingMetrics == null) {
            return;
        }
        currentPendingMetrics.add(normalizedTableName, rowBytes);
    }

    private String normalizeTableName(String tableId) {
        return tableNameCache.computeIfAbsent(tableId, id -> TablePath.of(id).getFullName());
    }

    private <T> void processMetrics(
            Map<String, T> metricMap,
            Class<T> cls,
            String tableName,
            String sinkMetric,
            String sourceMetric,
            MetricProcessor<T> processor) {
        T metric = metricMap.get(tableName);
        if (Objects.nonNull(metric)) {
            processor.process(metric);
        } else {
            String metricName =
                    PluginType.SINK.equals(type)
                            ? sinkMetric + "#" + tableName
                            : sourceMetric + "#" + tableName;
            T newMetric = createMetric(metricsContext, metricName, cls);
            processor.process(newMetric);
            metricMap.put(tableName, newMetric);
        }
    }

    private <T> T createMetric(
            MetricsContext metricsContext, String metricName, Class<T> metricClass) {
        if (metricClass == Counter.class) {
            return metricClass.cast(metricsContext.counter(metricName));
        } else if (metricClass == Meter.class) {
            return metricClass.cast(metricsContext.meter(metricName));
        }
        throw new IllegalArgumentException("Unsupported metric class: " + metricClass.getName());
    }

    @FunctionalInterface
    interface MetricProcessor<T> {
        void process(T t);
    }

    private static final class PendingMetrics {
        private long count;
        private long bytes;
        private final Map<String, TablePendingMetrics> tableMetrics = new ConcurrentHashMap<>();

        void add(String tableName, long rowBytes) {
            count++;
            bytes += rowBytes;
            if (StringUtils.isNotBlank(tableName)) {
                tableMetrics
                        .computeIfAbsent(tableName, key -> new TablePendingMetrics())
                        .add(rowBytes);
            }
        }

        boolean isEmpty() {
            return count == 0;
        }

        void merge(PendingMetrics other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            this.count += other.count;
            this.bytes += other.bytes;
            other.tableMetrics.forEach(
                    (table, metrics) ->
                            this.tableMetrics
                                    .computeIfAbsent(table, key -> new TablePendingMetrics())
                                    .merge(metrics));
        }

        long getCount() {
            return count;
        }

        long getBytes() {
            return bytes;
        }

        Map<String, TablePendingMetrics> getTableMetrics() {
            return tableMetrics;
        }
    }

    private static final class TablePendingMetrics {
        private long count;
        private long bytes;

        void add(long rowBytes) {
            this.count++;
            this.bytes += rowBytes;
        }

        void merge(TablePendingMetrics other) {
            if (other == null) {
                return;
            }
            this.count += other.count;
            this.bytes += other.bytes;
        }
    }
}
