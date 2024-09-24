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
package org.apache.seatunnel.api.common.metrics;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;

public class TaskMetricsCalcContext {

    @Getter private final MetricsContext metricsContext;

    private final PluginType type;

    private Counter count;

    private Map<String, Counter> countPerTable = new ConcurrentHashMap<>();

    private Meter QPS;

    private Map<String, Meter> QPSPerTable = new ConcurrentHashMap<>();

    private Counter bytes;

    private Map<String, Counter> bytesPerTable = new ConcurrentHashMap<>();

    private Meter bytesPerSeconds;

    private Map<String, Meter> bytesPerSecondsPerTable = new ConcurrentHashMap<>();

    public TaskMetricsCalcContext(
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
            this.initializeMetrics(
                    isMulti,
                    tables,
                    SINK_WRITE_COUNT,
                    SINK_WRITE_QPS,
                    SINK_WRITE_BYTES,
                    SINK_WRITE_BYTES_PER_SECONDS);
        } else if (type.equals(PluginType.SOURCE)) {
            this.initializeMetrics(
                    isMulti,
                    tables,
                    SOURCE_RECEIVED_COUNT,
                    SOURCE_RECEIVED_QPS,
                    SOURCE_RECEIVED_BYTES,
                    SOURCE_RECEIVED_BYTES_PER_SECONDS);
        }
    }

    private void initializeMetrics(
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
        if (isMulti && CollectionUtils.isNotEmpty(tables)) {
            tables.forEach(
                    tablePath -> {
                        countPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.counter(countName + "#" + tablePath.getFullName()));
                        QPSPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.meter(qpsName + "#" + tablePath.getFullName()));
                        bytesPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.counter(bytesName + "#" + tablePath.getFullName()));
                        bytesPerSecondsPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.meter(
                                        bytesPerSecondsName + "#" + tablePath.getFullName()));
                    });
        }
    }

    public void collectMetrics(MetricsContext metricsContext) {
        if (metricsContext instanceof AbstractMetricsContext) {
            AbstractMetricsContext context = (AbstractMetricsContext) metricsContext;

            context.getMetrics()
                    .forEach(
                            (k, v) -> {
                                long countValue = 0;
                                String[] keyParts = k.split("#");
                                String tableName = (keyParts.length > 1) ? keyParts[1] : null;
                                String metricName = keyParts[0];

                                switch (type) {
                                    case SINK:
                                        if (v instanceof Counter) {
                                            Counter counter = (Counter) v;
                                            countValue = counter.getCount();

                                            if (metricName.equals(SINK_WRITE_COUNT)) {
                                                if (tableName != null) {
                                                    countPerTable
                                                            .getOrDefault(
                                                                    k,
                                                                    createMetric(k, Counter.class))
                                                            .inc(countValue);
                                                }
                                                if (k.equals(SINK_WRITE_COUNT)) {
                                                    count.inc(countValue);
                                                }
                                            } else if (metricName.equals(SINK_WRITE_BYTES)) {
                                                if (tableName != null) {
                                                    bytesPerTable
                                                            .getOrDefault(
                                                                    k,
                                                                    createMetric(k, Counter.class))
                                                            .inc(countValue);
                                                }
                                                if (k.equals(SINK_WRITE_BYTES)) {
                                                    bytes.inc(countValue);
                                                }
                                            }

                                        } else if (v instanceof Meter) {
                                            Meter meter = (Meter) v;
                                            countValue = meter.getCount();

                                            if (metricName.equals(SINK_WRITE_QPS)) {
                                                if (tableName != null) {
                                                    QPSPerTable.getOrDefault(
                                                                    k, createMetric(k, Meter.class))
                                                            .markEvent(countValue);
                                                }
                                                if (k.equals(SINK_WRITE_QPS)) {
                                                    QPS.markEvent(countValue);
                                                }
                                            } else if (metricName.equals(
                                                    SINK_WRITE_BYTES_PER_SECONDS)) {
                                                if (tableName != null) {
                                                    bytesPerSecondsPerTable
                                                            .getOrDefault(
                                                                    k, createMetric(k, Meter.class))
                                                            .markEvent(countValue);
                                                }
                                                if (k.equals(SINK_WRITE_BYTES_PER_SECONDS)) {
                                                    bytesPerSeconds.markEvent(countValue);
                                                }
                                            }
                                        }
                                        break;

                                    case SOURCE:
                                    default:
                                        throw new IllegalArgumentException(
                                                "Unknown plugin type: " + type);
                                }
                            });
        }
    }

    public void collectMetrics(Object data) {
        count.inc();
        QPS.markEvent();
        if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            bytes.inc(row.getBytesSize());
            bytesPerSeconds.markEvent(row.getBytesSize());
            String tableId = row.getTableId();

            if (StringUtils.isNotBlank(tableId)) {
                String tableName = TablePath.of(tableId).getFullName();

                // Processing count
                processMetrics(
                        countPerTable,
                        Counter.class,
                        tableName,
                        SINK_WRITE_COUNT,
                        SOURCE_RECEIVED_COUNT,
                        Counter::inc);

                // Processing bytes
                processMetrics(
                        bytesPerTable,
                        Counter.class,
                        tableName,
                        SINK_WRITE_BYTES,
                        SOURCE_RECEIVED_BYTES,
                        counter -> counter.inc(row.getBytesSize()));

                // Processing QPS
                processMetrics(
                        QPSPerTable,
                        Meter.class,
                        tableName,
                        SINK_WRITE_QPS,
                        SOURCE_RECEIVED_QPS,
                        Meter::markEvent);

                // Processing bytes rate
                processMetrics(
                        bytesPerSecondsPerTable,
                        Meter.class,
                        tableName,
                        SINK_WRITE_BYTES_PER_SECONDS,
                        SOURCE_RECEIVED_BYTES_PER_SECONDS,
                        meter -> meter.markEvent(row.getBytesSize()));
            }
        }
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
            T newMetric = createMetric(metricName, cls);
            processor.process(newMetric);
            metricMap.put(tableName, newMetric);
        }
    }

    private <T> T createMetric(String metricName, Class<T> metricClass) {
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
}
