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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlGate;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;

import org.apache.flink.api.connector.source.ReaderOutput;

import lombok.extern.slf4j.Slf4j;

/** The implementation of {@link Collector} for flink engine. */
@Slf4j
public class FlinkRowCollector implements Collector<SeaTunnelRow> {

    private ReaderOutput<SeaTunnelRow> readerOutput;

    private final FlowControlGate flowControlGate;

    private final Counter sourceReadCount;

    private final Counter sourceReadBytes;

    private final Meter sourceReadQPS;

    private boolean emptyThisPollNext = true;

    public FlinkRowCollector(Config envConfig, MetricsContext metricsContext) {
        this.flowControlGate = FlowControlGate.create(FlowControlStrategy.fromConfig(envConfig));
        this.sourceReadCount = metricsContext.counter(MetricNames.SOURCE_RECEIVED_COUNT);
        this.sourceReadBytes = metricsContext.counter(MetricNames.SOURCE_RECEIVED_BYTES);
        this.sourceReadQPS = metricsContext.meter(MetricNames.SOURCE_RECEIVED_QPS);
    }

    @Override
    public void collect(SeaTunnelRow record) {
        flowControlGate.audit(record);
        try {
            readerOutput.collect(record);
            sourceReadCount.inc();
            sourceReadBytes.inc(record.getBytesSize());
            sourceReadQPS.markEvent();
            emptyThisPollNext = false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCheckpointLock() {
        return this;
    }

    @Override
    public boolean isEmptyThisPollNext() {
        return emptyThisPollNext;
    }

    @Override
    public void resetEmptyThisPollNext() {
        this.emptyThisPollNext = true;
    }

    public FlinkRowCollector withReaderOutput(ReaderOutput<SeaTunnelRow> readerOutput) {
        this.readerOutput = readerOutput;
        this.emptyThisPollNext = true;
        return this;
    }
}
