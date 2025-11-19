/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.Unit;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.HashMap;
import java.util.Map;

public class FlinkAccumulatorCounter implements Counter {

    private static final Map<String, String> METRIC_NAME_MAPPINGS = new HashMap<>();

    static {
        // Initialize standard metric name mappings
        METRIC_NAME_MAPPINGS.put("SinkWriteCount", MetricNames.SINK_WRITE_COUNT);
        METRIC_NAME_MAPPINGS.put("SinkWriteBytes", MetricNames.SINK_WRITE_BYTES);
        METRIC_NAME_MAPPINGS.put("SourceReceivedCount", MetricNames.SOURCE_RECEIVED_COUNT);
        METRIC_NAME_MAPPINGS.put("SourceReceivedBytes", MetricNames.SOURCE_RECEIVED_BYTES);
    }

    private final String name;
    private final org.apache.flink.metrics.Counter flinkCounter;
    private final LongCounter accumulator;
    private final RuntimeContext runtimeContext;

    public FlinkAccumulatorCounter(
            String name,
            org.apache.flink.metrics.Counter flinkCounter,
            RuntimeContext runtimeContext) {
        this.name = name;
        this.flinkCounter = flinkCounter;
        this.runtimeContext = runtimeContext;
        this.accumulator = new LongCounter();

        String accumulatorName = getStandardAccumulatorName(name);
        runtimeContext.addAccumulator(accumulatorName, accumulator);
    }

    @Override
    public void inc() {
        inc(1L);
    }

    @Override
    public void inc(long n) {
        if (flinkCounter != null) {
            flinkCounter.inc(n);
        }
        accumulator.add(n);
    }

    @Override
    public void dec() {
        dec(1L);
    }

    @Override
    public void dec(long n) {
        if (flinkCounter != null) {
            flinkCounter.inc(-n);
        }
        accumulator.add(-n);
    }

    @Override
    public void set(long n) {
        long current = accumulator.getLocalValue();
        long diff = n - current;
        if (flinkCounter != null) {
            flinkCounter.inc(diff);
        }
        accumulator.add(diff);
    }

    @Override
    public long getCount() {
        return accumulator.getLocalValue();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Unit unit() {
        return Unit.COUNT;
    }

    public LongCounter getAccumulator() {
        return accumulator;
    }

    private String getStandardAccumulatorName(String originalName) {
        if (METRIC_NAME_MAPPINGS.containsValue(originalName)) {
            return originalName;
        }

        for (Map.Entry<String, String> entry : METRIC_NAME_MAPPINGS.entrySet()) {
            if (originalName.contains(entry.getKey())) {
                return entry.getValue();
            }
        }

        return originalName;
    }
}
