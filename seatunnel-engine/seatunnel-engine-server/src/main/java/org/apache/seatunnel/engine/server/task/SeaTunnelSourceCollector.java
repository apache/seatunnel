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

package org.apache.seatunnel.engine.server.task;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;

import org.apache.seatunnel.api.common.metrics.Unit;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.server.metrics.Metrics;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;

import java.io.IOException;
import java.util.List;

public class SeaTunnelSourceCollector<T> implements Collector<T> {

    private final Object checkpointLock;

    private final List<OneInputFlowLifeCycle<Record<?>>> outputs;

    public SeaTunnelSourceCollector(Object checkpointLock, List<OneInputFlowLifeCycle<Record<?>>> outputs) {
        this.checkpointLock = checkpointLock;
        this.outputs = outputs;
    }

    @Override
    public void collect(T row) {
        try {
            sendRecordToNext(new Record<>(row));
            Metrics.qpsMetric(SOURCE_RECEIVED_QPS, Unit.COUNT).increment();
            Metrics.metric(SOURCE_RECEIVED_COUNT, Unit.COUNT).increment();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCheckpointLock() {
        return checkpointLock;
    }

    public void sendRecordToNext(Record<?> record) throws IOException {
        synchronized (checkpointLock) {
            for (OneInputFlowLifeCycle<Record<?>> output : outputs) {
                output.received(record);
            }
        }
    }
}
