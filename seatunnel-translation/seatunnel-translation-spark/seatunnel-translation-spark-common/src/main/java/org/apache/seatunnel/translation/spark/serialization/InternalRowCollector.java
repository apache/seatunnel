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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlGate;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy.getFlowControlStrategy;

public class InternalRowCollector implements Collector<SeaTunnelRow> {
    private final Handover<InternalRow> handover;
    private final Object checkpointLock;
    private final InternalRowConverter rowSerialization;
    private final AtomicLong collectTotalCount;
    private Map<String, Object> envOptions;
    private FlowControlGate flowControlGate;

    public InternalRowCollector(
            Handover<InternalRow> handover,
            Object checkpointLock,
            SeaTunnelDataType<?> dataType,
            Map<String, String> envOptionsInfo) {
        this.handover = handover;
        this.checkpointLock = checkpointLock;
        this.rowSerialization = new InternalRowConverter(dataType);
        this.collectTotalCount = new AtomicLong(0);
        this.envOptions = (Map) envOptionsInfo;
        FlowControlStrategy flowControlStrategy = getFlowControlStrategy(envOptions);
        if (flowControlStrategy != null) {
            this.flowControlGate = FlowControlGate.create(flowControlStrategy);
        }
    }

    @Override
    public void collect(SeaTunnelRow record) {
        try {
            synchronized (checkpointLock) {
                if (flowControlGate != null) {
                    flowControlGate.audit(record);
                }
                handover.produce(rowSerialization.convert(record));
            }
            collectTotalCount.incrementAndGet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long collectTotalCount() {
        return collectTotalCount.get();
    }

    @Override
    public Object getCheckpointLock() {
        return this.checkpointLock;
    }
}
