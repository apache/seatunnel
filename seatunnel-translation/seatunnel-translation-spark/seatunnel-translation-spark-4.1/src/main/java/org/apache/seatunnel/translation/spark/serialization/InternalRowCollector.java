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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlGate;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class InternalRowCollector implements Collector<SeaTunnelRow> {
    protected final Handover<InternalRow> handover;
    protected final Object checkpointLock;
    private final InternalRowConverter rowSerialization;
    protected final AtomicLong collectTotalCount;
    private Map<String, Object> envOptions;
    protected FlowControlGate flowControlGate;
    protected volatile boolean emptyThisPollNext;

    public InternalRowCollector(
            Handover<InternalRow> handover,
            Object checkpointLock,
            InternalRowConverter rowSerialization,
            Map<String, String> envOptionsInfo) {
        this.handover = handover;
        this.checkpointLock = checkpointLock;
        this.rowSerialization = rowSerialization;
        this.collectTotalCount = new AtomicLong(0);
        this.envOptions = (Map) envOptionsInfo;
        this.flowControlGate = FlowControlGate.create(FlowControlStrategy.fromMap(envOptions));
    }

    @Override
    public void collect(SeaTunnelRow record) {
        try {
            synchronized (checkpointLock) {
                flowControlGate.audit(record);
                handover.produce(rowSerialization.convert(record));
            }
            collectTotalCount.incrementAndGet();
            emptyThisPollNext = false;
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

    @Override
    public boolean isEmptyThisPollNext() {
        return emptyThisPollNext;
    }

    @Override
    public void resetEmptyThisPollNext() {
        this.emptyThisPollNext = true;
    }

    public InternalRowConverter getRowSerialization() {
        return rowSerialization;
    }
}
