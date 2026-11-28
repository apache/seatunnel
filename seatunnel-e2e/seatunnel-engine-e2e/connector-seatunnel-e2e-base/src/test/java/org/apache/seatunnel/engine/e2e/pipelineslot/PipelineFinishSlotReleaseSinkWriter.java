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

package org.apache.seatunnel.engine.e2e.pipelineslot;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer of {@link PipelineFinishSlotReleaseSink}.
 *
 * <p>It has no side effect other than the per-tag row counter and the optional sleep, so a stalled
 * writer holds exactly one pipeline of the job, never the whole cluster. On interruption (the
 * engine cancels a task) the sleep is abandoned and the interrupt flag is restored so cancellation
 * stays cooperative.
 */
@Slf4j
public class PipelineFinishSlotReleaseSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String tag;

    private final long sleepMs;

    private final String holdGate;

    private final AtomicLong counter;

    private boolean gateReleased;

    public PipelineFinishSlotReleaseSinkWriter(String tag, long sleepMs, String holdGate) {
        this.tag = tag;
        this.sleepMs = sleepMs;
        this.holdGate = holdGate;
        this.counter = PipelineFinishSlotReleaseSink.counterFor(tag);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (holdGate != null && !gateReleased) {
            log.warn("Writer {} parks before its first row on gate {}", tag, holdGate);
            PipelineFinishSlotReleaseGate.await(holdGate);
            gateReleased = true;
            log.warn("Writer {} left gate {}", tag, holdGate);
        }
        counter.incrementAndGet();
        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void close() {
        // Nothing to flush: the sink keeps no state and no external resource.
    }
}
