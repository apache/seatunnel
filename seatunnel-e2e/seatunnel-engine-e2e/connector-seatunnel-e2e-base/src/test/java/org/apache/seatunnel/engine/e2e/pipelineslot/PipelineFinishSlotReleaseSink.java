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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only sink with no committer and no writer state.
 *
 * <p>Every writer increments the counter of its {@code tag}, which lets the test observe which
 * pipeline of a job is still producing rows. A writer can be kept busy with a per-row sleep and can
 * be parked on a {@link PipelineFinishSlotReleaseGate} before its first row, so the test controls
 * both "this pipeline finishes now" and "this pipeline keeps running" without touching engine code.
 */
public class PipelineFinishSlotReleaseSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private static final ConcurrentMap<String, AtomicLong> ROWS = new ConcurrentHashMap<>();

    private final String tag;

    private final long sleepMs;

    private final String holdGate;

    public PipelineFinishSlotReleaseSink(String tag, long sleepMs, String holdGate) {
        this.tag = tag;
        this.sleepMs = sleepMs;
        this.holdGate = holdGate;
    }

    /** Drops every counter so a repeated run starts from zero. */
    public static void resetCounters() {
        ROWS.clear();
    }

    /** Number of rows the writers of {@code tag} have consumed so far. */
    public static long rows(String tag) {
        AtomicLong counter = ROWS.get(tag);
        return counter == null ? 0L : counter.get();
    }

    /** Counter shared by every writer of {@code tag}. */
    static AtomicLong counterFor(String tag) {
        return ROWS.computeIfAbsent(tag, ignored -> new AtomicLong());
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new PipelineFinishSlotReleaseSinkWriter(tag, sleepMs, holdGate);
    }

    @Override
    public String getPluginName() {
        return PipelineFinishSlotReleaseSinkFactory.IDENTIFIER;
    }
}
