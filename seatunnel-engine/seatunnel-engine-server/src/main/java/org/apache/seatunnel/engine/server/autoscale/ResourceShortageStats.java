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

package org.apache.seatunnel.engine.server.autoscale;

/**
 * Compact accumulator for scheduler resource-shortage evidence.
 *
 * <p>It records cumulative WAIT/REJECT counts and exposes sequence deltas without retaining
 * unbounded event history. All mutating and reading methods are synchronized to guarantee a
 * consistent snapshot across fields.
 */
public final class ResourceShortageStats {

    private long sequence;
    private long waitCount;
    private long rejectCount;
    private long latestWaitSequence;
    private long latestRejectSequence;
    private int latestTaskGroupCount;
    private String latestResourceShape;

    public synchronized void recordWaitShortage(int taskGroupCount, String resourceShape) {
        long currentSequence = ++sequence;
        waitCount++;
        latestWaitSequence = currentSequence;
        latestTaskGroupCount = taskGroupCount;
        latestResourceShape = resourceShape;
    }

    public synchronized void recordRejectShortage(int taskGroupCount, String resourceShape) {
        long currentSequence = ++sequence;
        rejectCount++;
        latestRejectSequence = currentSequence;
        latestTaskGroupCount = taskGroupCount;
        latestResourceShape = resourceShape;
    }

    public synchronized ResourceShortageSnapshot snapshot() {
        return snapshotSince(0L);
    }

    public synchronized ResourceShortageSnapshot snapshotSince(long previousSequence) {
        long shortageCount = Math.max(0L, sequence - previousSequence);
        boolean hasNewEvents = shortageCount > 0L;
        return new ResourceShortageSnapshot(
                sequence,
                shortageCount,
                waitCount,
                rejectCount,
                hasNewEvents && latestWaitSequence > previousSequence,
                hasNewEvents && latestRejectSequence > previousSequence,
                latestTaskGroupCount,
                latestResourceShape);
    }
}
