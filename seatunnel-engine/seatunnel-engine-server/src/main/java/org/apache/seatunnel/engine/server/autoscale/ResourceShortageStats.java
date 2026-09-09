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
 * Records scheduler resource-shortage events for autoscaler evaluation.
 *
 * <p>It keeps cumulative WAIT/REJECT counts and uses a sequence number to report only events since
 * the previous snapshot. It stores aggregate counters and the latest event details instead of the
 * full event history, and synchronizes all access to keep snapshots consistent.
 */
public final class ResourceShortageStats {

    /** Monotonically increasing cursor for recorded resource-shortage events. */
    private long sequence;

    private long waitCount;
    private long rejectCount;
    private long latestWaitSequence;
    private long latestRejectSequence;
    private int latestTaskGroupCount;
    private String latestRequestedResourceProfile;

    public synchronized void recordWaitShortage(
            int taskGroupCount, String requestedResourceProfile) {
        long currentSequence = ++sequence;
        waitCount++;
        latestWaitSequence = currentSequence;
        latestTaskGroupCount = taskGroupCount;
        latestRequestedResourceProfile = requestedResourceProfile;
    }

    public synchronized void recordRejectShortage(
            int taskGroupCount, String requestedResourceProfile) {
        long currentSequence = ++sequence;
        rejectCount++;
        latestRejectSequence = currentSequence;
        latestTaskGroupCount = taskGroupCount;
        latestRequestedResourceProfile = requestedResourceProfile;
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
                latestRequestedResourceProfile);
    }
}
