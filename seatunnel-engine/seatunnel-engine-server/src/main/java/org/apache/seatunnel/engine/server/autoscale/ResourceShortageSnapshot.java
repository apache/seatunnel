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

import java.io.Serializable;

/** Immutable snapshot of scheduler resource-shortage counters and the latest event details. */
public final class ResourceShortageSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Current monotonically increasing cursor of recorded shortage events. */
    private final long sequence;

    /** Number of shortage events observed since the previous snapshot cursor. */
    private final long shortageCount;

    /** Cumulative number of shortage events handled with the WAIT strategy. */
    private final long waitCount;

    /** Cumulative number of shortage events handled with the REJECT strategy. */
    private final long rejectCount;

    /** Whether a new WAIT shortage event occurred since the previous snapshot cursor. */
    private final boolean hasNewWaitShortage;

    /** Whether a new REJECT shortage event occurred since the previous snapshot cursor. */
    private final boolean hasNewRejectShortage;

    /** Number of task groups associated with the latest shortage event. */
    private final int latestTaskGroupCount;

    /** Requested resource profile associated with the latest shortage event. */
    private final String latestRequestedResourceProfile;

    /**
     * Creates an immutable resource-shortage snapshot.
     *
     * @param sequence current shortage-event cursor
     * @param shortageCount number of shortage events since the previous snapshot cursor
     * @param waitCount cumulative WAIT shortage count
     * @param rejectCount cumulative REJECT shortage count
     * @param hasNewWaitShortage whether a new WAIT shortage occurred since the previous cursor
     * @param hasNewRejectShortage whether a new REJECT shortage occurred since the previous cursor
     * @param latestTaskGroupCount task-group count from the latest shortage event
     * @param latestRequestedResourceProfile requested resource profile from the latest shortage
     *     event
     */
    public ResourceShortageSnapshot(
            long sequence,
            long shortageCount,
            long waitCount,
            long rejectCount,
            boolean hasNewWaitShortage,
            boolean hasNewRejectShortage,
            int latestTaskGroupCount,
            String latestRequestedResourceProfile) {
        this.sequence = sequence;
        this.shortageCount = shortageCount;
        this.waitCount = waitCount;
        this.rejectCount = rejectCount;
        this.hasNewWaitShortage = hasNewWaitShortage;
        this.hasNewRejectShortage = hasNewRejectShortage;
        this.latestTaskGroupCount = latestTaskGroupCount;
        this.latestRequestedResourceProfile = latestRequestedResourceProfile;
    }

    public long getSequence() {
        return sequence;
    }

    public long getShortageCount() {
        return shortageCount;
    }

    public long getWaitCount() {
        return waitCount;
    }

    public long getRejectCount() {
        return rejectCount;
    }

    public boolean hasNewWaitShortage() {
        return hasNewWaitShortage;
    }

    public boolean hasNewRejectShortage() {
        return hasNewRejectShortage;
    }

    public int getLatestTaskGroupCount() {
        return latestTaskGroupCount;
    }

    public String getLatestRequestedResourceProfile() {
        return latestRequestedResourceProfile;
    }
}
