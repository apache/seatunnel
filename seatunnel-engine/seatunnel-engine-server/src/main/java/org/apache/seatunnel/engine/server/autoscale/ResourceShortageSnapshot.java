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

public final class ResourceShortageSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long sequence;
    private final long shortageCount;
    private final long waitCount;
    private final long rejectCount;
    private final boolean latestWait;
    private final boolean latestReject;
    private final int latestTaskGroupCount;
    private final String latestRequestedResourceProfile;

    public ResourceShortageSnapshot(
            long sequence,
            long shortageCount,
            long waitCount,
            long rejectCount,
            boolean latestWait,
            boolean latestReject,
            int latestTaskGroupCount,
            String latestRequestedResourceProfile) {
        this.sequence = sequence;
        this.shortageCount = shortageCount;
        this.waitCount = waitCount;
        this.rejectCount = rejectCount;
        this.latestWait = latestWait;
        this.latestReject = latestReject;
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

    public boolean isLatestWait() {
        return latestWait;
    }

    public boolean isLatestReject() {
        return latestReject;
    }

    public int getLatestTaskGroupCount() {
        return latestTaskGroupCount;
    }

    public String getLatestRequestedResourceProfile() {
        return latestRequestedResourceProfile;
    }
}
