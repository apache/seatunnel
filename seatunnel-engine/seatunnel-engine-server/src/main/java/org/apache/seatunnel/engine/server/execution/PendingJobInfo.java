/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.engine.server.diagnostic.PendingJobDiagnostic;
import org.apache.seatunnel.engine.server.master.JobMaster;

import java.util.concurrent.atomic.AtomicInteger;

public class PendingJobInfo {
    private final PendingSourceState pendingSourceState;
    private final JobMaster jobMaster;
    private final long enqueueTimestamp;
    private final AtomicInteger checkTimes = new AtomicInteger();
    private volatile long lastCheckTime;
    private volatile PendingJobDiagnostic lastSnapshot;

    public PendingJobInfo(PendingSourceState pendingSourceState, JobMaster jobMaster) {
        this.pendingSourceState = pendingSourceState;
        this.jobMaster = jobMaster;
        this.enqueueTimestamp = System.currentTimeMillis();
        this.lastCheckTime = enqueueTimestamp;
    }

    public PendingSourceState getPendingSourceState() {
        return pendingSourceState;
    }

    public JobMaster getJobMaster() {
        return jobMaster;
    }

    public Long getJobId() {
        return jobMaster.getJobId();
    }

    public long getEnqueueTimestamp() {
        return enqueueTimestamp;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public int getCheckTimes() {
        return checkTimes.get();
    }

    public PendingJobDiagnostic getLastSnapshot() {
        return lastSnapshot;
    }

    public void recordSnapshot(PendingJobDiagnostic snapshot) {
        if (snapshot == null) {
            return;
        }
        this.lastSnapshot = snapshot;
        this.lastCheckTime = snapshot.getCheckTime();
        int current = this.checkTimes.incrementAndGet();
        snapshot.setCheckCount(current);
    }
}
