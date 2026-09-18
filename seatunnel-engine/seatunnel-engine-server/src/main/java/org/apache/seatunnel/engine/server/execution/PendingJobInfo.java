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

import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobDiagnostic;
import org.apache.seatunnel.engine.server.master.JobMaster;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PendingJobInfo {
    private final PendingSourceState pendingSourceState;
    private volatile JobMaster jobMaster;
    private final Long jobId;
    private final JobImmutableInformation jobImmutableInformation;
    private Supplier<JobMaster> jobMasterSupplier;
    private volatile boolean interrupted;
    private Throwable initializationFailure;
    private final CompletableFuture<JobResult> completionFuture = new CompletableFuture<>();
    private final long enqueueTimestamp;
    private final AtomicInteger checkTimes = new AtomicInteger();
    private volatile long lastCheckTime;
    private volatile PendingJobDiagnostic lastSnapshot;

    public PendingJobInfo(PendingSourceState pendingSourceState, JobMaster jobMaster) {
        this.pendingSourceState = pendingSourceState;
        this.jobMaster = jobMaster;
        this.jobId = jobMaster.getJobId();
        this.jobImmutableInformation = null;
        this.enqueueTimestamp = System.currentTimeMillis();
        this.lastCheckTime = enqueueTimestamp;
    }

    /** Restores a waiting job without loading plugins or constructing its physical plan. */
    public PendingJobInfo(
            Long jobId,
            JobImmutableInformation jobImmutableInformation,
            Supplier<JobMaster> jobMasterSupplier) {
        this.pendingSourceState = PendingSourceState.RESTORE;
        this.jobId = jobId;
        this.jobImmutableInformation = jobImmutableInformation;
        this.jobMasterSupplier = jobMasterSupplier;
        this.enqueueTimestamp = System.currentTimeMillis();
        this.lastCheckTime = enqueueTimestamp;
    }

    public PendingSourceState getPendingSourceState() {
        return pendingSourceState;
    }

    /**
     * Materializes a restored job at most once. Call from a coordinator executor, since planning
     * may load plugins and perform distributed operations.
     */
    public synchronized JobMaster getJobMaster() {
        if (interrupted) {
            throw new CancellationException(
                    "Pending job " + jobId + " belongs to an inactive coordinator");
        }
        if (initializationFailure != null) {
            throw ExceptionUtil.rethrow(initializationFailure);
        }
        if (jobMaster == null) {
            JobMaster initialized;
            try {
                initialized = jobMasterSupplier.get();
            } catch (Throwable e) {
                initializationFailure = e;
                completionFuture.completeExceptionally(e);
                throw ExceptionUtil.rethrow(e);
            }
            jobMaster = initialized;
            jobMasterSupplier = null;
            if (interrupted) {
                initialized.interrupt();
                throw new CancellationException(
                        "Pending job " + jobId + " was interrupted during restore");
            }
            initialized
                    .getJobMasterCompleteFuture()
                    .whenComplete(
                            (result, error) -> {
                                if (error == null) {
                                    completionFuture.complete(result);
                                } else {
                                    completionFuture.completeExceptionally(error);
                                }
                            });
        }
        return jobMaster;
    }

    /** Returns the materialized master without causing a queued job to be initialized. */
    public JobMaster getInitializedJobMaster() {
        return jobMaster;
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation != null
                ? jobImmutableInformation
                : jobMaster.getJobImmutableInformation();
    }

    public PassiveCompletableFuture<JobResult> getCompletionFuture() {
        return jobImmutableInformation != null
                ? new PassiveCompletableFuture<>(completionFuture)
                : jobMaster.getJobMasterCompleteFuture();
    }

    /** Discards a coordinator generation without materializing its waiting backlog. */
    public void interrupt() {
        interrupted = true;
        completionFuture.completeExceptionally(new InterruptedException());
        JobMaster initialized = jobMaster;
        if (initialized != null) {
            initialized.interrupt();
        }
    }

    public Long getJobId() {
        return jobId;
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
