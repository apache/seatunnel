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

package org.apache.seatunnel.engine.server.execution;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Admission policy for promoted cooperative workers.
 *
 * <p>A cooperative worker thread is shared by many tasks. When one task call runs longer than the
 * call timer allows, the worker is promoted: it becomes exclusive to that slow task and a new
 * worker is started to keep serving the shared queue. Every promotion therefore adds one thread,
 * and without a budget the worker thread count grows with the number of slow cooperative calls
 * rather than with the number of slots.
 *
 * <p>This class keeps that growth explicit. A promotion must first acquire budget, both globally
 * and for the job that owns the task, and must release it when the promoted worker finishes. A
 * denied promotion is counted so the decision stays observable; the caller is expected to retry it
 * later rather than to drop the task.
 *
 * <p>A limit that is not positive means "unlimited", which is the shipped default and keeps the
 * historical behaviour unchanged.
 */
public class CooperativeWorkerBudget {

    /** Marker for an unlimited budget. */
    public static final int UNLIMITED = 0;

    private final int maxPromotedWorkers;

    private final int maxPromotedWorkersPerJob;

    private final AtomicInteger promotedWorkers = new AtomicInteger();

    private final ConcurrentMap<Long, Integer> promotedWorkersPerJob = new ConcurrentHashMap<>();

    private final AtomicLong totalPromotions = new AtomicLong();

    private final AtomicLong deniedPromotions = new AtomicLong();

    public CooperativeWorkerBudget(int maxPromotedWorkers, int maxPromotedWorkersPerJob) {
        this.maxPromotedWorkers = Math.max(maxPromotedWorkers, UNLIMITED);
        this.maxPromotedWorkersPerJob = Math.max(maxPromotedWorkersPerJob, UNLIMITED);
    }

    /**
     * Tries to reserve budget for one promoted worker of the given job.
     *
     * @param jobId the job that owns the task the worker would become exclusive to
     * @return true when the promotion is admitted, false when a limit is already reached
     */
    public boolean tryAcquire(long jobId) {
        if (!tryAcquireGlobal()) {
            deniedPromotions.incrementAndGet();
            return false;
        }
        if (!tryAcquireJob(jobId)) {
            promotedWorkers.decrementAndGet();
            deniedPromotions.incrementAndGet();
            return false;
        }
        totalPromotions.incrementAndGet();
        return true;
    }

    /**
     * Returns the budget held by one promoted worker of the given job. Must be called exactly once
     * for every {@link #tryAcquire(long)} that returned true.
     *
     * @param jobId the job the promotion was acquired for
     */
    public void release(long jobId) {
        promotedWorkers.updateAndGet(current -> current > 0 ? current - 1 : 0);
        promotedWorkersPerJob.computeIfPresent(jobId, (id, count) -> count <= 1 ? null : count - 1);
    }

    public int getMaxPromotedWorkers() {
        return maxPromotedWorkers;
    }

    public int getMaxPromotedWorkersPerJob() {
        return maxPromotedWorkersPerJob;
    }

    public int getPromotedWorkers() {
        return promotedWorkers.get();
    }

    public int getPromotedWorkers(long jobId) {
        return promotedWorkersPerJob.getOrDefault(jobId, 0);
    }

    public long getTotalPromotions() {
        return totalPromotions.get();
    }

    public long getDeniedPromotions() {
        return deniedPromotions.get();
    }

    private boolean tryAcquireGlobal() {
        if (maxPromotedWorkers == UNLIMITED) {
            promotedWorkers.incrementAndGet();
            return true;
        }
        while (true) {
            int current = promotedWorkers.get();
            if (current >= maxPromotedWorkers) {
                return false;
            }
            if (promotedWorkers.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private boolean tryAcquireJob(long jobId) {
        AtomicBoolean acquired = new AtomicBoolean();
        promotedWorkersPerJob.compute(
                jobId,
                (id, count) -> {
                    int current = count == null ? 0 : count;
                    if (maxPromotedWorkersPerJob != UNLIMITED
                            && current >= maxPromotedWorkersPerJob) {
                        return count;
                    }
                    acquired.set(true);
                    return current + 1;
                });
        return acquired.get();
    }
}
