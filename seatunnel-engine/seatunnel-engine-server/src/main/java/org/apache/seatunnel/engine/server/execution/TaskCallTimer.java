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

import org.apache.seatunnel.engine.server.TaskExecutionService;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/** TaskCallTimer is a time-consuming timer for Task Call method execution */
@Slf4j
public class TaskCallTimer extends Thread {

    long nextExecutionTime;
    long delay;

    TaskExecutionService.CooperativeTaskWorker cooperativeTaskWorker;
    AtomicBoolean keep;
    TaskExecutionService.RunBusWorkSupplier runBusWorkSupplier;

    TaskTracker taskTracker;

    private final Object lock = new Object();
    boolean started = false;
    AtomicBoolean wait0 = new AtomicBoolean(false);

    /** Upper bound of the backoff used while a denied promotion is retried. */
    private static final long MAX_PROMOTION_RETRY_DELAY_MS = 1000;

    private long promotionRetryDelay;

    public TaskCallTimer(
            long delay,
            AtomicBoolean keep,
            TaskExecutionService.RunBusWorkSupplier runBusWorkSupplier,
            TaskExecutionService.CooperativeTaskWorker cooperativeTaskWorker) {
        this.delay = delay;
        this.keep = keep;
        this.runBusWorkSupplier = runBusWorkSupplier;
        this.cooperativeTaskWorker = cooperativeTaskWorker;
        this.promotionRetryDelay = delay;
    }

    private void startTimer() {
        nextExecutionTime = System.currentTimeMillis() + delay;
        this.start();
    }

    public void reSet(long tmpDelay) {
        nextExecutionTime = System.currentTimeMillis() + tmpDelay;
        if (started) {
            synchronized (lock) {
                lock.notifyAll();
            }
        } else {
            started = true;
            this.start();
        }
    }

    public void reSet() {
        nextExecutionTime = System.currentTimeMillis() + delay;
        if (!started) {
            started = true;
            this.start();
        }
    }

    public void timerStart(TaskTracker taskTracker) {
        wait0.set(false);
        this.taskTracker = taskTracker;
        this.promotionRetryDelay = delay;
        nextExecutionTime = System.currentTimeMillis() + delay;
        if (started) {
            synchronized (lock) {
                lock.notifyAll();
            }
        } else {
            started = true;
            this.start();
        }
    }

    public void timerStop() {
        // Wait until the next time the timer is enabled to wake up
        wait0.set(true);
    }

    @Override
    public void run() {
        while (true) {
            long currentTime;
            long executionTime;
            boolean wait;
            try {
                synchronized (this) {
                    wait = wait0.get();
                    currentTime = System.currentTimeMillis();
                    executionTime = this.nextExecutionTime;
                    if (!wait && executionTime <= currentTime) {
                        if (timeoutAct(this.taskTracker.expiredTimes.incrementAndGet())) {
                            break;
                        }
                        // The promotion was denied by the cooperative worker budget. Keep the
                        // task where it is and retry the promotion after a bounded backoff
                        // instead of dropping the timeout.
                        promotionRetryDelay =
                                Math.min(promotionRetryDelay * 2, MAX_PROMOTION_RETRY_DELAY_MS);
                        nextExecutionTime = System.currentTimeMillis() + promotionRetryDelay;
                        executionTime = nextExecutionTime;
                        currentTime = System.currentTimeMillis();
                    }
                }
                if (wait) {
                    synchronized (lock) {
                        lock.wait();
                    }
                } else {
                    synchronized (lock) {
                        lock.wait(Math.max(1, executionTime - currentTime));
                    }
                }
            } catch (InterruptedException e) {
                log.warn("TaskCallTimer thread interrupted", e);
            }
        }
    }

    /**
     * The action to be performed when the task call method execution times out.
     *
     * @param expiredTimes how often this task call has already expired
     * @return true when the timer is done with this task call, false when the promotion was denied
     *     by the cooperative worker budget and has to be retried
     */
    private boolean timeoutAct(int expiredTimes) {
        if (expiredTimes >= 1) {
            // busWork keeps running the current taskTracker exclusively and a new BusWork is
            // submitted for the other tasks, but only if the promotion fits the worker budget
            return runBusWorkSupplier.tryPromoteCooperativeWorker(
                    cooperativeTaskWorker, taskTracker);
        }
        // 1 Stop the current busWork from continuing to execute the new Task
        keep.set(false);
        // 2 Submit a new BusWork to execute other tasks
        runBusWorkSupplier.runNewBusWork(false);
        return true;
    }
}
