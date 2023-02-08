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

/**
 * TaskCallTimer is a time-consuming timer for Task Call method execution
 */
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

    public TaskCallTimer(
        long delay,
        AtomicBoolean keep,
        TaskExecutionService.RunBusWorkSupplier runBusWorkSupplier,
        TaskExecutionService.CooperativeTaskWorker cooperativeTaskWorker) {
        this.delay = delay;
        this.keep = keep;
        this.runBusWorkSupplier = runBusWorkSupplier;
        this.cooperativeTaskWorker = cooperativeTaskWorker;
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
                        timeoutAct(this.taskTracker.expiredTimes.incrementAndGet());
                        break;
                    }
                }
                if (wait) {
                    synchronized (lock) {
                        lock.wait();
                    }
                } else {
                    synchronized (lock) {
                        lock.wait(executionTime - currentTime);
                    }
                }
            } catch (InterruptedException e) {
                log.warn("TaskCallTimer thread interrupted", e);
            }
        }
    }

    /**
     * The action to be performed when the task call method execution times out
     */
    private void timeoutAct(int expiredTimes) {
        if (expiredTimes >= 1) {
            // 1 busWork keep on running
            keep.set(true);
            // 2 busWork exclusive to the current taskTracker
            cooperativeTaskWorker.exclusiveTaskTracker.set(taskTracker);
            // 3 Submit a new BusWork to execute other tasks
            runBusWorkSupplier.runNewBusWork(false);
        } else {
            // 1 Stop the current busWork from continuing to execute the new Task
            keep.set(false);
            // 2 Submit a new BusWork to execute other tasks
            runBusWorkSupplier.runNewBusWork(false);
        }
    }
}
