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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.state.CheckpointLock;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkCheckpointLock implements CheckpointLock, AutoCloseable {

    private static final Integer LOOP_INTERVAL = 10;
    private final Object flinkLock;
    private final AtomicInteger lock = new AtomicInteger(0);
    private volatile boolean engineLock = false;
    private volatile boolean running = true;
    private final ScheduledThreadPoolExecutor executor;
    public FlinkCheckpointLock(Object flinkLock, int subtaskId) {
        this.flinkLock = flinkLock;
        this.executor = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, String.format("checkpoint-lock-monitor-%s", subtaskId));
        executor.execute(this::monitorLock);
    }

    private void monitorLock() {
        if (!running) {
            return;
        }
        while (lock.get() == 0 && running) {
            sleep();
        }
        if (!running) {
            return;
        }
        synchronized (flinkLock) {
            engineLock = true;
            while (lock.get() != 0 && running) {
                sleep();
            }
        }
        engineLock = false;
        monitorLock();
    }

    @Override
    public void lock() {
        lock.incrementAndGet();
        while (!engineLock) {
            sleep();
        }
    }

    @Override
    public void unlock() {
        int num = lock.decrementAndGet();
        while (engineLock && num == 0) {
            sleep();
        }
    }

    private void sleep() {
        try {
            Thread.sleep(LOOP_INTERVAL);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        executor.shutdown();
    }
}
