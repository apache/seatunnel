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

import lombok.NonNull;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class FixedCallTestTimeTask implements Task {
    long callTime;
    String name;
    long currentTime;
    CopyOnWriteArrayList<Long> lagList;
    AtomicBoolean stop;

    public FixedCallTestTimeTask(long callTime, String name, AtomicBoolean stop, CopyOnWriteArrayList<Long> lagList) {
        this.callTime = callTime;
        this.name = name;
        this.stop = stop;
        this.lagList = lagList;
    }

    @NonNull
    @Override
    public ProgressState call() {
        if (currentTime != 0) {
            lagList.add(System.currentTimeMillis() - currentTime);
        }
        currentTime = System.currentTimeMillis();

        try {
            Thread.sleep(callTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.toString());
        }
        if (stop.get()) {
            return ProgressState.DONE;
        }
        return ProgressState.MADE_PROGRESS;
    }

    @NonNull
    @Override
    public Long getTaskID() {
        return (long) this.hashCode();
    }

    @Override
    public boolean isThreadsShare() {
        return true;
    }
}
