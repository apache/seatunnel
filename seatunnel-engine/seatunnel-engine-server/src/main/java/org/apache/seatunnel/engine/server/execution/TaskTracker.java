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

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTracker {
    public AtomicInteger expiredTimes = new AtomicInteger(0);
    public final NonCompletableFuture taskFuture = new NonCompletableFuture();
    public final Task task;
    public volatile Future<?> taskRuntimeFutures;

    public TaskTracker(Task task, CompletableFuture<Void> cancellationFuture, ILogger logger) {
        this.task = task;

        cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
            if (e == null) {
                e = new IllegalStateException("cancellationFuture should be completed exceptionally");
            }
            taskFuture.internalCompleteExceptionally(e);
            taskRuntimeFutures.cancel(true);
        }));
    }

    @Override
    public String toString() {
        return "Tracking " + task;
    }
}
