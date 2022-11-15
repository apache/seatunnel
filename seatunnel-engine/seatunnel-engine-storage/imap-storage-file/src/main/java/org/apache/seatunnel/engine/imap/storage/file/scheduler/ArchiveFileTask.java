/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.scheduler;

import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ArchiveFileTask {

    /**
     * archive file task scheduler time
     */
    private static final int DEFAULT_SCHEDULE_TIME = 60;

    private static final int DEFAULT_SCHEDULER_START_DELAY_TIME = 60;

    private static volatile ScheduledExecutorService SCHEDULER_EXECUTOR_SERVICE = null;

    private static ConcurrentHashMap<WALDisruptor, SchedulerTaskInfo> TASK_MAP = new ConcurrentHashMap<>();

    public static void addTask(WALDisruptor disruptor, Long scheduleTime) {
        start();
        SchedulerTaskInfo taskInfo = SchedulerTaskInfo.builder()
            .scheduledTime(scheduleTime)
            .latestTime(System.currentTimeMillis()).build();
        TASK_MAP.putIfAbsent(disruptor, taskInfo);
    }

    public static void removeTask(WALDisruptor disruptor) {
        TASK_MAP.remove(disruptor);
    }

    private static void start() {
        if (SCHEDULER_EXECUTOR_SERVICE == null) {
            synchronized (ArchiveFileTask.class) {
                if (SCHEDULER_EXECUTOR_SERVICE == null) {
                    SCHEDULER_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
                    SCHEDULER_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> TASK_MAP.forEach((disruptor, taskInfo) -> {
                        if (taskInfo.getLatestTime() + taskInfo.getScheduledTime() < System.nanoTime()) {
                            return;
                        }
                        disruptor.tryPublishSchedulerArchive();
                        TASK_MAP.get(disruptor).setLatestTime(System.currentTimeMillis());
                    }), DEFAULT_SCHEDULER_START_DELAY_TIME, DEFAULT_SCHEDULE_TIME, TimeUnit.SECONDS);
                }
            }
        }
    }

}
