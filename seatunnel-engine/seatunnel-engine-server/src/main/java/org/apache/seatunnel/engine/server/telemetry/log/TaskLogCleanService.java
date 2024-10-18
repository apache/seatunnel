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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobHistoryService;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TaskLogCleanService {

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running;
    private final long keepTime;
    private final String prefix;
    private final String path;

    private final NodeEngine nodeEngine;

    public TaskLogCleanService(
            String cron, long keepTime, String prefix, String path, NodeEngine nodeEngine) {
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "job-log-clean-thread");
                            thread.setDaemon(true);
                            return thread;
                        });
        this.running = new AtomicBoolean(false);
        this.keepTime = keepTime;
        this.prefix = prefix;
        this.path = path;
        this.nodeEngine = nodeEngine;
        log.info(
                "TaskLogCleanService init with cron: {}, keepTime: {}, prefix: {}, path: {}",
                cron,
                keepTime,
                prefix,
                path);
        scheduleTask(cron, new TaskLogCleanThread());
    }

    public void scheduleTask(String cronExpression, Runnable task) {
        CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
        CronParser parser = new CronParser(cronDefinition);
        Cron cron = parser.parse(cronExpression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);

        if (!running.get()) {
            running.set(true);

            long initialDelay = calculateInitialDelay(executionTime);
            log.info("Initial delay: {} ms", initialDelay);

            scheduler.schedule(
                    () -> executeTaskAtNextExecution(executionTime, task),
                    initialDelay,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void executeTaskAtNextExecution(ExecutionTime executionTime, Runnable task) {
        try {
            try {
                task.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                running.set(false);
            }

            long nextDelay = calculateNextDelay(executionTime);
            log.info("Next execution in: {} ms", nextDelay);

            scheduler.schedule(
                    () -> executeTaskAtNextExecution(executionTime, task),
                    nextDelay,
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error executing task", e);
        }
    }

    private long calculateInitialDelay(ExecutionTime executionTime) {
        ZonedDateTime now = ZonedDateTime.now();
        return executionTime
                .nextExecution(now)
                .map(next -> next.toInstant().toEpochMilli() - System.currentTimeMillis())
                .orElseThrow(() -> new RuntimeException("Failed to get next execution time"));
    }

    private long calculateNextDelay(ExecutionTime executionTime) {
        ZonedDateTime now = ZonedDateTime.now();
        return executionTime
                .nextExecution(now)
                .map(next -> next.toInstant().toEpochMilli() - System.currentTimeMillis())
                .orElseThrow(() -> new RuntimeException("Failed to get next execution time"));
    }

    public void shutdown() {
        running.set(false);
        scheduler.shutdown();
    }

    class TaskLogCleanThread implements Runnable {

        @Override
        public void run() {

            File logDir = new File(path);
            if (!logDir.exists() || !logDir.isDirectory()) {
                log.error("Invalid job log directory: {}", path);
                return;
            }

            // Get current date
            long currentTimeMillis = System.currentTimeMillis();
            long keepMillis = TimeUnit.MILLISECONDS.toMillis(keepTime);

            File[] logFiles =
                    logDir.listFiles(
                            (dir, name) ->
                                    name.startsWith(prefix + "-") || name.startsWith(prefix));
            if (logFiles != null) {
                for (File logFile : logFiles) {
                    // check status
                    boolean isTaskRunning = checkTaskStatus(logFile.getName());
                    if (isTaskRunning) {
                        log.info(
                                "Task is still not end for log file: {}, skipping deletion.",
                                logFile.getName());
                        continue;
                    }

                    // Determine the last modification time of the fileDetermine the last
                    // modification time of the file
                    long lastModified = logFile.lastModified();
                    // Check whether the file retention period has expired
                    if (currentTimeMillis - lastModified > keepMillis) {
                        if (logFile.delete()) {
                            log.info("Deleted log file: {}", logFile.getName());
                        } else {
                            log.error("Failed to delete log file: {}", logFile.getName());
                        }
                    }
                }
            }
            log.info("Log clearing completed!!!");
        }

        private boolean checkTaskStatus(String logFileName) {
            Pattern pattern = Pattern.compile("\\b(\\d{18})\\b");
            Matcher matcher = pattern.matcher(logFileName);
            boolean isRuning = true;

            if (matcher.find()) {
                JobStatus jobStateWithRunMap = null;
                JobStatus jobStateWithFinishedMap = null;
                long jobId = Long.parseLong(matcher.group(1));
                IMap<Object, Object> finishedMap =
                        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);
                IMap<Object, Object> runMap =
                        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);

                Object status = finishedMap.get(jobId);
                if (status != null) {
                    jobStateWithFinishedMap = ((JobHistoryService.JobState) status).getJobStatus();
                }
                status = runMap.get(jobId);
                if (status != null) {
                    jobStateWithRunMap = (JobStatus) status;
                }
                log.info(
                        "jobId {} jobStateWithRunMap {} jobStateWithFinishedMap {}",
                        jobId,
                        jobStateWithRunMap,
                        jobStateWithFinishedMap);
                isRuning =
                        (jobStateWithRunMap != null)
                                ? !jobStateWithRunMap.isEndState()
                                : (jobStateWithFinishedMap != null
                                        && !jobStateWithFinishedMap.isEndState());
                log.info("Job {} is running: {}", jobId, isRuning);
            }
            return isRuning;
        }
    }

    public static void main(String[] args) {
        System.out.println();
    }
}
