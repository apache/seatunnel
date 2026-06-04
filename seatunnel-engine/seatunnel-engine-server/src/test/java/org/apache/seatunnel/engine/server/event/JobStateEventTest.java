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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.job.JobStateEvent;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.engine.server.checkpoint.CheckpointErrorRestoreEndTest.STREAM_CONF_WITH_ERROR_PATH;
import static org.awaitility.Awaitility.await;

class JobStateEventTest extends AbstractSeaTunnelServerTest {

    @Test
    void testJobStateEvent() {

        JobEventProcessor eventProcessor =
                (JobEventProcessor) server.getCoordinatorService().getEventProcessor();

        AtomicInteger accessCounter = new AtomicInteger(0);
        AtomicReference<JobStateEvent> jobStateEventReference = new AtomicReference<>();
        EventHandler eventHandler =
                event -> {
                    if (event.getEventType() != EventType.JOB_STATUS) {
                        return;
                    }
                    JobStateEvent jobStateEvent = (JobStateEvent) event;
                    JobStatus status = jobStateEvent.getJobStatus();
                    switch (status) {
                        case FAILED:
                        case CANCELED:
                        case SAVEPOINT_DONE:
                        case FINISHED:
                            accessCounter.incrementAndGet();
                            jobStateEventReference.lazySet(jobStateEvent);
                            break;
                        default:
                            break;
                    }
                };
        // register the event handler
        List<EventHandler> handlers =
                (List<EventHandler>) ReflectionUtils.getField(eventProcessor, "handlers").get();
        handlers.add(eventHandler);
        long jobIdFinished = System.currentTimeMillis();
        long currentTimeMillis = System.currentTimeMillis();
        startJob(jobIdFinished, "fake_to_console.conf", false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService()
                                                .getJobStatus(jobIdFinished)));
        // check whether the event handler is executed
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(1, accessCounter.get()));
        JobStateEvent jobStateEventFinished = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobIdFinished), jobStateEventFinished.getJobId());
        Assertions.assertEquals(JobStatus.FINISHED, jobStateEventFinished.getJobStatus());
        Assertions.assertTrue(jobStateEventFinished.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobIdFinished), jobStateEventFinished.getJobName());

        long jobIdFailed = System.currentTimeMillis();
        startJob(jobIdFailed, STREAM_CONF_WITH_ERROR_PATH, false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        server.getCoordinatorService().getJobStatus(jobIdFailed)));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(2, accessCounter.get()));
        JobStateEvent jobStateEventFailed = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobIdFailed), jobStateEventFailed.getJobId());
        Assertions.assertEquals(JobStatus.FAILED, jobStateEventFailed.getJobStatus());
        Assertions.assertTrue(jobStateEventFailed.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobIdFailed), jobStateEventFailed.getJobName());
    }

    @Test
    void testNotEndJobStateEvent() {
        server.getCoordinatorService().getEngineConfig().setReportNonTerminalJobState(true);

        JobEventProcessor eventProcessor =
                (JobEventProcessor) server.getCoordinatorService().getEventProcessor();

        AtomicInteger accessCounter = new AtomicInteger(0);
        AtomicReference<JobStateEvent> jobStateEventReference = new AtomicReference<>();
        EventHandler eventHandler =
                event -> {
                    if (event.getEventType() != EventType.JOB_STATUS) {
                        return;
                    }
                    JobStateEvent jobStateEvent = (JobStateEvent) event;
                    JobStatus status = jobStateEvent.getJobStatus();
                    switch (status) {
                        case PENDING:
                        case SCHEDULED:
                        case RUNNING:
                        case DOING_SAVEPOINT:
                        case FAILING:
                        case CANCELING:
                            accessCounter.incrementAndGet();
                            jobStateEventReference.lazySet(jobStateEvent);
                            break;
                        default:
                            break;
                    }
                };
        // register the event handler
        List<EventHandler> handlers =
                (List<EventHandler>) ReflectionUtils.getField(eventProcessor, "handlers").get();
        handlers.add(eventHandler);
        long jobIdFinished = System.currentTimeMillis();
        long currentTimeMillis = System.currentTimeMillis();
        startJob(jobIdFinished, "fake_to_console.conf", false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService()
                                                .getJobStatus(jobIdFinished)));
        // check whether the event handler is executed
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(3, accessCounter.get()));
        JobStateEvent jobStateEventFinished = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobIdFinished), jobStateEventFinished.getJobId());
        Assertions.assertEquals(JobStatus.RUNNING, jobStateEventFinished.getJobStatus());
        Assertions.assertTrue(jobStateEventFinished.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobIdFinished), jobStateEventFinished.getJobName());

        long jobIdFailed = System.currentTimeMillis();
        startJob(jobIdFailed, STREAM_CONF_WITH_ERROR_PATH, false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        server.getCoordinatorService().getJobStatus(jobIdFailed)));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(7, accessCounter.get()));
        JobStateEvent jobStateEventFailed = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobIdFailed), jobStateEventFailed.getJobId());
        Assertions.assertEquals(JobStatus.FAILING, jobStateEventFailed.getJobStatus());
        Assertions.assertTrue(jobStateEventFailed.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobIdFailed), jobStateEventFailed.getJobName());
    }

    @Test
    void testJobStateEventOrderWhenReportNonTerminalJobStateEnabled() {
        server.getCoordinatorService().getEngineConfig().setReportNonTerminalJobState(true);

        JobEventProcessor eventProcessor =
                (JobEventProcessor) server.getCoordinatorService().getEventProcessor();

        List<JobStatus> reportedStatuses = new CopyOnWriteArrayList<>();
        AtomicReference<JobStateEvent> lastJobStateEventReference = new AtomicReference<>();

        EventHandler eventHandler =
                event -> {
                    if (event.getEventType() != EventType.JOB_STATUS) {
                        return;
                    }

                    JobStateEvent jobStateEvent = (JobStateEvent) event;
                    reportedStatuses.add(jobStateEvent.getJobStatus());
                    lastJobStateEventReference.lazySet(jobStateEvent);
                };

        List handlers = (List) ReflectionUtils.getField(eventProcessor, "handlers").get();
        handlers.add(eventHandler);

        long jobId = System.currentTimeMillis();
        long currentTimeMillis = System.currentTimeMillis();

        startJob(jobId, "fake_to_console.conf", false);

        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        reportedStatuses.contains(JobStatus.FINISHED),
                                        "FINISHED event should be reported"));

        Assertions.assertTrue(
                reportedStatuses.contains(JobStatus.SCHEDULED),
                "SCHEDULED event should be reported");
        Assertions.assertTrue(
                reportedStatuses.contains(JobStatus.RUNNING), "RUNNING event should be reported");
        Assertions.assertTrue(
                reportedStatuses.contains(JobStatus.FINISHED), "FINISHED event should be reported");

        Assertions.assertTrue(
                reportedStatuses.indexOf(JobStatus.SCHEDULED)
                        < reportedStatuses.indexOf(JobStatus.RUNNING),
                "SCHEDULED should be reported before RUNNING. Actual events: " + reportedStatuses);

        Assertions.assertTrue(
                reportedStatuses.indexOf(JobStatus.RUNNING)
                        < reportedStatuses.indexOf(JobStatus.FINISHED),
                "RUNNING should be reported before FINISHED. Actual events: " + reportedStatuses);

        JobStateEvent lastJobStateEvent = lastJobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobId), lastJobStateEvent.getJobId());
        Assertions.assertEquals(JobStatus.FINISHED, lastJobStateEvent.getJobStatus());
        Assertions.assertTrue(lastJobStateEvent.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobId), lastJobStateEvent.getJobName());
    }
}
