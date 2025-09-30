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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.engine.server.checkpoint.CheckpointErrorRestoreEndTest.STREAM_CONF_WITH_ERROR_PATH;
import static org.awaitility.Awaitility.await;

public class JobStateEventTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testJobStateEvent() throws InterruptedException {

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
        long jobId_finished = System.currentTimeMillis();
        long currentTimeMillis = System.currentTimeMillis();
        startJob(jobId_finished, "fake_to_console.conf", false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService()
                                                .getJobStatus(jobId_finished)));
        // check whether the event handler is executed
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(1, accessCounter.get()));
        JobStateEvent jobStateEventFinished = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobId_finished), jobStateEventFinished.getJobId());
        Assertions.assertEquals(JobStatus.FINISHED, jobStateEventFinished.getJobStatus());
        Assertions.assertTrue(jobStateEventFinished.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobId_finished), jobStateEventFinished.getJobName());

        long jobId_failed = System.currentTimeMillis();
        startJob(jobId_failed, STREAM_CONF_WITH_ERROR_PATH, false);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        server.getCoordinatorService().getJobStatus(jobId_failed)));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(2, accessCounter.get()));
        JobStateEvent jobStateEventFailed = jobStateEventReference.get();
        Assertions.assertEquals(String.valueOf(jobId_failed), jobStateEventFailed.getJobId());
        Assertions.assertEquals(JobStatus.FAILED, jobStateEventFailed.getJobStatus());
        Assertions.assertTrue(jobStateEventFailed.getCreatedTime() > currentTimeMillis);
        Assertions.assertEquals(String.valueOf(jobId_failed), jobStateEventFailed.getJobName());
    }
}
