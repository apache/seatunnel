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
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

public class JobStateEventTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testJobStateEvent() {

        JobEventProcessor eventProcessor =
                (JobEventProcessor) server.getCoordinatorService().getEventProcessor();

        AtomicInteger accessCounter = new AtomicInteger(0);
        EventHandler eventHandler =
                event -> {
                    if (event instanceof JobStateEvent) {
                        accessCounter.incrementAndGet();
                    }
                };
        // register the event handler
        List<EventHandler> handlers =
                (List<EventHandler>) ReflectionUtils.getField(eventProcessor, "handlers").get();
        handlers.add(eventHandler);
        long jobId = System.currentTimeMillis();
        startJob(jobId, "fake_to_console.conf", false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        // check whether the event handler is executed
        Assertions.assertEquals(1, accessCounter.get());
    }
}
