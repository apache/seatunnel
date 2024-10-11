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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.server.event.JobEventReportOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class EventService {
    private final BlockingQueue<Event> eventBuffer;

    private ExecutorService eventForwardService;

    private final NodeEngineImpl nodeEngine;

    public EventService(NodeEngineImpl nodeEngine) {
        eventBuffer = new ArrayBlockingQueue<>(2048);
        initEventForwardService();
        this.nodeEngine = nodeEngine;
    }

    private void initEventForwardService() {
        eventForwardService =
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("event-forwarder-%d").build());
        eventForwardService.submit(
                () -> {
                    List<Event> events = new ArrayList<>();
                    RetryUtils.RetryMaterial retryMaterial =
                            new RetryUtils.RetryMaterial(2, true, e -> true);
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            events.clear();

                            Event first = eventBuffer.take();
                            events.add(first);

                            eventBuffer.drainTo(events, 500);
                            JobEventReportOperation operation = new JobEventReportOperation(events);

                            RetryUtils.retryWithException(
                                    () ->
                                            NodeEngineUtil.sendOperationToMasterNode(
                                                            nodeEngine, operation)
                                                    .join(),
                                    retryMaterial);

                            log.debug("Event forward success, events " + events.size());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.info("Event forward thread interrupted");
                        } catch (Throwable t) {
                            log.warn("Event forward failed, discard events " + events.size(), t);
                        }
                    }
                });
    }

    public void reportEvent(Event e) {
        while (!eventBuffer.offer(e)) {
            eventBuffer.poll();
            log.warn("Event buffer is full, discard the oldest event");
        }
    }

    public void shutdownNow() {
        if (eventForwardService != null) {
            eventForwardService.shutdownNow();
        }
    }
}
