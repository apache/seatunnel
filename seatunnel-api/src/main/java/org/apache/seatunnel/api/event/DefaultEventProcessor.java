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

package org.apache.seatunnel.api.event;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@AllArgsConstructor
public class DefaultEventProcessor implements EventListener, EventProcessor {
    private final String jobId;
    private final List<EventHandler> handlers;

    public DefaultEventProcessor() {
        this(DefaultEventProcessor.class.getClassLoader());
    }

    public DefaultEventProcessor(String jobId) {
        this(jobId, EventProcessor.loadEventHandlers(DefaultEventProcessor.class.getClassLoader()));
    }

    public DefaultEventProcessor(ClassLoader classLoader) {
        this(null, EventProcessor.loadEventHandlers(classLoader));
    }

    @Override
    public void process(Event event) {
        handlers.forEach(listener -> listener.handle(event));
    }

    @Override
    public void onEvent(Event event) {
        if (jobId != null) {
            event.setJobId(jobId);
        }
        process(event);
    }

    @Override
    public void close() throws Exception {
        log.info("Closing event handlers.");
        EventProcessor.close(handlers);
    }
}
