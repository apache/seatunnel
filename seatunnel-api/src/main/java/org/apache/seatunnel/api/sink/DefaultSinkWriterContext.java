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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;

/** The default {@link SinkWriter.Context} implement class. */
public class DefaultSinkWriterContext implements SinkWriter.Context {
    private final int subtask;
    private final int numberOfParallelSubtasks;
    private final EventListener eventListener;

    public DefaultSinkWriterContext(int subtask, int parallelism) {
        this(subtask, parallelism, new DefaultEventProcessor());
    }

    public DefaultSinkWriterContext(String jobId, int subtask, int parallelism) {
        this(subtask, parallelism, new DefaultEventProcessor(jobId));
    }

    public DefaultSinkWriterContext(
            int subtask, int numberOfParallelSubtasks, EventListener eventListener) {
        this.subtask = subtask;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.eventListener = eventListener;
    }

    @Override
    public int getIndexOfSubtask() {
        return subtask;
    }

    public int getNumberOfParallelSubtasks() {
        return numberOfParallelSubtasks;
    }

    @Override
    public MetricsContext getMetricsContext() {
        // TODO Waiting for Flink and Spark to implement MetricsContext
        // https://github.com/apache/seatunnel/issues/3431
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }
}
