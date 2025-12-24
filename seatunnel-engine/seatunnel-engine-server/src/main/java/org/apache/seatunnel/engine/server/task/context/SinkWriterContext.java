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

package org.apache.seatunnel.engine.server.task.context;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;

public class SinkWriterContext implements SinkWriter.Context {

    private static final long serialVersionUID = -3082515319043725121L;
    private final int indexOfSubtask;
    private final int numberOfParallelSubtasks;
    private final MetricsContext metricsContext;
    private final EventListener eventListener;

    public SinkWriterContext(
            int numberOfParallelSubtasks,
            int indexOfSubtask,
            MetricsContext metricsContext,
            EventListener eventListener) {
        Preconditions.checkArgument(
                numberOfParallelSubtasks >= 1, "Parallelism must be a positive number.");
        Preconditions.checkArgument(
                indexOfSubtask >= 0, "Task index must be a non-negative number.");
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.indexOfSubtask = indexOfSubtask;
        this.metricsContext = metricsContext;
        this.eventListener = eventListener;
    }

    @Override
    public int getIndexOfSubtask() {
        return indexOfSubtask;
    }

    public int getNumberOfParallelSubtasks() {
        return numberOfParallelSubtasks;
    }

    @Override
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }
}
