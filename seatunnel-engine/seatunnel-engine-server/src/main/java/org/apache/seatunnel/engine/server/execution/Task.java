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

import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.Stateful;
import org.apache.seatunnel.engine.server.metrics.MetricsContext;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface Task extends DynamicMetricsProvider, InternalCheckpointListener, Stateful, Serializable {

    default void init() throws Exception {
    }

    @NonNull
    ProgressState call() throws Exception;

    @NonNull
    Long getTaskID();

    default boolean isThreadsShare() {
        return false;
    }

    default void close() throws IOException {
    }

    default void setTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
    }

    default void triggerBarrier(Barrier barrier) throws Exception {}

    @Override
    default void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {}

    default MetricsContext getMetricsContext() {
        return null;
    }

    default void provideDynamicMetrics(MetricDescriptor tagger, MetricsCollectionContext context) {
    }
}
