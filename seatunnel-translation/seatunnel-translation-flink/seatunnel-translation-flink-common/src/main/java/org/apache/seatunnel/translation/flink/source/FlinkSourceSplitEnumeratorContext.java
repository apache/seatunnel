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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The implementation of {@link org.apache.seatunnel.api.source.SourceSplitEnumerator.Context} for
 * flink engine.
 *
 * @param <SplitT>
 */
@Slf4j
public class FlinkSourceSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext;
    protected final EventListener eventListener;

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) {
        this.enumContext = enumContext;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(enumContext));
    }

    @Override
    public int currentParallelism() {
        return enumContext.currentParallelism();
    }

    @Override
    public Set<Integer> registeredReaders() {
        return enumContext.registeredReaders().keySet();
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        splits.forEach(
                split -> {
                    enumContext.assignSplit(new SplitWrapper<>(split), subtaskId);
                });
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        enumContext.signalNoMoreSplits(subtask);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        enumContext.sendEventToSourceReader(subtaskId, new SourceEventWrapper(event));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    private static String getFlinkJobId(SplitEnumeratorContext enumContext) {
        try {
            return getJobIdForV15(enumContext);
        } catch (Exception e) {
            log.warn("Get flink job id failed", e);
            return null;
        }
    }

    private static String getJobIdForV15(SplitEnumeratorContext enumContext) {
        try {
            SourceCoordinatorContext coordinatorContext = (SourceCoordinatorContext) enumContext;
            Field field =
                    coordinatorContext.getClass().getDeclaredField("operatorCoordinatorContext");
            field.setAccessible(true);
            OperatorCoordinator.Context operatorCoordinatorContext =
                    (OperatorCoordinator.Context) field.get(coordinatorContext);
            Field[] fields = operatorCoordinatorContext.getClass().getDeclaredFields();
            Optional<Field> fieldOptional =
                    Arrays.stream(fields)
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst();
            if (!fieldOptional.isPresent()) {
                // RecreateOnResetOperatorCoordinator.QuiesceableContext
                fieldOptional =
                        Arrays.stream(fields)
                                .filter(f -> f.getName().equals("context"))
                                .findFirst();
                field = fieldOptional.get();
                field.setAccessible(true);
                operatorCoordinatorContext =
                        (OperatorCoordinator.Context) field.get(operatorCoordinatorContext);
            }

            // OperatorCoordinatorHolder.LazyInitializedCoordinatorContext
            field =
                    Arrays.stream(operatorCoordinatorContext.getClass().getDeclaredFields())
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst()
                            .get();
            field.setAccessible(true);

            // SchedulerBase$xxx
            Object obj = field.get(operatorCoordinatorContext);
            fields = obj.getClass().getDeclaredFields();
            field =
                    Arrays.stream(fields)
                            .filter(f -> f.getName().equals("arg$1"))
                            .findFirst()
                            .get();
            field.setAccessible(true);
            SchedulerBase schedulerBase = (SchedulerBase) field.get(obj);
            return schedulerBase.getExecutionGraph().getJobID().toString();
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink job-id failed", e);
        }
    }
}
