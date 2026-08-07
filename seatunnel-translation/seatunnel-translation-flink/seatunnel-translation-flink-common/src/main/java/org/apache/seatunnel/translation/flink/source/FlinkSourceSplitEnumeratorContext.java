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
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

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
    private final IntConsumer noMoreSplitsSignalListener;

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) {
        this(enumContext, null);
    }

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext,
            IntConsumer noMoreSplitsSignalListener) {
        this.enumContext = enumContext;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(enumContext));
        this.noMoreSplitsSignalListener = noMoreSplitsSignalListener;
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
        if (noMoreSplitsSignalListener != null) {
            noMoreSplitsSignalListener.accept(subtask);
        }
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

    /**
     * Best-effort Flink job id resolution for event enrichment. Failure must never abort
     * checkpoint/savepoint restore (see issue #10193).
     */
    private static String getFlinkJobId(SplitEnumeratorContext enumContext) {
        try {
            return getJobIdForV15(enumContext);
        } catch (Exception e) {
            log.warn(
                    "Failed to resolve Flink job id from SplitEnumeratorContext ({}). "
                            + "Event jobId will be null; checkpoint/savepoint restore continues. Cause: {}",
                    enumContext == null ? "null" : enumContext.getClass().getName(),
                    e.toString());
            return null;
        }
    }

    /**
     * Resolves Flink job id via reflection into Flink internals. On Flink 1.16+ restore, {@code
     * RecreateOnResetOperatorCoordinator} may expose a QuiesceableContext whose nested {@code
     * globalFailureHandler} is null; treat that as a soft miss instead of throwing NPE.
     */
    private static String getJobIdForV15(SplitEnumeratorContext enumContext) throws Exception {
        if (!(enumContext instanceof SourceCoordinatorContext)) {
            return null;
        }
        SourceCoordinatorContext coordinatorContext = (SourceCoordinatorContext) enumContext;
        Field operatorCoordinatorContextField =
                findDeclaredField(coordinatorContext.getClass(), "operatorCoordinatorContext");
        if (operatorCoordinatorContextField == null) {
            return null;
        }
        operatorCoordinatorContextField.setAccessible(true);
        OperatorCoordinator.Context operatorCoordinatorContext =
                (OperatorCoordinator.Context)
                        operatorCoordinatorContextField.get(coordinatorContext);
        if (operatorCoordinatorContext == null) {
            return null;
        }

        // RecreateOnResetOperatorCoordinator.QuiesceableContext wraps the real context.
        if (findDeclaredField(operatorCoordinatorContext.getClass(), "globalFailureHandler")
                == null) {
            Field nestedContextField =
                    findDeclaredField(operatorCoordinatorContext.getClass(), "context");
            if (nestedContextField == null) {
                log.warn(
                        "Cannot resolve Flink job id under restore coordinator context {}; skipping.",
                        operatorCoordinatorContext.getClass().getName());
                return null;
            }
            nestedContextField.setAccessible(true);
            operatorCoordinatorContext =
                    (OperatorCoordinator.Context)
                            nestedContextField.get(operatorCoordinatorContext);
            if (operatorCoordinatorContext == null) {
                return null;
            }
        }

        Field globalFailureHandlerField =
                findDeclaredField(operatorCoordinatorContext.getClass(), "globalFailureHandler");
        if (globalFailureHandlerField == null) {
            return null;
        }
        globalFailureHandlerField.setAccessible(true);
        Object globalFailureHandler = globalFailureHandlerField.get(operatorCoordinatorContext);
        if (globalFailureHandler == null) {
            // Expected on Flink 1.16+ coordinator reset/restore paths.
            log.warn(
                    "Flink globalFailureHandler is null under {}; job id unavailable during restore.",
                    operatorCoordinatorContext.getClass().getName());
            return null;
        }

        Field schedulerField = findDeclaredField(globalFailureHandler.getClass(), "arg$1");
        if (schedulerField == null) {
            return null;
        }
        schedulerField.setAccessible(true);
        Object scheduler = schedulerField.get(globalFailureHandler);
        if (!(scheduler instanceof SchedulerBase)) {
            return null;
        }
        return ((SchedulerBase) scheduler).getExecutionGraph().getJobID().toString();
    }

    private static Field findDeclaredField(Class<?> clazz, String name) {
        for (Field field : clazz.getDeclaredFields()) {
            if (name.equals(field.getName())) {
                return field;
            }
        }
        return null;
    }
}
