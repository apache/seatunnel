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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.event.EnumeratorOpenEvent;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider.CoordinatorExecutorThreadFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Covers job-id event enrichment through direct and wrapped Flink coordinator contexts, and
 * best-effort context construction when the optional metadata cannot be resolved.
 */
class FlinkSourceSplitEnumeratorContextTest {

    private static final class DummySplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    @Test
    void constructContextWhenJobIdCannotBeResolved() {
        @SuppressWarnings("unchecked")
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumContext.currentParallelism()).thenReturn(1);
        Mockito.when(enumContext.registeredReaders()).thenReturn(Collections.emptyMap());

        AtomicBoolean signaled = new AtomicBoolean(false);
        FlinkSourceSplitEnumeratorContext<DummySplit> context =
                Assertions.assertDoesNotThrow(
                        () ->
                                new FlinkSourceSplitEnumeratorContext<>(
                                        enumContext, id -> signaled.set(true)));

        Assertions.assertNotNull(context.getEventListener());
        Assertions.assertEquals(1, context.currentParallelism());
        Assertions.assertTrue(context.registeredReaders().isEmpty());

        context.signalNoMoreSplits(0);
        Assertions.assertTrue(signaled.get());
        Mockito.verify(enumContext).signalNoMoreSplits(0);
    }

    @Test
    void resolveJobIdFromCoordinatorContext() throws Exception {
        JobID jobId = new JobID();
        assertEventJobId(
                new FailureHandlerContext(schedulerFailureHandler(jobId)), jobId.toString());
    }

    @Test
    void resolveJobIdFromWrappedRestoreContext() throws Exception {
        JobID jobId = new JobID();
        assertEventJobId(
                new WrappedContext(new FailureHandlerContext(schedulerFailureHandler(jobId))),
                jobId.toString());
    }

    @Test
    void wrappedRestoreContextWithNullFailureHandlerPreservesSourceContext() throws Exception {
        assertEventJobId(new WrappedContext(new FailureHandlerContext(null)), null);
    }

    private SchedulerFailureHandler schedulerFailureHandler(JobID jobId) {
        ExecutionGraph executionGraph = Mockito.mock(ExecutionGraph.class);
        Mockito.when(executionGraph.getJobID()).thenReturn(jobId);
        SchedulerBase scheduler = Mockito.mock(SchedulerBase.class);
        Mockito.when(scheduler.getExecutionGraph()).thenReturn(executionGraph);
        return new SchedulerFailureHandler(scheduler);
    }

    private void assertEventJobId(OperatorCoordinator.Context operatorContext, String expectedJobId)
            throws Exception {
        try (SourceCoordinatorContext<SplitWrapper<DummySplit>> flinkContext =
                new SourceCoordinatorContext<>(
                        Mockito.mock(CoordinatorExecutorThreadFactory.class),
                        1,
                        operatorContext,
                        Mockito.mock(SimpleVersionedSerializer.class))) {
            FlinkSourceSplitEnumeratorContext<DummySplit> context =
                    new FlinkSourceSplitEnumeratorContext<>(flinkContext);
            EnumeratorOpenEvent event = new EnumeratorOpenEvent();

            context.getEventListener().onEvent(event);

            Assertions.assertEquals(expectedJobId, event.getJobId());
            Assertions.assertEquals(1, context.currentParallelism());
            Assertions.assertEquals(Collections.emptySet(), context.registeredReaders());
        }
    }

    /**
     * Supplies stable coordinator metadata without starting a Flink job. Unexpected job failure
     * callbacks fail the test instead of being ignored by a coordinator mock.
     */
    private abstract static class TestOperatorContext implements OperatorCoordinator.Context {
        @Override
        public OperatorID getOperatorId() {
            return new OperatorID();
        }

        @Override
        public void failJob(Throwable cause) {
            throw new AssertionError("Job-id enrichment must not fail the job", cause);
        }

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return getClass().getClassLoader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return null;
        }
    }

    /**
     * Models the wrapper field shape used by Flink's reset coordinator. The real resolver must
     * unwrap this context before looking for the failure handler.
     */
    private static final class WrappedContext extends TestOperatorContext {
        private final OperatorCoordinator.Context context;

        private WrappedContext(OperatorCoordinator.Context context) {
            this.context = context;
        }
    }

    /**
     * Models the lazily initialized coordinator's optional failure handler, including the null
     * value exposed before the scheduler has completed restore initialization.
     */
    private static final class FailureHandlerContext extends TestOperatorContext {
        private final Object globalFailureHandler;

        private FailureHandlerContext(Object globalFailureHandler) {
            this.globalFailureHandler = globalFailureHandler;
        }
    }

    /**
     * Models the scheduler captured by Flink's failure-handler closure. The field name matches the
     * existing production reflection contract rather than a test-only accessor.
     */
    private static final class SchedulerFailureHandler {
        private final SchedulerBase arg$1;

        private SchedulerFailureHandler(SchedulerBase scheduler) {
            this.arg$1 = scheduler;
        }
    }
}
