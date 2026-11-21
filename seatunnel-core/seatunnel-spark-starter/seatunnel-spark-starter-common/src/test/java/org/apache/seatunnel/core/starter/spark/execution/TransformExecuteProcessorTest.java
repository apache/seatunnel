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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TransformExecuteProcessorTest {

    @Test
    void opensOnceAndClosesOnceOnTaskCompletion() {
        LifecycleTracker tracker = new LifecycleTracker();
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        function.initialize(taskContext);
        function.initialize(taskContext);

        Assertions.assertEquals(1, tracker.openCount);
        verify(taskContext, times(1)).addTaskCompletionListener(listener.capture());

        listener.getValue().onTaskCompletion(taskContext);
        listener.getValue().onTaskCompletion(taskContext);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void completionListenerSuppressesCloseFailure() {
        LifecycleTracker tracker = new LifecycleTracker();
        tracker.failOnClose = true;
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        function.initialize(taskContext);
        verify(taskContext).addTaskCompletionListener(listener.capture());

        Assertions.assertDoesNotThrow(() -> listener.getValue().onTaskCompletion(taskContext));
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void registersCleanupBeforeOpeningTransform() {
        LifecycleTracker tracker = new LifecycleTracker();
        tracker.failOnOpen = true;
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        Assertions.assertThrows(RuntimeException.class, () -> function.initialize(taskContext));
        verify(taskContext).addTaskCompletionListener(listener.capture());

        listener.getValue().onTaskCompletion(taskContext);
        Assertions.assertEquals(1, tracker.openCount);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    private static TransformExecuteProcessor.TransformMapPartitionsFunction createFunction(
            SeaTunnelMapTransform<SeaTunnelRow> transform) {
        return new TransformExecuteProcessor.TransformMapPartitionsFunction(transform, null, null);
    }

    private static ArgumentCaptor<TaskCompletionListener> registerCompletionListener(
            TaskContext taskContext) {
        ArgumentCaptor<TaskCompletionListener> listener =
                ArgumentCaptor.forClass(TaskCompletionListener.class);
        doReturn(taskContext).when(taskContext).addTaskCompletionListener(listener.capture());
        return listener;
    }

    private static class LifecycleTracker {
        private int openCount;
        private int closeCount;
        private boolean failOnOpen;
        private boolean failOnClose;
    }

    private static class TrackingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {
        private final LifecycleTracker tracker;

        private TrackingMapTransform(LifecycleTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public String getPluginName() {
            return "lifecycle-test";
        }

        @Override
        public void open() {
            tracker.openCount++;
            if (tracker.failOnOpen) {
                throw new RuntimeException("expected open failure");
            }
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            tracker.closeCount++;
            if (tracker.failOnClose) {
                throw new RuntimeException("expected close failure");
            }
        }
    }
}
