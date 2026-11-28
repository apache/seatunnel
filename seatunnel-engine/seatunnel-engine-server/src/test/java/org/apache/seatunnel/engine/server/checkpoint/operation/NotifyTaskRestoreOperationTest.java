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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the delivery reliability of the restore-error report: losing it leaves the task spinning
 * in WAITING_RESTORE forever, so it is retried like the terminal task-status report.
 */
class NotifyTaskRestoreOperationTest {

    @Test
    void shouldRetryUntilTheServiceStops() {
        TaskExecutionService taskExecutionService = mock(TaskExecutionService.class);
        when(taskExecutionService.isRunning()).thenReturn(true, true, false);
        TaskExecutionContext executionContext = mock(TaskExecutionContext.class);
        when(executionContext.getTaskExecutionService()).thenReturn(taskExecutionService);
        when(executionContext.sendToMaster(any(CheckpointErrorReportOperation.class)))
                .thenThrow(new RuntimeException("master unreachable"));
        Task task = mock(Task.class);
        when(task.getExecutionContext()).thenReturn(executionContext);
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 0, 0);

        NotifyTaskRestoreOperation.reportRestoreErrorWithRetry(
                task, taskLocation, new RuntimeException("restore failed"), 1L);

        // Two failed deliveries are retried; the loop gives up once the service stops.
        verify(executionContext, times(2)).sendToMaster(any(CheckpointErrorReportOperation.class));
    }

    @Test
    void shouldNotReportWhenTheServiceAlreadyStopped() {
        TaskExecutionService taskExecutionService = mock(TaskExecutionService.class);
        when(taskExecutionService.isRunning()).thenReturn(false);
        TaskExecutionContext executionContext = mock(TaskExecutionContext.class);
        when(executionContext.getTaskExecutionService()).thenReturn(taskExecutionService);
        Task task = mock(Task.class);
        when(task.getExecutionContext()).thenReturn(executionContext);
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 0, 0);

        NotifyTaskRestoreOperation.reportRestoreErrorWithRetry(
                task, taskLocation, new RuntimeException("restore failed"), 1L);

        verify(executionContext, never()).sendToMaster(any(CheckpointErrorReportOperation.class));
    }
}
