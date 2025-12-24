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

package org.apache.seatunnel.engine.server.task.operation.source;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

/**
 * For {@link org.apache.seatunnel.api.source.SourceReader} send event to the {@link
 * org.apache.seatunnel.api.source.SourceSplitEnumerator}
 */
public class SourceReaderEventOperation extends SourceEventOperation {
    public SourceReaderEventOperation() {}

    public SourceReaderEventOperation(
            TaskLocation targetTaskLocation, TaskLocation currentTaskLocation, SourceEvent event) {
        super(targetTaskLocation, currentTaskLocation, event);
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.SOURCE_READER_EVENT_OPERATOR;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(
                () -> {
                    SourceSplitEnumeratorTask<?> task =
                            server.getTaskExecutionService().getTask(taskLocation);
                    ClassLoader classLoader =
                            server.getTaskExecutionService()
                                    .getExecutionContext(taskLocation.getTaskGroupLocation())
                                    .getClassLoader(task.getTaskID());
                    task.handleSourceEvent(
                            currentTaskLocation.getTaskIndex(),
                            SerializationUtils.deserialize(sourceEvent, classLoader));
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception ->
                                exception instanceof TaskGroupContextNotFoundException
                                        && !server.taskIsEnded(taskLocation.getTaskGroupLocation()),
                        Constant.OPERATION_RETRY_SLEEP));
    }
}
