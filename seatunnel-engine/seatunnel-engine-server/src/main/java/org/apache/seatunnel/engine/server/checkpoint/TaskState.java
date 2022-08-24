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

package org.apache.seatunnel.engine.server.checkpoint;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class TaskState implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The id of the task.
     */
    private final String taskId;

    /**
     * The handles to states created by the parallel tasks: subtaskIndex -> subtask state.
     */
    private final Map<Integer, byte[]> subtaskStates;

    /**
     * key: subtask index;
     * <br> value: Marks whether a subtask is complete;
     */
    private final Map<Integer, Boolean> completedSubtasks;

    /**
     * The parallelism of the operator when it was checkpointed.
     */
    private final int parallelism;

    public TaskState(String taskId, int parallelism) {
        this.taskId = taskId;
        this.subtaskStates = new HashMap<>(parallelism);
        this.completedSubtasks = new HashMap<>(parallelism);
        IntStream.range(0, parallelism).forEach(index -> completedSubtasks.put(index, false));
        this.parallelism = parallelism;
    }

    public String getTaskId() {
        return taskId;
    }

    public Map<Integer, byte[]> getSubtaskStates() {
        return subtaskStates;
    }

    public void completed(int subtaskIndex) {
        completedSubtasks.put(subtaskIndex, true);
    }

    public boolean isCompleted() {
        return parallelism == completedSubtasks.values()
            .stream().filter(completed -> completed)
            .count();
    }

    public int getParallelism() {
        return parallelism;
    }
}
