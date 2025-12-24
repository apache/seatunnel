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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class TaskGroupDefaultImpl implements TaskGroup {
    private final TaskGroupLocation taskGroupLocation;

    private final String taskGroupName;

    private final Map<Long, Task> tasks;

    public TaskGroupDefaultImpl(
            TaskGroupLocation taskGroupLocation, String taskGroupName, Collection<Task> tasks) {
        this.taskGroupLocation = taskGroupLocation;
        this.taskGroupName = taskGroupName;
        // keep the order of tasks, make sure the order of tasks is the same as the jars order in
        // {@link PhysicalVertex::pluginJarsUrls}
        this.tasks = new LinkedHashMap<>();
        tasks.forEach(t -> this.tasks.put(t.getTaskID(), t));
    }

    public String getTaskGroupName() {
        return taskGroupName;
    }

    @Override
    public TaskGroupLocation getTaskGroupLocation() {
        return taskGroupLocation;
    }

    @Override
    public void init() {}

    @Override
    public Collection<Task> getTasks() {
        return tasks.values();
    }

    @Override
    public <T extends Task> T getTask(long taskID) {
        return (T) tasks.get(taskID);
    }

    @Override
    public void setTasksContext(Map<Long, TaskExecutionContext> taskExecutionContextMap) {}

    @Override
    public TaskGroupType getTaskGroupType() {
        return TaskGroupType.DEFAULT;
    }
}
