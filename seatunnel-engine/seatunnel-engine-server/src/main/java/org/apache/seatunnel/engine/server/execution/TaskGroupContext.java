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

import lombok.Getter;
import lombok.Setter;

import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-side runtime context for one deployment of a task group.
 *
 * <p>{@link TaskGroupLocation} identifies the logical task group and is reused after restore. In
 * contrast, {@code executionId} identifies this particular deployment. Equality and hash code are
 * therefore based only on the immutable execution ID so this context can safely be used as the key
 * for execution-scoped resources.
 *
 * <p>Mutable fields such as {@code taskGroup}, {@code classLoaders}, and {@code jars} must never
 * participate in {@code equals} or {@code hashCode}. Changing those fields after inserting this
 * context into a map must not change its key.
 */
@Getter
@Setter
public class TaskGroupContext {

    /**
     * Globally unique ID of this deployment attempt.
     *
     * <p>The master generates a new ID whenever it deploys the task group and sends it to the
     * worker through {@code TaskGroupImmutableInformation}. It is immutable because this field is
     * the complete identity used by {@link #equals(Object)} and {@link #hashCode()}.
     */
    private final long executionId;

    /**
     * Runtime task group created for this deployment.
     *
     * <p>It contains the executable tasks and their shared lifecycle state. Worker operations use
     * it to locate and control a task after resolving the active context. The reference is mutable
     * for lifecycle management, so it is deliberately excluded from equality and hash code.
     */
    private TaskGroup taskGroup;

    /**
     * Classloader used by each task, indexed by task ID.
     *
     * <p>The worker keeps these references for task execution and clears them when this deployment
     * finishes, allowing the deployment's classloaders to be reclaimed. This mutable cleanup state
     * must not affect the context's map-key identity.
     */
    private ConcurrentHashMap<Long, ClassLoader> classLoaders;

    /**
     * Connector/plugin jar URLs acquired for each task, indexed by task ID.
     *
     * <p>The URLs record which classloader references belong to this deployment. They are used to
     * release the matching references from {@code ClassLoaderService} during cleanup, including
     * cleanup of a stale deployment after a newer one has started at the same task-group location.
     */
    private ConcurrentHashMap<Long, Collection<URL>> jars;

    public TaskGroupContext(
            long executionId,
            TaskGroup taskGroup,
            ConcurrentHashMap<Long, ClassLoader> classLoaders,
            ConcurrentHashMap<Long, Collection<URL>> jars) {
        this.executionId = executionId;
        this.taskGroup = taskGroup;
        this.classLoaders = classLoaders;
        this.jars = jars;
    }

    public ClassLoader getClassLoader(long taskId) {
        if (classLoaders != null) {
            return classLoaders.get(taskId);
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TaskGroupContext that = (TaskGroupContext) other;
        return executionId == that.executionId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(executionId);
    }
}
