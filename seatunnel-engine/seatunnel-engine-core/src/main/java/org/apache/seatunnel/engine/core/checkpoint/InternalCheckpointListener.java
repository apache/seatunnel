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

package org.apache.seatunnel.engine.core.checkpoint;

import org.apache.seatunnel.api.state.CheckpointListener;

public interface InternalCheckpointListener extends CheckpointListener {

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task. Note that this will NOT lead to the checkpoint being revoked.
     */
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * This method is called as a notification once a distributed checkpoint has been aborted.
     *
     * @param checkpointId The ID of the checkpoint that has been aborted.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task or job.
     */
    @Override
    default void notifyCheckpointAborted(long checkpointId) throws Exception {}

    /**
     * The notification that the checkpoint has ended means that the notifyCheckpointComplete method
     * has been called for all tasks.
     *
     * @param checkpointId The ID of the checkpoint .
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task or job.
     */
    default void notifyCheckpointEnd(long checkpointId) throws Exception {}
}
