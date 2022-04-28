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

package org.apache.seatunnel.api.sink;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface SinkWriter<T, StateT> {
    void write(T element) throws IOException, InterruptedException;

    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     * @deprecated implement {@link #snapshotState(long)}
     */
    default List<StateT> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     */
    default List<StateT> snapshotState(long checkpointId) throws IOException {
        return snapshotState();
    }

    interface Context {

        /**
         * Gets the configuration with which Flink was started.
         */
        Map<String, String> getConfiguration();

        /**
         * @return The index of this subtask.
         */
        int getIndexOfSubtask();

        /**
         * @return The number of parallel Sink tasks.
         */
        int getNumberOfParallelSubtasks();

    }
}
