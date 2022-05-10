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

public interface SinkWriter<T, CommitInfoT, StateT> {

    void write(T element) throws IOException;

    /**
     * prepare the commit, will be called before {@link #snapshotState()}
     *
     * @return the commit info need to commit
     */
    CommitInfoT prepareCommit() throws IOException;

    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     */
    default List<StateT> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    void close() throws IOException;

    interface Context {

        /**
         * Gets the configuration with which Job was started.
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
