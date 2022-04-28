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

package org.apache.seatunnel.api.source;

import org.apache.seatunnel.api.state.CheckpointListener;

import java.util.List;
import java.util.Map;

public interface SourceReader<T, SplitT extends SourceSplit> extends CheckpointListener {

    void start(Collector<T> output) throws Exception;

    List<SplitT> snapshotState(long checkpointId);

    void addSplits(List<SplitT> splits);

    default void handleSourceEvent(SourceEvent sourceEvent) {
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
         * Sends a split request to the source's {@link SourceSplitEnumerator}. This will result in a call to
         * the {@link SourceSplitEnumerator#handleSplitRequest(int, String)} method, with this reader's
         * parallel subtask id and the hostname where this reader runs.
         */
        void sendSplitRequest();

        /**
         * Send a source event to the source coordinator.
         *
         * @param sourceEvent the source event to coordinator.
         */
        void sendSourceEventToCoordinator(SourceEvent sourceEvent);
    }
}
