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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SourceSplitEnumerator<SplitT extends SourceSplit, StateT> extends CheckpointListener {

    void handleSplitRequest(int subtaskId, String requesterHostname);

    void registerReader(int subtaskId);

    StateT snapshotState(long checkpointId) throws Exception;

    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    }

    interface Context<SplitT extends SourceSplit> {

        int currentParallelism();

        /**
         * Get the currently registered readers. The mapping is from subtask id to the reader info.
         *
         * @return the currently registered readers.
         */
        Set<Integer> registeredReaders();

        /**
         * Assign the splits.
         *
         * @param newSplitAssignments the new split assignments to add.
         */
        void assignSplits(Map<Integer, List<SplitT>> newSplitAssignments);

        /**
         * Assigns a single split.
         *
         * <p>When assigning multiple splits, it is more efficient to assign all of them in a single
         * call to the {@link #assignSplits} method.
         *
         * @param split   The new split
         * @param subtask The index of the operator's parallel subtask that shall receive the split.
         */
        default void assignSplit(SplitT split, int subtask) {
            Map<Integer, List<SplitT>> splits = new HashMap<>();
            splits.put(subtask, Collections.singletonList(split));
            assignSplits(splits);
        }

        /**
         * Signals a subtask that it will not receive any further split.
         *
         * @param subtask The index of the operator's parallel subtask that shall be signaled it will
         *                not receive any further split.
         */
        void signalNoMoreSplits(int subtask);

        /**
         * Send a source event to a source reader. The source reader is identified by its subtask id.
         *
         * @param subtaskId the subtask id of the source reader to send this event to.
         * @param event     the source event to send.
         */
        void sendEventToSourceReader(int subtaskId, SourceEvent event);
    }

}
