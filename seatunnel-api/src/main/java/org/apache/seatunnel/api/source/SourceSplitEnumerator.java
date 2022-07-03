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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The {@link SourceSplitEnumerator} is responsible for enumerating the splits of a source. It will run at master.
 *
 * @param <SplitT>       source split type
 * @param <StateT>source split state type
 */
public interface SourceSplitEnumerator<SplitT extends SourceSplit, StateT> extends AutoCloseable, CheckpointListener {

    void open();

    /**
     * The method is executed by the engine only once.
     */
    void run() throws Exception;

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    void close() throws IOException;

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits    The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    void addSplitsBack(List<SplitT> splits, int subtaskId);

    int currentUnassignedSplitSize();

    void handleSplitRequest(int subtaskId);

    void registerReader(int subtaskId);

    /**
     * If the source is bounded, checkpoint is not triggered.
     */
    StateT snapshotState(long checkpointId) throws Exception;

    /**
     * Handle the source event from {@link SourceReader}.
     *
     * @param subtaskId   The id of the subtask to which the source event from.
     * @param sourceEvent source event.
     */
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
         */
        void assignSplit(int subtaskId, List<SplitT> splits);

        /**
         * Assigns a single split.
         *
         * <p>When assigning multiple splits, it is more efficient to assign all of them in a single
         * call to the {@link #assignSplit} method.
         *
         * @param split     The new split
         * @param subtaskId The index of the operator's parallel subtask that shall receive the split.
         */
        default void assignSplit(int subtaskId, SplitT split) {
            assignSplit(subtaskId, Collections.singletonList(split));
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
