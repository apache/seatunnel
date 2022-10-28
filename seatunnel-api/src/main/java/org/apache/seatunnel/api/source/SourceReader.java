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
import java.util.List;

/**
 * The {@link SourceReader} is used to generate source record, and it will be running at worker.
 *
 * @param <T>      record type.
 * @param <SplitT> source split type.
 */
public interface SourceReader<T, SplitT extends SourceSplit> extends AutoCloseable, CheckpointListener {

    /**
     * Open the source reader.
     */
    void open() throws Exception;

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    void close() throws IOException;

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    void pollNext(Collector<T> output) throws Exception;

    /**
     * Get the current split checkpoint state by checkpointId.
     *
     * If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint Id.
     * @return split checkpoint state.
     * @throws Exception if error occurs.
     */
    List<SplitT> snapshotState(long checkpointId) throws Exception;

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    void addSplits(List<SplitT> splits);

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SourceSplitEnumerator.Context#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    void handleNoMoreSplits();

    /**
     * Handle the source event form {@link SourceSplitEnumerator}.
     *
     * @param sourceEvent source event.
     */
    default void handleSourceEvent(SourceEvent sourceEvent) {
    }

    interface Context {

        /**
         * @return The index of this subtask.
         */
        int getIndexOfSubtask();

        /**
         * @return boundedness of this reader.
         */
        Boundedness getBoundedness();

        /**
         * Indicator that the input has reached the end of data. Then will cancel this reader.
         */
        void signalNoMoreElement();

        /**
         * Sends a split request to the source's {@link SourceSplitEnumerator}. This will result in a call to
         * the {@link SourceSplitEnumerator#handleSplitRequest(int)} method, with this reader's
         * parallel subtask id and the hostname where this reader runs.
         */
        void sendSplitRequest();

        /**
         * Send a source event to the source coordinator.
         *
         * @param sourceEvent the source event to coordinator.
         */
        void sendSourceEventToEnumerator(SourceEvent sourceEvent);
    }
}
