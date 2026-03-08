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

package org.apache.seatunnel.api.transform;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.state.CheckpointListener;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Stateful transform that can buffer rows, emit batched results, and participate in checkpoint.
 *
 * @param <T> input/output record type
 * @param <StateT> checkpoint state type
 */
public interface SeaTunnelBatchTransform<T, StateT>
        extends SeaTunnelTransform<T>, CheckpointListener {

    /** Collect one input record into the internal batch buffer. */
    void collect(T row);

    /**
     * Drain results that are ready to be emitted without forcing the remaining buffered data to be
     * processed.
     */
    default List<T> drainOutput() {
        return Collections.emptyList();
    }

    /** Force flush buffered data and return all rows ready to be emitted. */
    List<T> flush();

    /** Snapshot buffered state for checkpoint. */
    List<StateT> snapshotState(long checkpointId) throws Exception;

    /** Restore buffered state after recovery. */
    void restoreState(List<StateT> states) throws Exception;

    /** Serializer for checkpoint state. */
    Optional<Serializer<StateT>> getStateSerializer();

    default boolean hasBufferedData() {
        return false;
    }

    default int getBufferSize() {
        return 0;
    }

    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
