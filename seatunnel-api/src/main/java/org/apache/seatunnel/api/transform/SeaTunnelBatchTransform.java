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

import java.util.List;
import java.util.Optional;

/**
 * A stateful transform that supports batch processing with checkpoint capability.
 *
 * <p>Unlike {@link SeaTunnelMapTransform} and {@link SeaTunnelFlatMapTransform} which process
 * records one by one, this transform can accumulate records in a buffer and process them in
 * batches. The buffered data is considered as "state" and will be checkpointed.
 *
 * <p>Typical use cases:
 *
 * <ul>
 *   <li>Row-to-column transformation (pivot)
 *   <li>Column-to-row transformation (unpivot)
 *   <li>Aggregation operations
 *   <li>Window-based transformations
 * </ul>
 *
 * <p>The lifecycle of this transform:
 *
 * <ol>
 *   <li>{@link #open()} - Initialize the transform
 *   <li>{@link #collect(Object)} - Collect input records into buffer
 *   <li>{@link #snapshotState(long)} - Save buffer state during checkpoint
 *   <li>{@link #restoreState(List)} - Restore buffer state after recovery
 *   <li>{@link #flush()} - Output buffered data (triggered by checkpoint or close)
 *   <li>{@link #close()} - Clean up resources
 * </ol>
 *
 * @param <T> The type of input and output records (typically SeaTunnelRow)
 * @param <StateT> The type of state that will be checkpointed
 */
public interface SeaTunnelBatchTransform<T, StateT>
        extends SeaTunnelTransform<T>, CheckpointListener {

    /**
     * Collect a record into the internal buffer.
     *
     * <p>This method is called for each input record. The implementation should store the record in
     * an internal buffer for batch processing.
     *
     * @param row The input record to collect
     */
    void collect(T row);

    /**
     * Flush the buffered records and return the transformed results.
     *
     * <p>This method is called when:
     *
     * <ul>
     *   <li>A checkpoint barrier is received
     *   <li>The transform is being closed
     *   <li>The buffer reaches its capacity (implementation-dependent)
     * </ul>
     *
     * @return A list of transformed records, or empty list if no output
     */
    List<T> flush();

    /**
     * Take a snapshot of the current state for checkpointing.
     *
     * <p>This method is called when a checkpoint is triggered. The returned state should contain
     * all buffered data that hasn't been flushed yet.
     *
     * @param checkpointId The ID of the checkpoint
     * @return A list of state objects representing the current buffer state
     * @throws Exception if snapshot fails
     */
    List<StateT> snapshotState(long checkpointId) throws Exception;

    /**
     * Restore the state from a previous checkpoint.
     *
     * <p>This method is called during recovery. The implementation should restore the internal
     * buffer from the provided state.
     *
     * @param states The states to restore from
     * @throws Exception if restoration fails
     */
    void restoreState(List<StateT> states) throws Exception;

    /**
     * Get the serializer for the state type.
     *
     * <p>This serializer is used to serialize/deserialize the state during checkpointing. If the
     * transform doesn't need state serialization (e.g., state is already serializable), return
     * {@link Optional#empty()}.
     *
     * @return Optional containing the state serializer, or empty if not needed
     */
    Optional<Serializer<StateT>> getStateSerializer();

    /**
     * Check if the buffer has any data that needs to be flushed.
     *
     * @return true if the buffer contains data, false otherwise
     */
    default boolean hasBufferedData() {
        return false;
    }

    /**
     * Get the current buffer size.
     *
     * @return The number of records currently in the buffer
     */
    default int getBufferSize() {
        return 0;
    }
}
