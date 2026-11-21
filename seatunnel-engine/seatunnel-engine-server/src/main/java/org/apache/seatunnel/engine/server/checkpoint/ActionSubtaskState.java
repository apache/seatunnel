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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.api.ChunkSupportStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.InlineStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.StateHandle;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The checkpointed state of one action of one subtask.
 *
 * <p>The state is either held inline as {@code List<byte[]>} ({@link #getState()}) or, since
 * STIP-45, referenced by an external {@link StateHandle} such as a chunk file. Exactly one of the
 * two is set.
 *
 * <p><b>Compatibility:</b> checkpoint blobs are ProtoStuff-serialized object graphs whose field
 * numbers follow the declaration order. {@code externalStateHandle} must therefore stay the last
 * field, and it is {@code null} for inline state, so inline states serialize to exactly the same
 * bytes as before STIP-45 and older checkpoints remain restorable.
 */
@Data
@ToString(exclude = "state")
public class ActionSubtaskState implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ActionStateKey stateKey;
    private final int index;

    /** Inline state segments; {@code null} when the state is externalized. */
    private final List<byte[]> state;

    /** Externalized state; {@code null} when the state is inline. Must remain the last field. */
    private final StateHandle externalStateHandle;

    /** Creates an inline state, the representation used before STIP-45. */
    public ActionSubtaskState(ActionStateKey stateKey, int index, List<byte[]> state) {
        this(stateKey, index, state, null);
    }

    private ActionSubtaskState(
            ActionStateKey stateKey,
            int index,
            List<byte[]> state,
            StateHandle externalStateHandle) {
        this.stateKey = stateKey;
        this.index = index;
        this.state = state;
        this.externalStateHandle = externalStateHandle;
    }

    /**
     * Creates a state whose bytes live outside the checkpoint metadata, e.g. in a chunk file.
     *
     * @param handle the external handle; inline handles must use the inline constructor instead
     */
    public static ActionSubtaskState ofExternal(
            ActionStateKey stateKey, int index, StateHandle handle) {
        Objects.requireNonNull(handle, "handle");
        if (handle instanceof InlineStateHandle) {
            return new ActionSubtaskState(
                    stateKey, index, ((InlineStateHandle) handle).getStates());
        }
        return new ActionSubtaskState(stateKey, index, null, handle);
    }

    /** Returns whether the state bytes are embedded, i.e. {@link #getState()} is usable. */
    public boolean isInline() {
        return externalStateHandle == null;
    }

    /** Returns a handle to this state, inline or external. */
    public StateHandle getStateHandle() {
        return isInline() ? new InlineStateHandle(state) : externalStateHandle;
    }

    /**
     * Returns an inline copy of this state, reading externalized bytes from the storage. Restore
     * code consumes {@link #getState()}, so external states must be resolved before they reach a
     * task.
     *
     * @param storageSupplier provides the storage the chunk was written to; only called for
     *     external states
     * @throws CheckpointStorageException if the chunk cannot be read
     */
    public ActionSubtaskState toInline(Supplier<ChunkSupportStorage> storageSupplier)
            throws CheckpointStorageException {
        if (isInline()) {
            return this;
        }
        if (!(externalStateHandle instanceof FileChunkStateHandle)) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported state handle %s for action %s, subtask %d",
                            externalStateHandle.getClass().getName(), stateKey, index));
        }
        List<byte[]> states =
                storageSupplier.get().readChunk((FileChunkStateHandle) externalStateHandle);
        return new ActionSubtaskState(stateKey, index, states);
    }

    /**
     * Returns the state size in bytes: the sum of the lengths of the non-null segments, measured
     * where the state was produced.
     */
    public long getStateSize() {
        if (isInline()) {
            return state == null ? 0 : InlineStateHandle.sizeOf(state);
        }
        return externalStateHandle.getStateSize();
    }
}
