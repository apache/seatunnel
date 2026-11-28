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

package org.apache.seatunnel.engine.checkpoint.storage.state;

import java.io.Serializable;

/**
 * A reference to the checkpointed state of one action of one subtask (STIP-45).
 *
 * <p>A handle either embeds the state bytes ({@link InlineStateHandle}) or points to a chunk file
 * written to the checkpoint storage ({@link FileChunkStateHandle}). Handles are small and are what
 * travels in acknowledgements and in the checkpoint metadata, so the master never has to hold the
 * state bytes of externalized state.
 */
public interface StateHandle extends Serializable {

    /**
     * Returns the size of the referenced state in bytes, i.e. the sum of the lengths of all
     * non-null state segments. Chunk file framing overhead is not included.
     *
     * @return state size in bytes
     */
    long getStateSize();
}
