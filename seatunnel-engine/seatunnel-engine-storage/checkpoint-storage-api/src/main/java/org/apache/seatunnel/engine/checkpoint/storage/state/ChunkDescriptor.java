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

import lombok.Value;

/**
 * Identifies the chunk file of one action of one subtask in one checkpoint. The chunk location is
 * derived deterministically from these fields, see {@link CheckpointFileLayout#chunkPath}.
 */
@Value
public class ChunkDescriptor {

    String jobId;

    String pipelineId;

    long checkpointId;

    /** The action state key name; it is hashed into the file name, never used verbatim. */
    String actionKey;

    /** The subtask index; {@code -1} identifies the coordinator (e.g. enumerator) state. */
    int subtaskIndex;
}
