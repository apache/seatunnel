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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import java.util.Map;

/** Reads one savepoint bundle format version into runtime checkpoint states. */
public interface SavepointReader {

    /**
     * Decodes all pipeline payloads of a bundle metadata into runtime state.
     *
     * @param meta bundle metadata (already integrity-validated by storage)
     * @param pipelinePayloads raw payload bytes keyed by pipeline id
     * @return runtime checkpoints keyed by pipeline id
     */
    Map<Integer, CompletedCheckpoint> read(
            SavepointMeta meta, Map<Integer, byte[]> pipelinePayloads);
}
