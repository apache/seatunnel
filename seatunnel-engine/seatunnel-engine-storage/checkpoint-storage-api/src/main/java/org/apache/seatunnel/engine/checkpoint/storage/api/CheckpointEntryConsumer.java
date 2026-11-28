/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

/**
 * Receives one stored checkpoint at a time from {@link CheckpointStorage#forEachCheckpoint(String,
 * CheckpointEntryConsumer)}.
 *
 * <p>The visited {@link PipelineState} and its state payload are only owned by the storage for the
 * duration of the call. A consumer that needs a payload after returning has to copy it; a consumer
 * that only classifies and deletes lets the storage release the payload, which is what keeps a
 * cleanup over a long saved history within a bounded amount of memory.
 */
@FunctionalInterface
public interface CheckpointEntryConsumer {

    /**
     * Consumes one stored checkpoint.
     *
     * @param pipelineState the stored checkpoint, valid only for the duration of this call
     * @throws CheckpointStorageException to abort the visit, for example when the entry cannot be
     *     classified and nothing may be deleted
     */
    void accept(PipelineState pipelineState) throws CheckpointStorageException;
}
