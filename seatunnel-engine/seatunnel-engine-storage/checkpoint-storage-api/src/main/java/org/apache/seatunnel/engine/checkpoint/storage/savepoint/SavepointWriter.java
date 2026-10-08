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

package org.apache.seatunnel.engine.checkpoint.storage.savepoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

/**
 * A writer for one savepoint attempt. Pipelines are written into the staging directory first;
 * {@link #commitSavepoint(SavepointMeta)} publishes the bundle by moving payloads to the final
 * directory and writing the {@code _metadata.ser} commit marker.
 */
public interface SavepointWriter {

    /**
     * Writes one pipeline payload into staging.
     *
     * @param state pipeline state (the storage wrapper; payload bytes inside are opaque)
     */
    void writePipeline(PipelineState state) throws CheckpointStorageException;

    /**
     * Publishes the savepoint bundle. The storage fills in the manifest entries (file, length,
     * checksum of the actually written payloads) and the manifest checksum, then atomically writes
     * {@code _metadata.ser} as the commit marker.
     */
    void commitSavepoint(SavepointMeta meta) throws CheckpointStorageException;

    /** Discards the staging directory; safe to call after a failed write or commit. */
    void abortSavepoint() throws CheckpointStorageException;
}
