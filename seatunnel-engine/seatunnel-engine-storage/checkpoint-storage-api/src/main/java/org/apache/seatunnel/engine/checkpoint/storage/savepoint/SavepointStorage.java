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

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import java.util.List;

/**
 * Optional capability for storage plugins that support versioned savepoint bundles.
 *
 * <p>Storage plugins are free to implement only {@link
 * org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage}; savepoint capability is
 * detected via {@code instanceof SavepointStorage}. Plugins that do not support it must keep the
 * legacy store/restore behavior and never claim directory isolation they do not provide.
 */
public interface SavepointStorage {

    /** Opens a new write attempt. */
    SavepointWriter beginSavepoint(SavepointRequest request) throws CheckpointStorageException;

    /** Lists all completed (fully committed) savepoint bundles of a job, newest first. */
    List<SavepointHandle> listCompletedSavepoints(String jobId) throws CheckpointStorageException;

    /** Reads and fully validates one savepoint bundle. */
    SavepointData readSavepoint(String jobId, String savepointId) throws CheckpointStorageException;

    /** Deletes one savepoint bundle. */
    void deleteSavepoint(String jobId, String savepointId) throws CheckpointStorageException;

    /** Deletes all savepoint bundles of a job (including leftover staging attempts). */
    void deleteSavepoints(String jobId) throws CheckpointStorageException;
}
