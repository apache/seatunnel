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

package org.apache.seatunnel.engine.checkpoint.storage.exception;

/**
 * A temporary transport failure before a recovery scan observed any checkpoint state.
 *
 * <p>Callers may retry the complete scan. Storage implementations must not use this exception for
 * missing, corrupt or unauthorized state, or after a scan has observed checkpoint files. Backend
 * connection and request timeouts must bound each individual call.
 */
public class CheckpointStorageUnavailableException extends CheckpointStorageException {
    public CheckpointStorageUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
