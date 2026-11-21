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

package org.apache.seatunnel.engine.checkpoint.storage.exception;

/**
 * Thrown when checkpoint data was read from the storage but its content is invalid: it fails
 * framing, length or checksum validation, cannot be deserialized, or does not belong to the
 * checkpoint it is stored as.
 *
 * <p>Restore may skip such a checkpoint and use an older one. A plain {@link
 * CheckpointStorageException} instead means the data could not be read at all (e.g. a transient I/O
 * error, a NameNode failover or missing permissions); restore must fail then, because the
 * checkpoint itself may be intact.
 */
public class CorruptCheckpointException extends CheckpointStorageException {

    public CorruptCheckpointException(String message) {
        super(message);
    }

    public CorruptCheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
