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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;

/**
 * A {@link StateHandle} that references a chunk file in a {@link
 * org.apache.seatunnel.engine.checkpoint.storage.api.ChunkSupportStorage}.
 *
 * <p>The location is relative to the storage namespace (for example {@code
 * 100/1/chk-7/subtask-ab12cd34ef56ab78-0-<attemptId>.chunk}), so a checkpoint directory stays valid
 * when the namespace is moved. Reading it back requires the storage it was written to, see {@link
 * org.apache.seatunnel.engine.checkpoint.storage.api.ChunkSupportStorage#readChunk}.
 *
 * <p>The handle binds the chunk content: readers verify the encoded length and CRC32 of the file
 * and the decoded state size against it, so a replaced, truncated or foreign chunk is rejected.
 */
@EqualsAndHashCode
@ToString
public final class FileChunkStateHandle implements StateHandle {

    private static final long serialVersionUID = 1L;

    private final String location;

    private final long stateSize;

    private final long encodedLength;

    private final long checksum;

    /**
     * @param location chunk location relative to the storage namespace
     * @param stateSize total bytes of the state segments
     * @param encodedLength length of the chunk file
     * @param checksum CRC32 of the chunk file content
     */
    public FileChunkStateHandle(
            String location, long stateSize, long encodedLength, long checksum) {
        this.location = Objects.requireNonNull(location, "location");
        this.stateSize = stateSize;
        this.encodedLength = encodedLength;
        this.checksum = checksum;
    }

    /** Returns the chunk location relative to the storage namespace. */
    public String getLocation() {
        return location;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    /** Returns the length of the chunk file. */
    public long getEncodedLength() {
        return encodedLength;
    }

    /** Returns the CRC32 of the chunk file content. */
    public long getChecksum() {
        return checksum;
    }
}
