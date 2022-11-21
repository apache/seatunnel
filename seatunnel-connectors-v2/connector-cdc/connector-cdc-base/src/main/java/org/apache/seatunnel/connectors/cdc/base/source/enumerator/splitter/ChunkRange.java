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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter;

import static com.google.common.base.Preconditions.checkArgument;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

/**
 * An internal structure describes a chunk range with a chunk start (inclusive) and chunk end
 * (exclusive). Note that {@code null} represents unbounded chunk start/end.
 */
@Getter
@EqualsAndHashCode
public class ChunkRange {
    private final Object chunkStart;
    private final Object chunkEnd;

    /**
     * Returns a {@link ChunkRange} which represents a full table scan with unbounded chunk start
     * and chunk end.
     */
    public static ChunkRange all() {
        return new ChunkRange(null, null);
    }

    /** Returns a {@link ChunkRange} with the given chunk start and chunk end. */
    public static ChunkRange of(Object chunkStart, Object chunkEnd) {
        return new ChunkRange(chunkStart, chunkEnd);
    }

    private ChunkRange(Object chunkStart, Object chunkEnd) {
        if (chunkStart != null || chunkEnd != null) {
            checkArgument(
                    !Objects.equals(chunkStart, chunkEnd),
                    "Chunk start %s shouldn't be equal to chunk end %s",
                    chunkStart,
                    chunkEnd);
        }
        this.chunkStart = chunkStart;
        this.chunkEnd = chunkEnd;
    }
}
