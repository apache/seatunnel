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

package org.apache.seatunnel.api.sink;

import java.util.Optional;

/**
 * Optional sink capability for engines to route equal-key records to the same writer.
 *
 * <p>This keeps ownership-sensitive sinks correct when their parallel writers cannot safely update
 * the same physical shard concurrently.
 *
 * @param <T> record type accepted by the sink
 */
public interface SupportSinkDataPartition<T> {

    /**
     * Returns a partitioner for {@code writerCount} parallel writers, or empty when default engine
     * routing is safe for this sink.
     */
    Optional<SinkDataPartitioner<T>> getSinkDataPartitioner(int writerCount);
    /** Resolves routing on the actual sink, including a multi-table wrapper. */
    static <T> Optional<SinkDataPartitioner<T>> resolve(
            SeaTunnelSink<T, ?, ?, ?> sink, int writerCount) {
        if (writerCount <= 0) {
            throw new IllegalArgumentException("Sink writer parallelism must be positive");
        }
        return sink instanceof SupportSinkDataPartition
                ? ((SupportSinkDataPartition<T>) sink).getSinkDataPartitioner(writerCount)
                : Optional.empty();
    }
}
