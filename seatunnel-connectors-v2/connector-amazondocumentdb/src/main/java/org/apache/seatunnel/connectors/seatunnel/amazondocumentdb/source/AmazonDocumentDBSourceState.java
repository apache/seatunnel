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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enumerator checkpoint for the V1 single-split contract.
 *
 * <p>The pending map contains copies of split id, filter, and projection only. Assigned reader
 * state is checkpointed by the reader, and neither side stores a MongoDB cursor position; a
 * restored active scan therefore starts again from the beginning.
 */
public class AmazonDocumentDBSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean shouldEnumerate;
    private final Map<Integer, List<AmazonDocumentDBSourceSplit>> pendingSplits;

    public AmazonDocumentDBSourceState(
            boolean shouldEnumerate,
            Map<Integer, List<AmazonDocumentDBSourceSplit>> pendingSplits) {
        this.shouldEnumerate = shouldEnumerate;
        this.pendingSplits = copyPendingSplits(pendingSplits);
    }

    public boolean isShouldEnumerate() {
        return shouldEnumerate;
    }

    public Map<Integer, List<AmazonDocumentDBSourceSplit>> getPendingSplits() {
        return copyPendingSplits(pendingSplits);
    }

    private static Map<Integer, List<AmazonDocumentDBSourceSplit>> copyPendingSplits(
            Map<Integer, List<AmazonDocumentDBSourceSplit>> source) {
        Map<Integer, List<AmazonDocumentDBSourceSplit>> copy = new HashMap<>();
        source.forEach(
                (reader, splits) -> {
                    List<AmazonDocumentDBSourceSplit> splitCopies = new ArrayList<>();
                    splits.forEach(split -> splitCopies.add(split.copy()));
                    copy.put(reader, splitCopies);
                });
        return copy;
    }
}
