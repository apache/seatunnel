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

package org.apache.seatunnel.connectors.seatunnel.timeplus.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TimeplusSourceState;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TimeplusSourceSplitEnumerator
        implements SourceSplitEnumerator<TimeplusSourceSplit, TimeplusSourceState> {

    private final Context<TimeplusSourceSplit> context;
    private final Set<Integer> readers;
    private volatile int assigned = -1;

    // TODO support read distributed engine use multi split
    TimeplusSourceSplitEnumerator(Context<TimeplusSourceSplit> enumeratorContext) {
        this.context = enumeratorContext;
        this.readers = new HashSet<>();
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<TimeplusSourceSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (subtaskId == assigned) {
            Optional<Integer> otherReader = readers.stream().filter(r -> r != subtaskId).findAny();
            if (otherReader.isPresent()) {
                context.assignSplit(otherReader.get(), splits);
            } else {
                assigned = -1;
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return assigned < 0 ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        readers.add(subtaskId);
        if (assigned < 0) {
            assigned = subtaskId;
            context.assignSplit(subtaskId, new TimeplusSourceSplit());
        }
    }

    @Override
    public TimeplusSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
