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

package org.apache.seatunnel.e2e.source.inmemory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class InMemorySourceReader implements SourceReader<SeaTunnelRow, InMemorySourceSplit> {

    private final Iterator<SeaTunnelRow> iterator;
    private final SourceReader.Context context;
    private final Deque<InMemorySourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();
    private volatile boolean noMoreSplit;

    public InMemorySourceReader(List<SeaTunnelRow> rows, SourceReader.Context context) {
        this.iterator = rows.iterator();
        this.context = context;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            InMemorySourceSplit split = sourceSplits.poll();
            if (null != split) {
                while (iterator.hasNext()) {
                    SeaTunnelRow row = iterator.next();
                    output.collect(row);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<InMemorySourceSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<InMemorySourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
