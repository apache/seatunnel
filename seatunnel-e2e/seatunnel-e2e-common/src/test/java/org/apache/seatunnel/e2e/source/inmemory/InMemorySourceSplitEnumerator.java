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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InMemorySourceSplitEnumerator
        implements SourceSplitEnumerator<InMemorySourceSplit, InMemoryState> {

    private final Context<InMemorySourceSplit> context;
    private final Object lock = new Object();

    public static final List<String> methodInvoked = new ArrayList<>();

    public InMemorySourceSplitEnumerator(Context<InMemorySourceSplit> context) {
        this.context = context;
    }

    public static List<String> getMethodInvoked() {
        return methodInvoked;
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        methodInvoked.add("run");
        for (int i = 0; i < context.currentParallelism(); i++) {
            synchronized (lock) {
                context.assignSplit(i, new InMemorySourceSplit("split-" + i));
                context.signalNoMoreSplits(i);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<InMemorySourceSplit> splits, int subtaskId) {
        methodInvoked.add("addSplitsBack");
    }

    @Override
    public int currentUnassignedSplitSize() {
        return -1;
    }

    @Override
    public void registerReader(int subtaskId) {
        methodInvoked.add("registerReader_" + subtaskId);
    }

    @Override
    public InMemoryState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new InMemoryState();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {}
}
