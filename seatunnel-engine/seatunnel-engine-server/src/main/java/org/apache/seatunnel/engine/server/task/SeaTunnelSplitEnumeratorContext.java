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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.util.List;
import java.util.Set;

public class SeaTunnelSplitEnumeratorContext<SplitT extends SourceSplit> implements SourceSplitEnumerator.Context<SplitT> {

    private int parallelism;

    public SeaTunnelSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return null;
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {

    }

    @Override
    public void assignSplit(int subtaskId, SplitT split) {
        SourceSplitEnumerator.Context.super.assignSplit(subtaskId, split);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {

    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {

    }
}
