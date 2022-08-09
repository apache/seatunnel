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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;
    private SeaTunnelSplitEnumeratorContext<SplitT> context;
    private Map<Integer, UUID> taskMemberMapping;

    @Override
    public void init() throws Exception {
        context = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumerator = this.source.getSource().createEnumerator(context);
        taskMemberMapping = new ConcurrentHashMap<>();
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
    }

    public SourceSplitEnumeratorTask(int taskID, SourceAction<?, SplitT, ?> source) {
        super(taskID);
        this.source = source;
    }

    private void registerReader(int readerId, UUID memberID) {
        this.addTaskMemberMapping(readerId, memberID);
        enumerator.registerReader(readerId);
    }

    private void requestSplit(int taskID) {
        enumerator.handleSplitRequest(taskID);
    }

    private void addTaskMemberMapping(int taskID, UUID memberID) {
        taskMemberMapping.put(taskID, memberID);
    }

    public UUID getTaskMemberID(int taskID) {
        return taskMemberMapping.get(taskID);
    }

    public Set<Integer> getRegisteredReaders() {
        return taskMemberMapping.keySet();
    }

    private void noMoreElement(int taskID) {
        enumerator.handleSplitRequest(taskID);
    }

    @Override
    public void receivedMessage(Object message) {
        // TODO custom message
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
