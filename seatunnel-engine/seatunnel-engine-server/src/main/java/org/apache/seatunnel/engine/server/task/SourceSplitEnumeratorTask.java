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
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;

import com.hazelcast.cluster.Address;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final long serialVersionUID = -3713701594297977775L;

    private final PhysicalSourceAction<?, SplitT, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;
    private SeaTunnelSplitEnumeratorContext<SplitT> context;
    private Map<Integer, Address> taskMemberMapping;

    @Override
    public void init() throws Exception {
        context = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumerator = this.source.getSource().createEnumerator(context);
        taskMemberMapping = new HashMap<>();
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
    }

    public SourceSplitEnumeratorTask(long taskID, PhysicalSourceAction<?, SplitT, ?> source) {
        super(taskID);
        this.source = source;
    }

    private void receivedReader(int readerId, Address memberAddr) {
        this.addTaskMemberMapping(readerId, memberAddr);
        enumerator.registerReader(readerId);
    }

    private void requestSplit(int taskID) {
        enumerator.handleSplitRequest(taskID);
    }

    private void addTaskMemberMapping(int taskID, Address memberAddr) {
        taskMemberMapping.put(taskID, memberAddr);
    }

    public Address getTaskMemberAddr(int taskID) {
        return taskMemberMapping.get(taskID);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
