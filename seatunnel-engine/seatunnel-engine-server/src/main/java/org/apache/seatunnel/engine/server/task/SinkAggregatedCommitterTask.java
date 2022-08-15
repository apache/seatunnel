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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SinkAggregatedCommitterTask<AggregatedCommitInfoT> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SinkAggregatedCommitterTask.class);
    private static final long serialVersionUID = 5906594537520393503L;
    private final SinkAction<?, ?, ?, AggregatedCommitInfoT> sink;

    private final SinkAggregatedCommitter<?, AggregatedCommitInfoT> aggregatedCommitter;

    private final Map<Long, Address> writerAddressMap;

    private final Map<Long, List<AggregatedCommitInfoT>> checkpointCommitInfoMap;

    public SinkAggregatedCommitterTask(long jobID, TaskLocation taskID, SinkAction<?, ?, ?, AggregatedCommitInfoT> sink,
                                       SinkAggregatedCommitter<?, AggregatedCommitInfoT> aggregatedCommitter) {
        super(jobID, taskID);
        this.sink = sink;
        this.aggregatedCommitter = aggregatedCommitter;
        this.writerAddressMap = new ConcurrentHashMap<>();
        this.checkpointCommitInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    public void init() throws Exception {
        super.init();
        LOGGER.info("starting seatunnel sink aggregated committer task, sink name: " + sink.getName());
    }

    public void receivedWriterRegister(TaskLocation writerID, Address address) {
        this.writerAddressMap.put(writerID.getTaskID(), address);
    }

    public void receivedWriterCommitInfo(long checkpointID, AggregatedCommitInfoT[] commitInfos) {
        if (!checkpointCommitInfoMap.containsKey(checkpointID)) {
            checkpointCommitInfoMap.put(checkpointID, new CopyOnWriteArrayList<>());
        }
        checkpointCommitInfoMap.get(checkpointID).addAll(Arrays.asList(commitInfos));
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(sink.getJarUrls());
    }
}
