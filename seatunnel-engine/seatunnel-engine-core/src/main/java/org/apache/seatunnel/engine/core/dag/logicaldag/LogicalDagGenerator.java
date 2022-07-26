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

package org.apache.seatunnel.engine.core.dag.logicaldag;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.dag.actions.Action;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;
    private LogicalDag logicalDag;
    private JobConfig jobConfig;

    private Map<Action, Collection<Integer>> alreadyTransformed = new HashMap<>();

    private Map<Integer, LogicalVertex> logicalIdVertexMap = new HashMap<>();

    public LogicalDagGenerator(@NonNull List<Action> actions,
                               @NonNull JobConfig jobConfig) {
        this.actions = actions;
        this.jobConfig = jobConfig;
        if (actions.size() <= 0) {
            throw new IllegalStateException("No actions define in the job. Cannot execute.");
        }
    }

    public LogicalDag generate() {
        logicalDag = new LogicalDag();
        for (Action action : actions) {
            transformAction(action);
        }
        return logicalDag;
    }

    private Collection<Integer> transformAction(Action action) {
        if (alreadyTransformed.containsKey(action)) {
            return alreadyTransformed.get(action);
        }

        Collection<Integer> upstreamVertexIds = new ArrayList<>();
        List<Action> upstream = action.getUpstream();
        if (!CollectionUtils.isEmpty(upstream)) {
            for (Action upstreamAction : upstream) {
                upstreamVertexIds.addAll(transformAction(upstreamAction));
            }
        }

        LogicalVertex logicalVertex =
            new LogicalVertex(action.getId(), action, action.getParallelism());
        logicalDag.addLogicalVertex(logicalVertex);
        Collection<Integer> transformedActions = Lists.newArrayList(logicalVertex.getVertexId());
        alreadyTransformed.put(action, transformedActions);
        logicalIdVertexMap.put(logicalVertex.getVertexId(), logicalVertex);

        if (!CollectionUtils.isEmpty(upstreamVertexIds)) {
            upstreamVertexIds.stream().forEach(id -> {
                LogicalEdge logicalEdge = new LogicalEdge(logicalIdVertexMap.get(id), logicalVertex);
                logicalDag.addEdge(logicalEdge);
            });
        }
        return transformedActions;
    }
}
