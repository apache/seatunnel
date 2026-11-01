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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.InputPortBinding;
import org.apache.seatunnel.engine.core.dag.actions.PortAwareAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;
    private JobConfig jobConfig;
    private IdGenerator idGenerator;
    private boolean isStartWithSavePoint;

    private final Map<Long, LogicalVertex> logicalVertexMap = new LinkedHashMap<>();

    /**
     * key: input vertex id; <br>
     * value: target vertices id;
     */
    private final Map<Long, LinkedHashSet<Long>> inputVerticesMap = new LinkedHashMap<>();

    /** Port-aware edges staged until all logical vertices have been materialized. */
    private final List<PortAwareEdgeSpec> portAwareEdgeSpecs = new ArrayList<>();

    public LogicalDagGenerator(
            @NonNull List<Action> actions,
            @NonNull JobConfig jobConfig,
            @NonNull IdGenerator idGenerator) {
        this(actions, jobConfig, idGenerator, false);
    }

    public LogicalDagGenerator(
            @NonNull List<Action> actions,
            @NonNull JobConfig jobConfig,
            @NonNull IdGenerator idGenerator,
            boolean isStartWithSavePoint) {
        this.actions = actions;
        this.jobConfig = jobConfig;
        this.idGenerator = idGenerator;
        this.isStartWithSavePoint = isStartWithSavePoint;
        if (actions.isEmpty()) {
            throw new IllegalStateException("No actions define in the job. Cannot execute.");
        }
    }

    public LogicalDag generate() {
        actions.forEach(this::createLogicalVertex);
        validatePortAwareTopology();
        Set<LogicalEdge> logicalEdges = createLogicalEdges();
        LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
        logicalEdges.forEach(logicalDag::addEdge);
        logicalDag.getLogicalVertexMap().putAll(logicalVertexMap);
        logicalDag.setStartWithSavePoint(isStartWithSavePoint);
        return logicalDag;
    }

    private void createLogicalVertex(Action action) {
        final Long logicalVertexId = action.getId();
        if (logicalVertexMap.containsKey(logicalVertexId)) {
            return;
        }
        // connection vertices info
        if (action instanceof PortAwareAction) {
            if (!(action instanceof DynamicLookupAction)) {
                throw new IllegalArgumentException(
                        "Dynamic lookup currently supports only DynamicLookupAction as a port-aware action");
            }
            createPortAwareEdges((PortAwareAction) action, logicalVertexId);
        } else {
            action.getUpstream()
                    .forEach(
                            inputAction -> {
                                createLogicalVertex(inputAction);
                                inputVerticesMap
                                        .computeIfAbsent(
                                                inputAction.getId(), id -> new LinkedHashSet<>())
                                        .add(logicalVertexId);
                            });
        }

        final LogicalVertex logicalVertex =
                new LogicalVertex(logicalVertexId, action, action.getParallelism());
        logicalVertexMap.put(logicalVertexId, logicalVertex);
    }

    private Set<LogicalEdge> createLogicalEdges() {
        Set<LogicalEdge> logicalEdges =
                inputVerticesMap.entrySet().stream()
                        .map(
                                entry ->
                                        entry.getValue().stream()
                                                .map(
                                                        targetId ->
                                                                new LogicalEdge(
                                                                        entry.getKey(), targetId))
                                                .collect(Collectors.toList()))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        portAwareEdgeSpecs.stream()
                .map(PortAwareEdgeSpec::toLogicalEdge)
                .forEach(logicalEdges::add);
        return logicalEdges;
    }

    private void createPortAwareEdges(PortAwareAction action, long targetVertexId) {
        Map<Long, Action> upstreamById =
                action.getUpstream().stream()
                        .collect(Collectors.toMap(Action::getId, upstream -> upstream));
        if (upstreamById.size() != action.getInputPortBindings().size()) {
            throw new IllegalArgumentException(
                    "Port-aware action must bind every upstream exactly once: " + action.getName());
        }
        for (InputPortBinding binding : action.getInputPortBindings()) {
            Action upstream = upstreamById.get(binding.getUpstreamActionId());
            if (upstream == null) {
                throw new IllegalArgumentException(
                        "Missing upstream "
                                + binding.getUpstreamActionId()
                                + " for port-aware action "
                                + action.getName());
            }
            if (action instanceof DynamicLookupAction && !(upstream instanceof SourceAction)) {
                throw new IllegalArgumentException(
                        "Dynamic lookup M0 requires direct SourceAction inputs");
            }
            createLogicalVertex(upstream);
            PortAwareEdgeSpec candidate =
                    new PortAwareEdgeSpec(
                            binding.getEdgeId(),
                            upstream.getId(),
                            targetVertexId,
                            binding.getTargetInputPort(),
                            binding.getExchangeDescriptor());
            portAwareEdgeSpecs.stream()
                    .filter(existing -> existing.edgeId == candidate.edgeId)
                    .filter(existing -> !existing.equals(candidate))
                    .findFirst()
                    .ifPresent(
                            conflicting -> {
                                throw new IllegalArgumentException(
                                        "EDGE_IDENTITY_COLLISION: edgeId="
                                                + candidate.edgeId
                                                + ", existing="
                                                + conflicting
                                                + ", candidate="
                                                + candidate);
                            });
            if (!portAwareEdgeSpecs.contains(candidate)) {
                portAwareEdgeSpecs.add(candidate);
            }
        }
    }

    /**
     * Rejects dynamic lookup topologies whose source ownership or routing semantics are not
     * implemented by the current runtime.
     */
    private void validatePortAwareTopology() {
        Map<String, Long> lookupActionByOperatorUid = new LinkedHashMap<>();
        Map<String, Long> sourceActionByUid = new LinkedHashMap<>();
        Map<Long, Long> lookupOwnerBySourceAction = new LinkedHashMap<>();
        logicalVertexMap.values().stream()
                .map(LogicalVertex::getAction)
                .filter(DynamicLookupAction.class::isInstance)
                .map(DynamicLookupAction.class::cast)
                .forEach(
                        lookupAction -> {
                            Long existingLookup =
                                    lookupActionByOperatorUid.putIfAbsent(
                                            lookupAction.getOperatorUid(), lookupAction.getId());
                            if (existingLookup != null
                                    && existingLookup.longValue() != lookupAction.getId()) {
                                throw new IllegalArgumentException(
                                        "DYNAMIC_LOOKUP_OPERATOR_UID_COLLISION: operatorUid="
                                                + lookupAction.getOperatorUid());
                            }
                            validateSourceIdentity(
                                    sourceActionByUid,
                                    lookupAction.getFactSourceActionUid(),
                                    lookupAction.getFactSourceActionId());
                            validateSourceIdentity(
                                    sourceActionByUid,
                                    lookupAction.getDimensionSourceActionUid(),
                                    lookupAction.getDimensionSourceActionId());
                            for (InputPortBinding binding : lookupAction.getInputPortBindings()) {
                                long sourceActionId = binding.getUpstreamActionId();
                                LogicalVertex sourceVertex = logicalVertexMap.get(sourceActionId);
                                if (sourceVertex == null
                                        || sourceVertex.getParallelism()
                                                != lookupAction.getParallelism()) {
                                    throw new IllegalArgumentException(
                                            "Dynamic lookup M0 FORWARD input requires equal source "
                                                    + "and target parallelism");
                                }
                                Long existingOwner =
                                        lookupOwnerBySourceAction.putIfAbsent(
                                                sourceActionId, lookupAction.getId());
                                if (existingOwner != null
                                        && existingOwner.longValue() != lookupAction.getId()) {
                                    throw new IllegalArgumentException(
                                            "DYNAMIC_LOOKUP_SOURCE_MULTIPLE_OWNERS: sourceActionId="
                                                    + sourceActionId);
                                }
                                Set<Long> normalTargets = inputVerticesMap.get(sourceActionId);
                                if (normalTargets != null && !normalTargets.isEmpty()) {
                                    throw new IllegalArgumentException(
                                            "DYNAMIC_LOOKUP_SOURCE_SHARED_WITH_NORMAL_BRANCH: "
                                                    + "sourceActionId="
                                                    + sourceActionId);
                                }
                            }
                        });
    }

    private static void validateSourceIdentity(
            Map<String, Long> sourceActionByUid, String sourceActionUid, long sourceActionId) {
        Long existingSource = sourceActionByUid.putIfAbsent(sourceActionUid, sourceActionId);
        if (existingSource != null && existingSource.longValue() != sourceActionId) {
            throw new IllegalArgumentException(
                    "DYNAMIC_LOOKUP_SOURCE_UID_COLLISION: sourceActionUid=" + sourceActionUid);
        }
    }

    /** Immutable edge metadata retained while the logical DAG is assembled. */
    private static final class PortAwareEdgeSpec {

        /** Stable edge identity supplied by the action binding. */
        private final long edgeId;

        /** Upstream logical vertex identity. */
        private final long inputVertexId;

        /** Downstream logical vertex identity. */
        private final long targetVertexId;

        /** Explicit downstream input port. */
        private final int targetInputPort;

        /** Versioned routing declaration. */
        private final ExchangeDescriptor exchangeDescriptor;

        private PortAwareEdgeSpec(
                long edgeId,
                long inputVertexId,
                long targetVertexId,
                int targetInputPort,
                ExchangeDescriptor exchangeDescriptor) {
            this.edgeId = edgeId;
            this.inputVertexId = inputVertexId;
            this.targetVertexId = targetVertexId;
            this.targetInputPort = targetInputPort;
            this.exchangeDescriptor = exchangeDescriptor;
        }

        private LogicalEdge toLogicalEdge() {
            return new PortAwareLogicalEdge(
                    edgeId, inputVertexId, targetVertexId, targetInputPort, exchangeDescriptor);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PortAwareEdgeSpec)) {
                return false;
            }
            PortAwareEdgeSpec that = (PortAwareEdgeSpec) other;
            return edgeId == that.edgeId
                    && inputVertexId == that.inputVertexId
                    && targetVertexId == that.targetVertexId
                    && targetInputPort == that.targetInputPort
                    && exchangeDescriptor.equals(that.exchangeDescriptor);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(
                    edgeId, inputVertexId, targetVertexId, targetInputPort, exchangeDescriptor);
        }

        @Override
        public String toString() {
            return "PortAwareEdgeSpec{"
                    + "edgeId="
                    + edgeId
                    + ", inputVertexId="
                    + inputVertexId
                    + ", targetVertexId="
                    + targetVertexId
                    + ", targetInputPort="
                    + targetInputPort
                    + ", exchangeDescriptor="
                    + exchangeDescriptor
                    + '}';
        }
    }
}
