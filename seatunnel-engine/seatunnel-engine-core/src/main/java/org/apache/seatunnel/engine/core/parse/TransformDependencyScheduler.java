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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;

/** Shared dependency resolution for runtime parsing and connect dry-run validation. */
public final class TransformDependencyScheduler {
    private TransformDependencyScheduler() {}

    /**
     * Resolves every distinct input before plugin discovery in O((V + E) log V) time. Circular
     * selection and Fenwick poll counts preserve the legacy queue's action indexes. Omitted inputs
     * first resolve DEFAULT_ID; only the final unresolved omitted/empty input, or a legacy
     * single-transform mismatch, may use the last inserted table as a fallback.
     */
    public static List<ScheduledTransform> scheduleTransforms(
            List<? extends Config> transformConfigs, Set<String> initialOutputIds) {
        // Index missing inputs once, then release dependents as each output becomes available.
        // The circular ready selection mirrors the legacy retry queue's evaluation order.
        List<ScheduledTransform> transforms = new ArrayList<>(transformConfigs.size());
        Map<String, List<ScheduledTransform>> waitingByInputId = new LinkedHashMap<>();
        NavigableSet<Integer> readyTransformIndexes = new TreeSet<>();
        NavigableSet<Integer> remainingTransformIndexes = new TreeSet<>();
        int[] remainingIndexTree = createRemainingIndexTree(transformConfigs.size());
        Set<String> availableOutputIds = new LinkedHashSet<>(initialOutputIds);

        for (int index = 0; index < transformConfigs.size(); index++) {
            Config config = transformConfigs.get(index);
            ScheduledTransform transform = new ScheduledTransform(index, config);
            transforms.add(transform);
            Set<String> missingInputIds = new LinkedHashSet<>(transform.inputIds);
            missingInputIds.removeAll(availableOutputIds);
            transform.unresolvedInputCount = missingInputIds.size();
            // Explicit empty input lists are fallback-only and are considered after every other
            // transform resolves.
            if (!transform.inputIds.isEmpty()) {
                if (missingInputIds.isEmpty()) {
                    readyTransformIndexes.add(index);
                } else {
                    for (String missingInputId : missingInputIds) {
                        waitingByInputId
                                .computeIfAbsent(missingInputId, ignored -> new ArrayList<>())
                                .add(transform);
                    }
                }
            }
            remainingTransformIndexes.add(index);
        }

        List<ScheduledTransform> orderedTransforms = new ArrayList<>(transforms.size());
        int queueHeadIndex = 0;
        long actionIndex = -1L;
        while (!readyTransformIndexes.isEmpty()) {
            Integer transformIndex = readyTransformIndexes.ceiling(queueHeadIndex);
            if (transformIndex == null) {
                transformIndex = readyTransformIndexes.first();
            }
            actionIndex =
                    nextTransformActionIndex(
                            actionIndex,
                            countRemainingIndexes(
                                    remainingIndexTree,
                                    queueHeadIndex,
                                    transformIndex,
                                    transformConfigs.size()));
            ScheduledTransform transform = transforms.get(transformIndex);
            transform.actionIndex = (int) actionIndex;
            transform.scheduled = true;
            orderedTransforms.add(transform);
            readyTransformIndexes.remove(transformIndex);
            remainingTransformIndexes.remove(transformIndex);
            updateRemainingIndexTree(remainingIndexTree, transformIndex, -1);
            if (!remainingTransformIndexes.isEmpty()) {
                Integer nextQueueHead = remainingTransformIndexes.ceiling(transformIndex);
                queueHeadIndex =
                        nextQueueHead == null ? remainingTransformIndexes.first() : nextQueueHead;
            }
            // Each ID satisfies a dependency once, even when later producers overwrite its value.
            List<ScheduledTransform> dependents = waitingByInputId.remove(transform.outputId);
            availableOutputIds.add(transform.outputId);
            for (ScheduledTransform dependent :
                    dependents == null ? Collections.<ScheduledTransform>emptyList() : dependents) {
                dependent.unresolvedInputCount--;
                if (dependent.unresolvedInputCount == 0) {
                    readyTransformIndexes.add(dependent.configIndex);
                }
            }
        }

        List<ScheduledTransform> unresolvedTransforms =
                transforms.stream()
                        .filter(transform -> !transform.scheduled)
                        .collect(Collectors.toList());
        if (unresolvedTransforms.isEmpty()) {
            return orderedTransforms;
        }

        checkTransformCycles(unresolvedTransforms, availableOutputIds);
        if (unresolvedTransforms.size() == 1) {
            ScheduledTransform transform = unresolvedTransforms.get(0);
            boolean anyInputAvailable =
                    transform.inputIds.stream().anyMatch(availableOutputIds::contains);
            boolean emptyInputFallback = transform.inputIds.isEmpty() || transform.inputOmitted;
            boolean singleTransformLegacyFallback = transforms.size() == 1 && !anyInputAvailable;
            if (emptyInputFallback || singleTransformLegacyFallback) {
                transform.legacyFallback = true;
                actionIndex = nextTransformActionIndex(actionIndex, 1);
                transform.actionIndex = (int) actionIndex;
                orderedTransforms.add(transform);
                return orderedTransforms;
            }
        }
        throw unresolvedTransformDependencies(unresolvedTransforms, availableOutputIds);
    }

    static long nextTransformActionIndex(long currentActionIndex, int polledTransformCount) {
        long nextActionIndex = currentActionIndex + polledTransformCount;
        if (nextActionIndex < 0 || nextActionIndex > Integer.MAX_VALUE) {
            throw new JobDefineCheckException(
                    "Transform action index "
                            + nextActionIndex
                            + " is outside the supported range [0, "
                            + Integer.MAX_VALUE
                            + "]. Reduce the number of transforms or declare them in dependency order.");
        }
        return nextActionIndex;
    }

    private static int[] createRemainingIndexTree(int size) {
        // A Fenwick tree preserves the retry loop's poll-derived action indexes without replaying
        // every unsuccessful poll in reverse-ordered graphs.
        int[] tree = new int[size + 1];
        for (int index = 1; index <= size; index++) {
            tree[index] = index & -index;
        }
        return tree;
    }

    private static void updateRemainingIndexTree(int[] tree, int transformIndex, int delta) {
        for (int index = transformIndex + 1; index < tree.length; index += index & -index) {
            tree[index] += delta;
        }
    }

    private static int countRemainingIndexes(
            int[] tree, int queueHeadIndex, int transformIndex, int transformCount) {
        if (queueHeadIndex <= transformIndex) {
            return countRemainingIndexesThrough(tree, transformIndex)
                    - countRemainingIndexesThrough(tree, queueHeadIndex - 1);
        }
        return countRemainingIndexesThrough(tree, transformCount - 1)
                - countRemainingIndexesThrough(tree, queueHeadIndex - 1)
                + countRemainingIndexesThrough(tree, transformIndex);
    }

    private static int countRemainingIndexesThrough(int[] tree, int transformIndex) {
        int count = 0;
        for (int index = transformIndex + 1; index > 0; index -= index & -index) {
            count += tree[index];
        }
        return count;
    }

    private static JobDefineCheckException unresolvedTransformDependencies(
            List<ScheduledTransform> transforms, Set<String> availableOutputIds) {
        String unresolvedTransforms =
                transforms.stream()
                        .map(
                                transform -> {
                                    return transform.outputId + " <- " + transform.inputIds;
                                })
                        .collect(Collectors.joining(", "));
        return new JobDefineCheckException(
                "Unable to resolve transform dependencies: ["
                        + unresolvedTransforms
                        + "]. Available output IDs: "
                        + availableOutputIds
                        + ". Check 'plugin_input' and 'plugin_output' options.");
    }

    /** A transform in evaluation order, with its legacy poll index and input-binding mode. */
    public static final class ScheduledTransform {
        private final int configIndex;
        private final Config config;
        private final String outputId;
        private final List<String> inputIds;
        private final boolean inputOmitted;
        private int unresolvedInputCount;
        private int actionIndex;
        private boolean scheduled;
        private boolean legacyFallback;

        private ScheduledTransform(int configIndex, Config config) {
            this.configIndex = configIndex;
            this.config = config;
            ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
            this.outputId = readonlyConfig.getOptional(PLUGIN_OUTPUT).orElse(DEFAULT_ID);
            this.inputIds = ConfigParserUtil.getInputIds(readonlyConfig);
            this.inputOmitted = !readonlyConfig.getOptional(PLUGIN_INPUT).isPresent();
        }

        public Config getConfig() {
            return config;
        }

        public boolean isLegacyFallback() {
            return legacyFallback;
        }

        public String getOutputId() {
            return outputId;
        }

        public List<String> getInputIds() {
            return inputIds;
        }

        public int getActionIndex() {
            return actionIndex;
        }
    }

    private static void checkTransformCycles(
            List<ScheduledTransform> transforms, Set<String> availableOutputIds) {
        if (transforms.isEmpty()) {
            return;
        }

        // Source inputs are graph roots, so only transform-to-transform edges contribute to the
        // in-degree. Linked collections keep the reported cycle stable for the configured order.
        Set<String> transformOutputIds = new LinkedHashSet<>();
        for (ScheduledTransform transform : transforms) {
            if (!availableOutputIds.contains(transform.outputId)) {
                transformOutputIds.add(transform.outputId);
            }
        }

        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, List<String>> dependentsByInput = new LinkedHashMap<>();
        Map<String, List<String>> dependenciesByOutput = new LinkedHashMap<>();
        for (String outputId : transformOutputIds) {
            inDegree.put(outputId, 0);
            dependentsByInput.put(outputId, new ArrayList<>());
            dependenciesByOutput.put(outputId, new ArrayList<>());
        }

        for (ScheduledTransform transform : transforms) {
            String outputId = transform.outputId;
            if (!transformOutputIds.contains(outputId)) {
                continue;
            }
            for (String inputId : transform.inputIds) {
                // An omitted input/output pair is a legacy implicit chain, not an explicit
                // self-edge.
                if (transform.inputOmitted && inputId.equals(outputId)) {
                    continue;
                }
                if (!transformOutputIds.contains(inputId)) {
                    continue;
                }
                inDegree.put(outputId, inDegree.get(outputId) + 1);
                dependentsByInput.get(inputId).add(outputId);
                dependenciesByOutput.get(outputId).add(inputId);
            }
        }

        Queue<String> ready = new LinkedList<>();
        inDegree.forEach(
                (outputId, dependencyCount) -> {
                    if (dependencyCount == 0) {
                        ready.offer(outputId);
                    }
                });

        int resolvedTransforms = 0;
        while (!ready.isEmpty()) {
            String resolvedOutputId = ready.poll();
            resolvedTransforms++;
            for (String dependentOutputId : dependentsByInput.get(resolvedOutputId)) {
                int remainingDependencies = inDegree.get(dependentOutputId) - 1;
                inDegree.put(dependentOutputId, remainingDependencies);
                if (remainingDependencies == 0) {
                    ready.offer(dependentOutputId);
                }
            }
        }

        if (resolvedTransforms == transformOutputIds.size()) {
            return;
        }

        Set<String> unresolvedOutputIds = new LinkedHashSet<>();
        inDegree.forEach(
                (outputId, dependencyCount) -> {
                    if (dependencyCount > 0) {
                        unresolvedOutputIds.add(outputId);
                    }
                });
        List<String> cycle = findTransformCycle(dependenciesByOutput, unresolvedOutputIds);
        throw new JobDefineCheckException(
                String.format(
                        "Transform dependency cycle detected: %s. Check '%s' and '%s' options.",
                        String.join(" -> ", cycle), PLUGIN_INPUT.key(), PLUGIN_OUTPUT.key()));
    }

    private static List<String> findTransformCycle(
            Map<String, List<String>> dependenciesByOutput, Set<String> unresolvedOutputIds) {
        // Every node left by Kahn's algorithm depends on another unresolved node. Following one
        // dependency at a time therefore reaches a repeated node and yields a concrete cycle.
        for (String startOutputId : unresolvedOutputIds) {
            Map<String, Integer> pathIndexes = new HashMap<>();
            List<String> path = new ArrayList<>();
            String outputId = startOutputId;
            while (outputId != null) {
                Integer cycleStartIndex = pathIndexes.get(outputId);
                if (cycleStartIndex != null) {
                    List<String> cycle =
                            new ArrayList<>(path.subList(cycleStartIndex, path.size()));
                    cycle.add(outputId);
                    return cycle;
                }
                pathIndexes.put(outputId, path.size());
                path.add(outputId);

                outputId = null;
                for (String dependencyId : dependenciesByOutput.get(path.get(path.size() - 1))) {
                    if (unresolvedOutputIds.contains(dependencyId)) {
                        outputId = dependencyId;
                        break;
                    }
                }
            }
        }
        return new ArrayList<>(unresolvedOutputIds);
    }
}
