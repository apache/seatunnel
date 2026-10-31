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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

class ConfigParserUtilTest {

    @Test
    void testRejectsCyclicTransformDependencies() {
        List<Config> sources =
                Collections.singletonList(config("plugin_name=FakeSource, plugin_output=src"));
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[t2], plugin_output=t1"),
                        config("plugin_name=sql, plugin_input=[t1], plugin_output=t2"));
        List<Config> sinks =
                Collections.singletonList(config("plugin_name=console, plugin_input=[src]"));

        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () -> ConfigParserUtil.checkGraph(sources, transforms, sinks));

        Assertions.assertTrue(exception.getMessage().contains("Transform dependency cycle"));
        Assertions.assertTrue(exception.getMessage().contains("t1 -> t2 -> t1"));
    }

    @Test
    void testAllowsTransformsDeclaredOutOfDependencyOrder() {
        List<Config> sources =
                Collections.singletonList(config("plugin_name=FakeSource, plugin_output=src"));
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[src,t1], plugin_output=t2"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=t1"));
        List<Config> sinks =
                Collections.singletonList(config("plugin_name=console, plugin_input=[t2]"));

        Assertions.assertDoesNotThrow(
                () -> ConfigParserUtil.checkGraph(sources, transforms, sinks));
    }

    @Test
    void testAllowsDuplicateInputsFromOneTransform() {
        List<Config> sources =
                Collections.singletonList(config("plugin_name=FakeSource, plugin_output=src"));
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[t1,t1], plugin_output=t2"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=t1"));
        List<Config> sinks =
                Collections.singletonList(config("plugin_name=console, plugin_input=[t2]"));

        Assertions.assertDoesNotThrow(
                () -> ConfigParserUtil.checkGraph(sources, transforms, sinks));
    }

    @Test
    void testRejectsSelfCycleInComplexGraph() {
        List<Config> sources =
                Collections.singletonList(config("plugin_name=FakeSource, plugin_output=src"));
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[t1], plugin_output=t1"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=t2"));
        List<Config> sinks =
                Collections.singletonList(config("plugin_name=console, plugin_input=[src,t2]"));

        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () -> ConfigParserUtil.checkGraph(sources, transforms, sinks));

        Assertions.assertTrue(exception.getMessage().contains("t1 -> t1"));
    }

    @Test
    void testPreservesSimpleGraphCompatibility() {
        List<Config> sources =
                Collections.singletonList(config("plugin_name=FakeSource, plugin_output=src"));
        List<Config> transforms =
                Collections.singletonList(
                        config("plugin_name=sql, plugin_input=[legacy], plugin_output=t1"));
        List<Config> sinks =
                Collections.singletonList(config("plugin_name=console, plugin_input=[legacy]"));

        Assertions.assertDoesNotThrow(
                () -> ConfigParserUtil.checkGraph(sources, transforms, sinks));
    }

    @Test
    void testSchedulesLongReverseDependencyChainOnceInLegacyEvaluationOrder() {
        int transformCount = 512;
        List<Config> transforms = new ArrayList<>(transformCount);
        for (int outputIndex = transformCount - 1; outputIndex >= 0; outputIndex--) {
            String inputId = outputIndex == 0 ? "src" : "t" + (outputIndex - 1);
            transforms.add(
                    config(
                            "plugin_name=sql, plugin_input=["
                                    + inputId
                                    + "], plugin_output=t"
                                    + outputIndex));
        }

        List<TransformDependencyScheduler.ScheduledTransform> scheduled =
                TransformDependencyScheduler.scheduleTransforms(
                        transforms, Collections.singleton("src"));

        Assertions.assertEquals(transformCount, scheduled.size());
        Set<String> scheduledOutputs = new HashSet<>();
        int expectedActionIndex = -1;
        for (int evaluationIndex = 0; evaluationIndex < transformCount; evaluationIndex++) {
            TransformDependencyScheduler.ScheduledTransform transform =
                    scheduled.get(evaluationIndex);
            expectedActionIndex += transformCount - evaluationIndex;
            Assertions.assertEquals("t" + evaluationIndex, transform.getOutputId());
            Assertions.assertEquals(expectedActionIndex, transform.getActionIndex());
            Assertions.assertTrue(scheduledOutputs.add(transform.getOutputId()));
        }
    }

    @Test
    void testRejectsLegacyActionIndexOutsideIntegerRange() {
        Assertions.assertEquals(
                Integer.MAX_VALUE,
                TransformDependencyScheduler.nextTransformActionIndex(Integer.MAX_VALUE - 1L, 1));

        JobDefineCheckException upperBoundException =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                TransformDependencyScheduler.nextTransformActionIndex(
                                        Integer.MAX_VALUE, 1));
        Assertions.assertTrue(
                upperBoundException.getMessage().contains("outside the supported range"));
        Assertions.assertThrows(
                JobDefineCheckException.class,
                () -> TransformDependencyScheduler.nextTransformActionIndex(-2L, 1));
    }

    @Test
    void testDoesNotApplyLegacyFallbackAfterAnotherTransformWasScheduled() {
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[src], plugin_output=t1"),
                        config("plugin_name=sql, plugin_input=[missing], plugin_output=t2"));

        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                TransformDependencyScheduler.scheduleTransforms(
                                        transforms, Collections.singleton("src")));

        Assertions.assertTrue(
                exception.getMessage().contains("Unable to resolve transform dependencies"));
        Assertions.assertTrue(exception.getMessage().contains("t2 <- [missing]"));
    }

    @Test
    void testRejectsAmbiguousEmptyInputFallbacks() {
        List<Config> multipleEmptyInputs =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[], plugin_output=t1"),
                        config("plugin_name=sql, plugin_input=[], plugin_output=t2"));
        List<Config> emptyInputWithDependent =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[], plugin_output=t1"),
                        config("plugin_name=sql, plugin_input=[t1], plugin_output=t2"));

        Assertions.assertThrows(
                JobDefineCheckException.class,
                () ->
                        TransformDependencyScheduler.scheduleTransforms(
                                multipleEmptyInputs, Collections.singleton("src")));
        Assertions.assertThrows(
                JobDefineCheckException.class,
                () ->
                        TransformDependencyScheduler.scheduleTransforms(
                                emptyInputWithDependent, Collections.singleton("src")));
    }

    private static Config config(String value) {
        return ConfigFactory.parseString("{" + value + "}");
    }

    @Test
    void testSimpleGraphRejectsExplicitSelfCycle() {
        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                ConfigParserUtil.checkGraph(
                                        Collections.singletonList(
                                                config(
                                                        "plugin_name=FakeSource, plugin_output=src")),
                                        Collections.singletonList(
                                                config(
                                                        "plugin_name=sql, plugin_input=[self], plugin_output=self")),
                                        Collections.singletonList(
                                                config(
                                                        "plugin_name=console, plugin_input=[self]"))));
        Assertions.assertTrue(exception.getMessage().contains("self -> self"));
    }

    @Test
    void testImplicitSingleTransformIsNotAnExplicitSelfCycle() {
        List<TransformDependencyScheduler.ScheduledTransform> scheduled =
                TransformDependencyScheduler.scheduleTransforms(
                        Collections.singletonList(config("plugin_name=sql")),
                        Collections.singleton("src"));
        Assertions.assertTrue(scheduled.get(0).isLegacyFallback());
        Assertions.assertEquals(0, scheduled.get(0).getActionIndex());
    }

    @Test
    void testExistingOutputAllowsInPlaceTransform() {
        List<TransformDependencyScheduler.ScheduledTransform> scheduled =
                TransformDependencyScheduler.scheduleTransforms(
                        Collections.singletonList(
                                config("plugin_name=sql, plugin_input=[src], plugin_output=src")),
                        Collections.singleton("src"));
        Assertions.assertFalse(scheduled.get(0).isLegacyFallback());
    }

    @Test
    void testDuplicateOutputsCannotHideUnproducedInput() {
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[src], plugin_output=dup"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=dup"),
                        config(
                                "plugin_name=sql, plugin_input=[dup,missing], plugin_output=joined"));
        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                TransformDependencyScheduler.scheduleTransforms(
                                        transforms, Collections.singleton("src")));
        Assertions.assertTrue(exception.getMessage().contains("joined <- [dup, missing]"));
    }

    @Test
    void testOmittedInputIsNotFallbackWhileAnotherTransformIsUnresolved() {
        Assertions.assertThrows(
                JobDefineCheckException.class,
                () ->
                        TransformDependencyScheduler.scheduleTransforms(
                                Arrays.asList(
                                        config("plugin_name=sql, plugin_output=one"),
                                        config("plugin_name=sql, plugin_output=two")),
                                Collections.singleton("src")));
    }

    @Test
    void testFallbackAndDefaultBindingMatchLegacyQueuePolls() {
        String defaultId = org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;
        assertLegacyQueueSchedule(
                Arrays.asList(
                        config("plugin_name=sql, plugin_output=terminal"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=named")),
                Collections.singleton("src"));
        assertLegacyQueueSchedule(
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[], plugin_output=terminal"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=named")),
                Collections.singleton("src"));
        assertLegacyQueueSchedule(
                Arrays.asList(
                        config("plugin_name=sql"),
                        config("plugin_name=sql, plugin_output=named"),
                        config("plugin_name=sql")),
                Collections.singleton(defaultId));
    }

    @Test
    void testShuffledValidChainsKeepLegacyQueuePollIndexes() {
        Random random = new Random(12079L);
        for (int iteration = 0; iteration < 100; iteration++) {
            List<Config> transforms = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                transforms.add(
                        config(
                                "plugin_name=sql, plugin_input=["
                                        + (i == 0 ? "src" : "t" + random.nextInt(i))
                                        + "], plugin_output=t"
                                        + i));
            }
            Collections.shuffle(transforms, random);
            assertLegacyQueueSchedule(transforms, Collections.singleton("src"));
        }
    }

    private void assertLegacyQueueSchedule(List<Config> transforms, Set<String> sources) {
        List<TransformDependencyScheduler.ScheduledTransform> actual =
                TransformDependencyScheduler.scheduleTransforms(transforms, sources);
        Queue<Config> queue = new LinkedList<>(transforms);
        Set<String> available = new HashSet<>(sources);
        int poll = 0;
        int evaluated = 0;
        // Literal legacy retry rule, restricted to fixtures without partial multi-input resolution.
        while (!queue.isEmpty()) {
            Assertions.assertTrue(poll < 10000, "Legacy fixture must terminate");
            Config config = queue.poll();
            ReadonlyConfig readonly = ReadonlyConfig.fromConfig(config);
            boolean hasInput =
                    ConfigParserUtil.getInputIds(readonly).stream().anyMatch(available::contains);
            if (!hasInput && !queue.isEmpty()) {
                queue.offer(config);
            } else {
                TransformDependencyScheduler.ScheduledTransform scheduled = actual.get(evaluated++);
                Assertions.assertEquals(config, scheduled.getConfig());
                Assertions.assertEquals(poll, scheduled.getActionIndex());
                Assertions.assertEquals(!hasInput, scheduled.isLegacyFallback());
                available.add(scheduled.getOutputId());
            }
            poll++;
        }
        Assertions.assertEquals(evaluated, actual.size());
    }

    @Test
    void testDuplicateOutputsReleaseEachMissingInputOnlyOnce() {
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[src], plugin_output=dup"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=dup"),
                        config("plugin_name=sql, plugin_input=[dup,later], plugin_output=joined"),
                        config("plugin_name=sql, plugin_input=[src], plugin_output=later"));
        List<TransformDependencyScheduler.ScheduledTransform> scheduled =
                TransformDependencyScheduler.scheduleTransforms(
                        transforms, Collections.singleton("src"));
        Assertions.assertEquals(4, scheduled.size());
        Assertions.assertEquals("later", scheduled.get(2).getOutputId());
        Assertions.assertEquals("joined", scheduled.get(3).getOutputId());
        Assertions.assertEquals(4, scheduled.get(3).getActionIndex());
    }

    @Test
    void testTerminalOmittedInputRetainsLegacyFallback() {
        List<Config> transforms =
                Arrays.asList(
                        config("plugin_name=sql, plugin_input=[src], plugin_output=named"),
                        config("plugin_name=sql, plugin_output=terminal"));
        List<TransformDependencyScheduler.ScheduledTransform> scheduled =
                TransformDependencyScheduler.scheduleTransforms(
                        transforms, Collections.singleton("src"));
        Assertions.assertEquals(2, scheduled.size());
        Assertions.assertEquals("terminal", scheduled.get(1).getOutputId());
        Assertions.assertEquals(1, scheduled.get(1).getActionIndex());
    }

    @Test
    void testSingleExplicitSelfCycleCannotUseLegacyFallback() {
        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                TransformDependencyScheduler.scheduleTransforms(
                                        Collections.singletonList(
                                                config(
                                                        "plugin_name=sql, plugin_input=[self], plugin_output=self")),
                                        Collections.singleton("src")));
        Assertions.assertTrue(exception.getMessage().contains("self -> self"));
    }
}
