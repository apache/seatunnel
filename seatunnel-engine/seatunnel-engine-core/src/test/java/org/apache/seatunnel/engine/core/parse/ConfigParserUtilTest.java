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

import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

        List<MultipleTableJobConfigParser.ScheduledTransform> scheduled =
                MultipleTableJobConfigParser.scheduleTransforms(
                        transforms, Collections.singleton("src"));

        Assertions.assertEquals(transformCount, scheduled.size());
        Set<String> scheduledOutputs = new HashSet<>();
        int expectedActionIndex = -1;
        for (int evaluationIndex = 0; evaluationIndex < transformCount; evaluationIndex++) {
            MultipleTableJobConfigParser.ScheduledTransform transform =
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
                MultipleTableJobConfigParser.nextTransformActionIndex(Integer.MAX_VALUE - 1L, 1));

        JobDefineCheckException upperBoundException =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                MultipleTableJobConfigParser.nextTransformActionIndex(
                                        Integer.MAX_VALUE, 1));
        Assertions.assertTrue(
                upperBoundException.getMessage().contains("outside the supported range"));
        Assertions.assertThrows(
                JobDefineCheckException.class,
                () -> MultipleTableJobConfigParser.nextTransformActionIndex(-2L, 1));
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
                                MultipleTableJobConfigParser.scheduleTransforms(
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
                        MultipleTableJobConfigParser.scheduleTransforms(
                                multipleEmptyInputs, Collections.singleton("src")));
        Assertions.assertThrows(
                JobDefineCheckException.class,
                () ->
                        MultipleTableJobConfigParser.scheduleTransforms(
                                emptyInputWithDependent, Collections.singleton("src")));
    }

    private static Config config(String value) {
        return ConfigFactory.parseString("{" + value + "}");
    }
}
