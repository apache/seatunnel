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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.lineage.LineageConfig;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DetachedJobExecutionResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class FlinkLineageSupportTest {

    @Test
    void shouldNotParseInactiveLineageOptions() {
        Map<String, Object> jobOptions = new LinkedHashMap<>();
        jobOptions.put(LineageConfig.ENABLED, false);
        jobOptions.put(LineageConfig.TIMEOUT_MS, "not-a-number");

        LineageConfig config =
                FlinkLineageSupport.resolveConfig(
                        jobOptions,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());

        Assertions.assertFalse(config.enabled());
    }

    @Test
    void shouldResolveEnablementWithJobEnvironmentClusterPrecedence() {
        Map<String, Object> clusterOptions = Collections.singletonMap(LineageConfig.ENABLED, true);
        Map<String, Object> environment = Collections.singletonMap("OPENLINEAGE_ENABLED", false);

        LineageConfig environmentConfig =
                FlinkLineageSupport.resolveConfig(
                        Collections.<String, Object>emptyMap(), clusterOptions, environment);
        Assertions.assertFalse(environmentConfig.enabled());

        Map<String, Object> jobOptions = Collections.singletonMap(LineageConfig.ENABLED, true);
        LineageConfig jobConfig =
                FlinkLineageSupport.resolveConfig(jobOptions, clusterOptions, environment);
        Assertions.assertTrue(jobConfig.enabled());

        Map<String, Object> disabledJobOptions =
                Collections.singletonMap(LineageConfig.ENABLED, false);
        Assertions.assertFalse(
                FlinkLineageSupport.isLineageEnabled(
                        disabledJobOptions,
                        clusterOptions,
                        Collections.singletonMap("OPENLINEAGE_ENABLED", true)));
    }

    @Test
    void shouldRejectJobTokenEvenWhenLineageIsDisabled() {
        Map<String, Object> jobOptions = new LinkedHashMap<>();
        jobOptions.put(LineageConfig.AUTH_TOKEN, "job-token");
        jobOptions.put(LineageConfig.ENABLED, false);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlinkLineageSupport.resolveConfig(
                                jobOptions,
                                Collections.<String, Object>emptyMap(),
                                Collections.<String, Object>emptyMap()));
    }

    /**
     * This starter compiles against Flink 1.15, which has no {@code JobStatusHook}, so its {@code
     * FlinkLineageHooks} answers that lineage is unsupported and {@code FlinkExecution} warns
     * instead of reporting. The 1.20 starter overrides that class and asserts the opposite; the
     * registration path itself is covered there.
     */
    @Test
    void shouldReportTheStatusHookApiAsUnsupportedOnThisStarter() {
        Assertions.assertFalse(
                FlinkLineageSupport.isSupported(),
                "a starter compiled against Flink 1.15 cannot register a job status hook");
    }

    @Test
    void shouldReportStatisticsOnlyForAttachedExecutionResult() {
        JobExecutionResult attached =
                new JobExecutionResult(new JobID(), 1L, Collections.emptyMap());
        DetachedJobExecutionResult detached = new DetachedJobExecutionResult(new JobID());

        Assertions.assertTrue(FlinkLineageSupport.isAttachedJobExecutionResult(attached, null));
        Assertions.assertFalse(FlinkLineageSupport.isAttachedJobExecutionResult(detached, null));
        Assertions.assertFalse(
                FlinkLineageSupport.isAttachedJobExecutionResult(
                        attached, new IllegalStateException("job failed")));
        Assertions.assertFalse(FlinkLineageSupport.isAttachedJobExecutionResult(null, null));
    }

    /**
     * Reproduces the exception chain a real detached submission produces when the hook class is not
     * on the JobManager. Flink surfaces only a bare ClassNotFoundException from
     * JobSubmitHandler.loadJobGraph, which names the class but never mentions lineage or the fix.
     */
    @Test
    void explainsAJobManagerThatCannotLoadTheStatusHook() {
        String hookClass = FlinkLineageSupport.HOOK_HANDLER_CLASS;
        Exception submissionFailure =
                new RuntimeException(
                        "Failed to submit JobGraph.",
                        new RuntimeException(
                                "Failed to deserialize JobGraph.",
                                new ClassNotFoundException(hookClass)));

        String hint = FlinkLineageSupport.describeMissingHookClass(submissionFailure);

        Assertions.assertNotNull(hint);
        Assertions.assertTrue(hint.contains("$FLINK_HOME/lib"), "must say where to install it");
        Assertions.assertTrue(
                hint.contains("seatunnel-lineage-flink"), "must name the artifact to install");
        Assertions.assertTrue(
                hint.contains("openlineage_enabled=false"),
                "must offer a way to submit without it");
        Assertions.assertTrue(hint.contains(hookClass), "must name the class Flink could not load");
    }

    /**
     * The deployment this hint exists for is a session cluster submitted over REST, where the
     * JobManager-side failure never crosses the wire as an exception object: the client sees a REST
     * exception whose message carries the server-side stack trace as text. Matching on the
     * exception type alone would leave exactly that deployment with the bare submission error.
     */
    @Test
    void explainsAMissingHookRelayedAsTextAcrossTheRestBoundary() {
        String hookClass = FlinkLineageSupport.HOOK_HANDLER_CLASS;
        Exception submissionFailure =
                new RuntimeException(
                        "Failed to submit job.",
                        new IllegalStateException(
                                "[Internal server error., <Exception on server side:"
                                        + " org.apache.flink.runtime.rest.handler.RestHandlerException:"
                                        + " Could not deserialize JobGraph."
                                        + " Caused by: java.lang.ClassNotFoundException: "
                                        + hookClass
                                        + ">]"));

        String hint = FlinkLineageSupport.describeMissingHookClass(submissionFailure);

        Assertions.assertNotNull(hint, "a class-loading failure relayed as text must be explained");
        Assertions.assertTrue(hint.contains("$FLINK_HOME/lib"), "must say where to install it");
        Assertions.assertTrue(hint.contains(hookClass), "must name the class Flink could not load");
    }

    @Test
    void leavesUnrelatedSubmissionFailuresUntouched() {
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new RuntimeException(
                                "Failed to submit JobGraph.",
                                new ClassNotFoundException("com.example.SomeConnector"))));
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new IllegalStateException("Recovery is suppressed")));
        Assertions.assertNull(FlinkLineageSupport.describeMissingHookClass(null));
        // A relayed trace can name several failures at once. The hook class must be the one that
        // could not be loaded; a different missing class in the same text is a different problem,
        // and the caller reports this hint as the top-level failure message.
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new IllegalStateException(
                                "[Internal server error., <Exception on server side:"
                                        + " java.lang.ClassNotFoundException:"
                                        + " com.example.SomeConnector at"
                                        + " JobSubmitHandler.loadJobGraph; JobGraph hooks: ["
                                        + FlinkLineageSupport.HOOK_HANDLER_CLASS
                                        + "]>]")));
        // Naming the hook class is not enough on its own: only a class-loading failure means the
        // artifact is missing from the JobManager.
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new IllegalStateException(
                                "Could not serialize "
                                        + FlinkLineageSupport.HOOK_HANDLER_CLASS
                                        + ": java.io.NotSerializableException")));
    }
}
