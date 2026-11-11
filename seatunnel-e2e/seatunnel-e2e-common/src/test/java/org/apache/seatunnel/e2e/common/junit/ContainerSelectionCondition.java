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

package org.apache.seatunnel.e2e.common.junit;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Skips a suite or test method when every container selected by {@code RUN_ENGINES} is excluded by
 * {@link DisabledOnContainer}.
 *
 * <p>JUnit requires a {@code TestTemplateInvocationContextProvider} to provide a non-empty stream.
 * Without this condition, a suite disabled for the only engine selected for a pull request produces
 * an empty stream and fails with {@code PreconditionViolationException} instead of being skipped.
 */
@Slf4j
public class ContainerSelectionCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        List<TestContainer> discoveredContainers = ContainerUtil.discoverTestContainers();
        if (shouldDisable(
                discoveredContainers,
                context.getRequiredTestClass(),
                context.getTestMethod().orElse(null))) {
            String reason =
                    String.format(
                            "All test containers selected by RUN_ENGINES are disabled for %s.",
                            context.getDisplayName());
            log.info(reason);
            return ConditionEvaluationResult.disabled(reason);
        }
        return ConditionEvaluationResult.enabled(
                "At least one applicable test container is available.");
    }

    /**
     * Decides whether the class or method must be skipped.
     *
     * <p>An empty discovered list keeps the suite enabled so that a real container discovery
     * failure stays visible instead of being silently skipped.
     */
    static boolean shouldDisable(
            List<TestContainer> discoveredContainers, Class<?> testClass, Method testMethod) {
        if (discoveredContainers.isEmpty()) {
            return false;
        }
        List<TestContainer> applicableContainers =
                AnnotationUtil.filterDisabledContainers(discoveredContainers, testClass);
        if (testMethod != null) {
            applicableContainers =
                    AnnotationUtil.filterDisabledContainers(applicableContainers, testMethod);
        }
        return applicableContainers.isEmpty();
    }
}
