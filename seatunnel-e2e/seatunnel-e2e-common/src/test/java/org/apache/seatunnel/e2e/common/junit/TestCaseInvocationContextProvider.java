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

import static org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension.TEST_CONTAINERS_STORE_KEY;
import static org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension.TEST_EXTENDED_FACTORY_STORE_KEY;
import static org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension.TEST_RESOURCE_NAMESPACE;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class TestCaseInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        // Only support test cases with TestContainer as parameter
        Class<?>[] parameterTypes = context.getRequiredTestMethod().getParameterTypes();
        return parameterTypes.length == 1 && Arrays.stream(parameterTypes)
            .anyMatch(TestContainer.class::isAssignableFrom);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        List<TestContainer> testContainers = AnnotationUtil.filterDisabledContainers((List<TestContainer>) context.getStore(TEST_RESOURCE_NAMESPACE)
            .get(TEST_CONTAINERS_STORE_KEY), context.getRequiredTestMethod());

        ContainerExtendedFactory containerExtendedFactory = (ContainerExtendedFactory) context.getStore(TEST_RESOURCE_NAMESPACE)
            .get(TEST_EXTENDED_FACTORY_STORE_KEY);

        int containerAmount = testContainers.size();
        return testContainers.stream()
            .map(testContainer -> new TestResourceProvidingInvocationContext(testContainer, containerExtendedFactory, containerAmount));
    }

    static class TestResourceProvidingInvocationContext implements TestTemplateInvocationContext {
        private final TestContainer testContainer;
        private final ContainerExtendedFactory containerExtendedFactory;
        private final Integer containerAmount;

        public TestResourceProvidingInvocationContext(
            TestContainer testContainer,
            ContainerExtendedFactory containerExtendedFactory,
            int containerAmount) {
            this.testContainer = testContainer;
            this.containerExtendedFactory = containerExtendedFactory;
            this.containerAmount = containerAmount;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format("TestContainer(%s/%s): %s", invocationIndex, containerAmount, testContainer.identifier());
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                // Extension for injecting parameters
                new TestContainerResolver(testContainer, containerExtendedFactory),
                // Extension for closing test container
                (AfterTestExecutionCallback) ignore -> {
                    testContainer.tearDown();
                    log.info("The TestContainer[{}] is closed.", testContainer.identifier());
                });
        }
    }

    private static class TestContainerResolver implements ParameterResolver {

        private final TestContainer testContainer;
        private final ContainerExtendedFactory containerExtendedFactory;

        private TestContainerResolver(TestContainer testContainer,
                                      ContainerExtendedFactory containerExtendedFactory) {
            this.testContainer = testContainer;
            this.containerExtendedFactory = containerExtendedFactory;
        }

        @Override
        public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
            return TestContainer.class.isAssignableFrom(parameterContext.getParameter().getType());
        }

        @SneakyThrows
        @Override
        public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
            testContainer.startUp();
            testContainer.executeExtraCommands(containerExtendedFactory);
            log.info("The TestContainer[{}] is running.", testContainer.identifier());
            return this.testContainer;
        }
    }
}
