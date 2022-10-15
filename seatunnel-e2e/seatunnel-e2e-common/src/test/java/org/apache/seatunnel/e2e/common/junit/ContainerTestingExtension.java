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

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;

public class ContainerTestingExtension implements BeforeAllCallback, AfterAllCallback {
    public static final ExtensionContext.Namespace TEST_RESOURCE_NAMESPACE =
        ExtensionContext.Namespace.create("testResourceNamespace");
    public static final String TEST_CONTAINERS_STORE_KEY = "testContainers";
    public static final String TEST_EXTENDED_FACTORY_STORE_KEY = "testContainerExtendedFactory";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        List<ContainerExtendedFactory> containerExtendedFactories =
            AnnotationSupport.findAnnotatedFieldValues(
                context.getRequiredTestInstance(),
                TestContainerExtension.class,
                ContainerExtendedFactory.class);
        checkAtMostOneAnnotationField(containerExtendedFactories, TestContainerExtension.class);
        ContainerExtendedFactory containerExtendedFactory = container -> {};
        if (!containerExtendedFactories.isEmpty()) {
            containerExtendedFactory = containerExtendedFactories.get(0);
        }
        context.getStore(TEST_RESOURCE_NAMESPACE)
            .put(TEST_EXTENDED_FACTORY_STORE_KEY, containerExtendedFactory);

        List<TestContainersFactory> containersFactories = AnnotationSupport.findAnnotatedFieldValues(
            context.getRequiredTestInstance(),
            TestContainers.class,
            TestContainersFactory.class);

        checkExactlyOneAnnotatedField(containersFactories, TestContainers.class);

        List<TestContainer> testContainers = AnnotationUtil.filterDisabledContainers(containersFactories.get(0).create(),
            context.getRequiredTestInstance().getClass());
        context.getStore(TEST_RESOURCE_NAMESPACE)
            .put(TEST_CONTAINERS_STORE_KEY, testContainers);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(TEST_CONTAINERS_STORE_KEY);
    }

    private void checkExactlyOneAnnotatedField(
        Collection<?> fields, Class<? extends Annotation> annotation) {
        checkAtMostOneAnnotationField(fields, annotation);
        checkAtLeastOneAnnotationField(fields, annotation);
    }

    private void checkAtLeastOneAnnotationField(
        Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                String.format(
                    "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }

    private void checkAtMostOneAnnotationField(
        Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.size() > 1) {
            throw new IllegalStateException(
                String.format(
                    "Multiple fields are annotated with '@%s'",
                    annotation.getSimpleName()));
        }
    }
}
