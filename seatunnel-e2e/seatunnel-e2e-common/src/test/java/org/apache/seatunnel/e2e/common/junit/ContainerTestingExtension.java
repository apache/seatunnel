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
import org.apache.seatunnel.e2e.common.container.ReusableTestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContainerTestingExtension implements BeforeAllCallback, AfterAllCallback {
    public static final ExtensionContext.Namespace TEST_RESOURCE_NAMESPACE =
            ExtensionContext.Namespace.create("testResourceNamespace");
    private static final ExtensionContext.Namespace SHARED_CONTAINER_NAMESPACE =
            ExtensionContext.Namespace.create(ContainerTestingExtension.class);
    public static final String TEST_CONTAINERS_STORE_KEY = "testContainers";
    public static final String TEST_EXTENDED_FACTORY_STORE_KEY = "testContainerExtendedFactory";
    public static final String SHARED_CONTAINER_IDS_STORE_KEY = "sharedContainerIds";
    private static final String SHARED_CONTAINER_RESOURCES_STORE_KEY = "sharedContainerResources";

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

        List<TestContainersFactory> containersFactories =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(),
                        TestContainers.class,
                        TestContainersFactory.class);

        checkExactlyOneAnnotatedField(containersFactories, TestContainers.class);

        List<TestContainer> testContainers =
                AnnotationUtil.filterDisabledContainers(
                        containersFactories.get(0).create(),
                        context.getRequiredTestInstance().getClass());
        Set<TestContainerId> sharedContainerIds = getSharedContainerIds(context);
        List<SharedTestContainerResource> sharedResources = new ArrayList<>();
        try {
            // Keep the factory-defined order so classes sharing multiple containers acquire their
            // leases consistently and cannot create a lock-ordering cycle.
            for (int i = 0; i < testContainers.size(); i++) {
                TestContainer testContainer = testContainers.get(i);
                if (!sharedContainerIds.contains(testContainer.identifier())) {
                    continue;
                }
                if (!(testContainer instanceof ReusableTestContainer)) {
                    throw new IllegalStateException(
                            String.format(
                                    "TestContainer[%s] does not support reuse",
                                    testContainer.identifier()));
                }
                SharedTestContainerResource sharedResource =
                        context.getRoot()
                                .getStore(SHARED_CONTAINER_NAMESPACE)
                                .getOrComputeIfAbsent(
                                        testContainer.getClass().getName()
                                                + ":"
                                                + testContainer.identifier(),
                                        ignored ->
                                                new SharedTestContainerResource(
                                                        (ReusableTestContainer) testContainer),
                                        SharedTestContainerResource.class);
                testContainers.set(i, sharedResource.acquire(containerExtendedFactory));
                sharedResources.add(sharedResource);
            }
        } catch (Exception acquireFailure) {
            for (int i = sharedResources.size() - 1; i >= 0; i--) {
                try {
                    sharedResources.get(i).release();
                } catch (Exception releaseFailure) {
                    acquireFailure.addSuppressed(releaseFailure);
                }
            }
            throw acquireFailure;
        }
        context.getStore(TEST_RESOURCE_NAMESPACE).put(TEST_CONTAINERS_STORE_KEY, testContainers);
        context.getStore(TEST_RESOURCE_NAMESPACE)
                .put(SHARED_CONTAINER_IDS_STORE_KEY, sharedContainerIds);
        context.getStore(TEST_RESOURCE_NAMESPACE)
                .put(SHARED_CONTAINER_RESOURCES_STORE_KEY, sharedResources);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        ExtensionContext.Store store = context.getStore(TEST_RESOURCE_NAMESPACE);
        @SuppressWarnings("unchecked")
        List<SharedTestContainerResource> sharedResources =
                (List<SharedTestContainerResource>)
                        store.remove(SHARED_CONTAINER_RESOURCES_STORE_KEY);
        Exception cleanupFailure = null;
        try {
            if (sharedResources != null) {
                for (SharedTestContainerResource sharedResource : sharedResources) {
                    try {
                        sharedResource.release();
                    } catch (Exception e) {
                        if (cleanupFailure == null) {
                            cleanupFailure = e;
                        } else {
                            cleanupFailure.addSuppressed(e);
                        }
                    }
                }
            }
        } finally {
            store.remove(TEST_CONTAINERS_STORE_KEY);
            store.remove(SHARED_CONTAINER_IDS_STORE_KEY);
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private Set<TestContainerId> getSharedContainerIds(ExtensionContext context) {
        return AnnotationSupport.findAnnotation(
                        context.getRequiredTestClass(), ReuseTestContainers.class)
                .map(annotation -> new HashSet<>(Arrays.asList(annotation.value())))
                .orElseGet(HashSet::new);
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
