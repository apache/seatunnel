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

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.junit.platform.commons.util.AnnotationUtils;

import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AnnotationUtil {

    public static List<TestContainer> filterDisabledContainers(List<TestContainer> containers, AnnotatedElement annotatedElement) {
        // Filters disabled containers
        final List<TestContainerId> disabledContainers = new ArrayList<>();
        final List<EngineType> disabledEngineTypes = new ArrayList<>();
        AnnotationUtils.findAnnotation(annotatedElement, DisabledOnContainer.class)
            .ifPresent(annotation -> {
                Collections.addAll(disabledContainers, annotation.value());
                Collections.addAll(disabledEngineTypes, annotation.type());
            });
        return containers.stream()
            .filter(container -> !disabledContainers.contains(container.identifier()))
            .filter(container -> !disabledEngineTypes.contains(container.identifier().getEngineType()))
            .collect(Collectors.toList());
    }
}
