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

package org.apache.seatunnel.resource.core.application;

import org.apache.seatunnel.engine.common.runtime.DeployType;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

/** Identity assigned by the external resource platform. */
@Getter
@EqualsAndHashCode
public final class ApplicationId {
    /** Platform namespace in which the identifier is meaningful. */
    private final DeployType deployType;
    /** Platform-assigned application ID or Kubernetes Job name. */
    private final String id;
    /**
     * Creates a platform-qualified identity without contacting the platform.
     *
     * @param deployType non-null platform namespace
     * @param id nonempty external identifier
     * @throws IllegalArgumentException if the external identifier is empty
     */
    public ApplicationId(DeployType deployType, String id) {
        this.deployType = Objects.requireNonNull(deployType, "deployType");
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Application ID must not be empty");
        }
        this.id = id;
    }

    @Override
    public String toString() {
        return deployType + ":" + id;
    }
}
