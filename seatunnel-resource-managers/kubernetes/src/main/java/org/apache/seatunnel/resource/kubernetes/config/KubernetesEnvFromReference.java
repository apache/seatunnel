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

package org.apache.seatunnel.resource.kubernetes.config;

import lombok.Getter;

import java.util.Objects;

/**
 * One {@code kubernetes.env-from} entry, equivalent to the Kubernetes {@code envFrom} field: every
 * key of the referenced Secret or ConfigMap is injected as an environment variable.
 */
@Getter
public final class KubernetesEnvFromReference {

    /** Referenced Kubernetes object kind. */
    public enum Source {
        SECRET,
        CONFIG_MAP
    }

    private final Source source;
    private final String name;

    private KubernetesEnvFromReference(Source source, String name) {
        this.source = Objects.requireNonNull(source, "source");
        this.name = Objects.requireNonNull(name, "name");
    }

    /** @return reference to an existing Secret */
    public static KubernetesEnvFromReference secret(String name) {
        return new KubernetesEnvFromReference(Source.SECRET, name);
    }

    /** @return reference to an existing ConfigMap */
    public static KubernetesEnvFromReference configMap(String name) {
        return new KubernetesEnvFromReference(Source.CONFIG_MAP, name);
    }
}
