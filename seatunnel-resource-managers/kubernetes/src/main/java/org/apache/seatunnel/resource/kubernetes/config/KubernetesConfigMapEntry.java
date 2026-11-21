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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One entry of the {@code kubernetes.configmap} block.
 *
 * <p>An entry either references an existing ConfigMap through {@code ref} and mounts declared keys,
 * or declares {@code data} entries that SeaTunnel writes into a ConfigMap it creates for the
 * application. Every mounted key declares its full container path through {@code mount}.
 */
@Getter
public final class KubernetesConfigMapEntry {
    private final String entryName;
    private final String reference;
    private final String name;
    private final Map<String, String> data;
    private final Map<String, String> mountPaths;

    private KubernetesConfigMapEntry(
            String entryName,
            String reference,
            String name,
            Map<String, String> data,
            Map<String, String> mountPaths) {
        this.entryName = Objects.requireNonNull(entryName, "entryName");
        this.reference = reference;
        this.name = name;
        this.data = Collections.unmodifiableMap(new LinkedHashMap<>(data));
        this.mountPaths = Collections.unmodifiableMap(new LinkedHashMap<>(mountPaths));
    }

    /** @return entry referencing an existing ConfigMap, never created or deleted by SeaTunnel */
    public static KubernetesConfigMapEntry reference(
            String entryName, String reference, Map<String, String> mountPaths) {
        return new KubernetesConfigMapEntry(
                entryName, reference, null, Collections.emptyMap(), mountPaths);
    }

    /** @return entry whose data entries SeaTunnel writes into a ConfigMap it creates and owns */
    public static KubernetesConfigMapEntry create(
            String entryName,
            String name,
            Map<String, String> data,
            Map<String, String> mountPaths) {
        return new KubernetesConfigMapEntry(entryName, null, name, data, mountPaths);
    }

    /** @return whether this entry only references an existing ConfigMap */
    public boolean isReference() {
        return reference != null;
    }

    /**
     * @return mount path for one key, or {@code null} to use the default configuration directory
     */
    public String mountPath(String key) {
        return mountPaths.get(key);
    }

    /**
     * Resolves the name of the ConfigMap SeaTunnel creates.
     *
     * @param applicationId generated application Job name used when no name is configured
     * @return configured name, or {@code <application-id>-<entry>}
     */
    public String createdName(String applicationId) {
        return name == null ? applicationId + "-" + entryName : name;
    }
}
