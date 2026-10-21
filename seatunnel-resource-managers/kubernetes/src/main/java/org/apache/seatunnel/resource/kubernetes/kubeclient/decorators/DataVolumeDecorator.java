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

package org.apache.seatunnel.resource.kubernetes.kubeclient.decorators;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

/**
 * Mounts a user-owned data PVC into a workload Pod.
 *
 * <p>The PVC can carry checkpoints, Hazelcast IMap state, or other shared runtime data. SeaTunnel
 * only references it; the PVC lifecycle stays with the user. The claim name is also used as the
 * volume name so the configuration has one source instead of a second fixed identifier.
 *
 * <p>The decorator is role-agnostic and can be reused by application mode when it needs a data
 * claim.
 */
public final class DataVolumeDecorator extends AbstractKubernetesStepDecorator {

    private final String claimName;
    private final String mountPath;

    public DataVolumeDecorator(String claimName, String mountPath) {
        this.claimName = claimName;
        this.mountPath = mountPath;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        if (claimName == null || claimName.trim().isEmpty()) {
            return pod;
        }
        String volumeName = claimName.trim();
        V1Container container = pod.getSpec().getContainers().get(0);
        container.addVolumeMountsItem(new V1VolumeMount().name(volumeName).mountPath(mountPath));
        pod.getSpec()
                .addVolumesItem(
                        new V1Volume()
                                .name(volumeName)
                                .persistentVolumeClaim(
                                        new V1PersistentVolumeClaimVolumeSource()
                                                .claimName(volumeName)));
        return pod;
    }
}
