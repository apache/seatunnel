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

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1TCPSocketAction;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

/**
 * Creates the application's owner Job, including the complete master Pod template: container, JVM
 * command, pod-address environment, probes and the optional checkpoint claim.
 *
 * <p>This step starts the master list: it creates the resource every later step decorates and
 * rejects a Job passed in by an earlier step. The created Job is suspended, so the client can
 * create the accompanying resources before the master starts; the retention period removes the Job
 * after its terminal state.
 */
public final class JobDecorator extends AbstractWorkloadDecorator<KubernetesJob> {
    private final String applicationId;
    private final String mainClass;

    public JobDecorator(
            String applicationId, String mainClass, KubernetesApplicationConfig parameters) {
        super(parameters);
        this.applicationId = applicationId;
        this.mainClass = mainClass;
    }

    @Override
    public KubernetesJob decorate(KubernetesJob job) {
        if (job != null) {
            throw new IllegalStateException("The Job step must start the master decorator list");
        }
        V1PodSpec masterSpec = masterPod();
        V1ObjectMeta masterMetadata =
                KubernetesResourceIdentity.podMetadata(
                        applicationId,
                        KubernetesConstants.MASTER_ROLE,
                        parameters.getMasterLabels(),
                        parameters.getMasterAnnotations());
        V1PodTemplateSpec template =
                new V1PodTemplateSpec().metadata(masterMetadata).spec(masterSpec);
        V1JobSpec spec =
                new V1JobSpec()
                        .suspend(true)
                        .backoffLimit(0)
                        .completions(1)
                        .parallelism(1)
                        .ttlSecondsAfterFinished(parameters.getRetentionSeconds())
                        .template(template);
        V1ObjectMeta metadata =
                KubernetesResourceIdentity.metadata(
                        applicationId, applicationId, KubernetesConstants.MASTER_ROLE);
        return new KubernetesJob(
                new V1Job()
                        .apiVersion(KubernetesConstants.BATCH_API_VERSION)
                        .kind(KubernetesConstants.JOB_KIND)
                        .metadata(metadata)
                        .spec(spec));
    }

    /** Builds the master Pod: shared container and command plus its private probes and claim. */
    private V1PodSpec masterPod() {
        ApplicationSpecification specification = parameters.getSpecification();
        int memoryMb = specification.getMasterMemoryMb();
        V1Container container = container(memoryMb, specification.getMasterCpuCores());
        container.setCommand(
                command(
                        memoryMb,
                        parameters.getMasterHazelcastConfig(),
                        parameters.getMasterJvmOptionsFiles(),
                        mainClass,
                        applicationId));
        container.addEnvItem(masterHostEnv());
        addMasterProbes(container, specification);
        V1PodSpec pod = podSpec(container, true);
        String checkpointClaim = parameters.getCheckpointPvc();
        if (checkpointClaim != null) {
            container.addVolumeMountsItem(
                    new V1VolumeMount()
                            .name(KubernetesConstants.CHECKPOINT_VOLUME)
                            .mountPath(parameters.getCheckpointDirectory())
                            .readOnly(false));
            pod.addVolumesItem(
                    new V1Volume()
                            .name(KubernetesConstants.CHECKPOINT_VOLUME)
                            .persistentVolumeClaim(
                                    new V1PersistentVolumeClaimVolumeSource()
                                            .claimName(checkpointClaim)
                                            .readOnly(false)));
        }
        return pod;
    }

    /**
     * Exposes the Pod IP under a stable name so workers can build the advertised master address.
     */
    private V1EnvVar masterHostEnv() {
        return new V1EnvVar()
                .name(KubernetesConstants.MASTER_HOST_ENV)
                .valueFrom(
                        new V1EnvVarSource()
                                .fieldRef(
                                        new V1ObjectFieldSelector()
                                                .fieldPath(KubernetesConstants.POD_IP_FIELD_PATH)));
    }

    /**
     * Guards master startup with TCP probes on the Hazelcast port. Kubernetes restarts nothing
     * because the restart policy is {@code Never}, but the Job only reports completion after the
     * master has been reachable.
     */
    private void addMasterProbes(V1Container container, ApplicationSpecification specification) {
        int port = specification.getMasterPort();
        container.setStartupProbe(
                tcpProbe(port)
                        .periodSeconds(KubernetesConstants.STARTUP_PROBE_PERIOD_MILLIS / 1000)
                        .failureThreshold(
                                startupFailureThreshold(specification.getStartupTimeoutMillis())));
        container.setReadinessProbe(tcpProbe(port));
        container.setLivenessProbe(tcpProbe(port));
    }

    /** Maps the startup timeout to probe failures: {@code max(1, ceil(timeout / probePeriod))}. */
    private static int startupFailureThreshold(long startupTimeoutMillis) {
        return Math.max(
                1,
                (int)
                        Math.ceil(
                                startupTimeoutMillis
                                        / (double)
                                                KubernetesConstants.STARTUP_PROBE_PERIOD_MILLIS));
    }

    private static V1Probe tcpProbe(int port) {
        return new V1Probe()
                .tcpSocket(new V1TCPSocketAction().port(new IntOrString(port)))
                .timeoutSeconds(KubernetesConstants.PROBE_TIMEOUT_SECONDS)
                .periodSeconds(KubernetesConstants.PROBE_PERIOD_SECONDS)
                .failureThreshold(KubernetesConstants.PROBE_FAILURE_THRESHOLD);
    }
}
