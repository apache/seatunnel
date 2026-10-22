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

package org.apache.seatunnel.resource.kubernetes.kubeclient.factory;

import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesHaApplication;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SpecificationEnvDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesStatefulSet;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** The owner observes one native job; Kubernetes maintains compute and Zeta recovers it. */
public final class HaApplicationFactory extends AbstractWorkloadFactory {
    public static final String OWNER_ROLE = "owner";
    public static final String OWNER_UID_ENV = "SEATUNNEL_APPLICATION_UID";

    private HaApplicationFactory(KubernetesApplicationConfig parameters) {
        super(parameters);
    }

    /** Stable internal peer names are independent of Pod addresses and the elected master. */
    public static List<String> masterAddresses(String id, KubernetesApplicationConfig parameters) {
        return IntStream.range(0, parameters.getMasterReplicas())
                .mapToObj(
                        index ->
                                id
                                        + "-master-"
                                        + index
                                        + "."
                                        + id
                                        + "-discovery."
                                        + parameters.getNamespace()
                                        + ".svc:"
                                        + parameters.getSpecification().getMasterPort())
                .collect(Collectors.toList());
    }

    public static List<KubernetesResource<?>> build(
            String id, KubernetesApplicationConfig parameters) {
        return new HaApplicationFactory(parameters).assemble(id);
    }

    private List<KubernetesResource<?>> assemble(String id) {
        List<KubernetesResource<?>> resources = new ArrayList<>();
        V1Pod master =
                ApplicationJobFactory.createMasterPod(
                        id, SeatunnelKubernetesHaApplication.class.getName(), parameters);
        master.getSpec().getContainers().get(0).getCommand().add("master");
        List<KubernetesStepDecorator> common =
                Arrays.asList(
                        new EnvironmentDecorator(parameters),
                        new SecretDecorator(id, parameters),
                        new ConfigMapDecorator(id, parameters));
        for (KubernetesStepDecorator decorator : common) {
            master = decorator.decorate(master);
            resources.addAll(decorator.build());
        }
        master = new SpecificationEnvDecorator(parameters).decorate(master);

        master.getSpec().setRestartPolicy("Always");
        // Active masters use the existing Kubernetes driver and guarded submission policy.
        master.getSpec().setAutomountServiceAccountToken(true);
        resources.add(pool(id, "master", parameters.getMasterReplicas(), master));

        resources.add(service(id, id, false));
        resources.add(service(id, id + "-discovery", true));

        String encoded =
                master.getSpec().getContainers().get(0).getEnv().stream()
                        .filter(env -> KubernetesConstants.SPECIFICATION_ENV.equals(env.getName()))
                        .findFirst()
                        .get()
                        .getValue();
        V1Container ownerContainer =
                container(128, 1)
                        .resources(
                                container(128, 1)
                                        .getResources()
                                        .putRequestsItem(
                                                KubernetesConstants.CPU_RESOURCE,
                                                io.kubernetes.client.custom.Quantity.fromString(
                                                        "100m")));
        ownerContainer.setCommand(
                command(
                        128,
                        null,
                        java.util.Collections.emptyList(),
                        SeatunnelKubernetesHaApplication.class.getName(),
                        id,
                        OWNER_ROLE,
                        parameters.getNamespace(),
                        Long.toString(parameters.getSpecification().getJobId()),
                        Long.toString(parameters.getSpecification().getStartupTimeoutMillis()),
                        HaApplicationState.digest(encoded)));
        ownerContainer
                .getCommand()
                .removeIf(
                        argument ->
                                argument.startsWith(KubernetesConstants.SYSPROP_SEATUNNEL_CONFIG)
                                        || argument.startsWith(
                                                KubernetesConstants.SYSPROP_LOG4J_CONFIG));
        V1Pod observer =
                new V1Pod()
                        .apiVersion("v1")
                        .kind("Pod")
                        .metadata(
                                KubernetesResourceIdentity.podMetadata(
                                        id,
                                        OWNER_ROLE,
                                        parameters.getMasterLabels(),
                                        parameters.getMasterAnnotations()))
                        .spec(podSpec(ownerContainer, true));
        ownerContainer.addEnvItem(
                new V1EnvVar()
                        .name(OWNER_UID_ENV)
                        .valueFrom(
                                new V1EnvVarSource()
                                        .fieldRef(
                                                new V1ObjectFieldSelector()
                                                        .fieldPath(
                                                                "metadata.labels['batch.kubernetes.io/controller-uid']"))));
        KubernetesJob owner = ApplicationJobFactory.createOwnerJob(id, parameters, observer);
        owner.getInternalResource()
                .getMetadata()
                .setLabels(KubernetesResourceIdentity.metadata(id, id, OWNER_ROLE).getLabels());
        owner.getInternalResource().getSpec().setBackoffLimit(6);
        owner.getInternalResource()
                .getMetadata()
                .setAnnotations(
                        HaApplicationState.initial(
                                encoded, parameters.getSpecification().getJobId()));
        resources.add(0, owner);
        return resources;
    }

    private KubernetesStatefulSet pool(String id, String role, int replicas, V1Pod pod) {
        // Do not propagate the owner Job's reference to a Pod template: the StatefulSet controls
        // it.
        pod.getMetadata().setOwnerReferences(null);
        return new KubernetesStatefulSet(
                new V1StatefulSet()
                        .apiVersion("apps/v1")
                        .kind("StatefulSet")
                        .metadata(KubernetesResourceIdentity.metadata(id, id + "-" + role, role))
                        .spec(
                                new V1StatefulSetSpec()
                                        .replicas(replicas)
                                        .podManagementPolicy("Parallel")
                                        .serviceName(id + "-discovery")
                                        .selector(
                                                new V1LabelSelector()
                                                        .matchLabels(
                                                                KubernetesResourceIdentity.metadata(
                                                                                id, id, role)
                                                                        .getLabels()))
                                        .template(
                                                new V1PodTemplateSpec()
                                                        .metadata(pod.getMetadata())
                                                        .spec(pod.getSpec()))));
    }

    private KubernetesService service(String id, String name, boolean headless) {
        int port = parameters.getSpecification().getMasterPort();
        V1ServiceSpec spec =
                new V1ServiceSpec()
                        .selector(KubernetesResourceIdentity.metadata(id, id, "master").getLabels())
                        .addPortsItem(
                                new V1ServicePort()
                                        .name(KubernetesConstants.HAZELCAST_PORT_NAME)
                                        .port(port)
                                        .targetPort(new IntOrString(port)));
        if (headless) {
            spec.clusterIP("None").publishNotReadyAddresses(true);
        }
        return new KubernetesService(
                new V1Service()
                        .apiVersion("v1")
                        .kind("Service")
                        .metadata(KubernetesResourceIdentity.metadata(id, name, "master"))
                        .spec(spec));
    }
}
