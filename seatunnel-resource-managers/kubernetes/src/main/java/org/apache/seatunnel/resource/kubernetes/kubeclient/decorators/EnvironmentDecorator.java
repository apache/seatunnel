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

import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesEnvFromReference;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesEnvironmentVariable;

import io.kubernetes.client.openapi.models.V1ConfigMapEnvSource;
import io.kubernetes.client.openapi.models.V1ConfigMapKeySelector;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvFromSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1SecretEnvSource;
import io.kubernetes.client.openapi.models.V1SecretKeySelector;

/**
 * Injects the configured {@code kubernetes.env} and {@code kubernetes.env-from} entries into every
 * application container.
 *
 * <p>Kubernetes resolves Secret and ConfigMap references, so placeholders in the job configuration
 * are resolved inside the Pod even when the submitting host cannot see those values.
 */
public final class EnvironmentDecorator extends AbstractKubernetesStepDecorator {
    private final KubernetesApplicationConfig parameters;

    public EnvironmentDecorator(KubernetesApplicationConfig parameters) {
        this.parameters = parameters;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        V1Container container = pod.getSpec().getContainers().get(0);
        for (KubernetesEnvironmentVariable variable : parameters.getEnvironmentVariables()) {
            container.addEnvItem(environmentVariable(variable));
        }
        parameters
                .getEnvFromReferences()
                .forEach(reference -> container.addEnvFromItem(envFrom(reference)));
        return pod;
    }

    private static V1EnvVar environmentVariable(KubernetesEnvironmentVariable variable) {
        V1EnvVar environment = new V1EnvVar().name(variable.getName());
        if (variable.isSecret()) {
            return environment.valueFrom(
                    new V1EnvVarSource()
                            .secretKeyRef(
                                    new V1SecretKeySelector()
                                            .name(variable.getSecretName())
                                            .key(variable.getKey())
                                            .optional(false)));
        }
        if (variable.isConfigMap()) {
            return environment.valueFrom(
                    new V1EnvVarSource()
                            .configMapKeyRef(
                                    new V1ConfigMapKeySelector()
                                            .name(variable.getConfigMapName())
                                            .key(variable.getKey())
                                            .optional(false)));
        }
        return environment.value(variable.getLiteralValue());
    }

    private static V1EnvFromSource envFrom(KubernetesEnvFromReference reference) {
        if (reference.getSource() == KubernetesEnvFromReference.Source.SECRET) {
            return new V1EnvFromSource()
                    .secretRef(new V1SecretEnvSource().name(reference.getName()).optional(false));
        }
        return new V1EnvFromSource()
                .configMapRef(new V1ConfigMapEnvSource().name(reference.getName()).optional(false));
    }
}
