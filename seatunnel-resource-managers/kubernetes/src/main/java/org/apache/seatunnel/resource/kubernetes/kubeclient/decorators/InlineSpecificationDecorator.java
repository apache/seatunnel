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

import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Carries the localized application specification, including the inline job configuration, in the
 * master container environment.
 *
 * <p>The value is a Base64 encoded properties document, so submission never requires the permission
 * to create a Secret. A mounted {@code job.conf} from the configured application Secret replaces
 * the inline job configuration on the master.
 */
public final class InlineSpecificationDecorator extends AbstractKubernetesStepDecorator {
    private final KubernetesApplicationParameters parameters;

    public InlineSpecificationDecorator(KubernetesApplicationParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        V1Container master =
                job.getInternalResource().getSpec().getTemplate().getSpec().getContainers().get(0);
        master.addEnvItem(
                new V1EnvVar().name(KubernetesConstants.SPECIFICATION_ENV).value(encode()));
        return job;
    }

    private String encode() {
        try {
            StringWriter writer = new StringWriter();
            parameters.write(writer);
            return Base64.getEncoder()
                    .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "Could not serialize the localized application specification", failure);
        }
    }
}
