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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Injects the localized application specification into the master container environment.
 *
 * <p>The specification, including the inline job configuration, is Base64 encoded in the {@link
 * KubernetesConstants#SPECIFICATION_ENV} environment variable, so submission never requires the
 * permission to create a Secret. A mounted {@code job.conf} from the configured application Secret
 * replaces the inline job configuration on the master. Only the master reads this specification, so
 * this decorator is part of the master decorator list and absent from the worker list.
 */
public final class SpecificationEnvDecorator extends AbstractKubernetesStepDecorator {
    /** Linux limits one environment string to 128 KiB; leave room for the launcher overhead. */
    private static final int MAX_SPECIFICATION_LENGTH = 96 * 1024;

    private final KubernetesApplicationConfig parameters;

    public SpecificationEnvDecorator(KubernetesApplicationConfig parameters) {
        this.parameters = parameters;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        V1Container master = pod.getSpec().getContainers().get(0);
        master.addEnvItem(
                new V1EnvVar().name(KubernetesConstants.SPECIFICATION_ENV).value(encode()));
        return pod;
    }

    private String encode() {
        try {
            StringWriter writer = new StringWriter();
            parameters.write(writer);
            String encoded =
                    Base64.getEncoder()
                            .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
            if (encoded.length() > MAX_SPECIFICATION_LENGTH) {
                throw new IllegalStateException(
                        "Localized application specification is too large for the Kubernetes env transport ("
                                + encoded.length()
                                + " characters, limit "
                                + MAX_SPECIFICATION_LENGTH
                                + "); move the job configuration into job.conf or reduce inline options");
            }
            return encoded;
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "Could not serialize the localized application specification", failure);
        }
    }
}
