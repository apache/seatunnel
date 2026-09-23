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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

/** Options for isolated, fixed-size Kubernetes applications. */
public final class KubernetesOptions {

    private KubernetesOptions() {}

    public static final Option<String> NAMESPACE =
            Options.key("kubernetes.namespace")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Existing Kubernetes namespace for all application resources.");
    public static final Option<String> IMAGE =
            Options.key("kubernetes.image")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Distribution image containing the Kubernetes resource manager and required connectors.");
    public static final Option<String> IMAGE_PULL_POLICY =
            Options.key("kubernetes.image-pull-policy")
                    .stringType()
                    .defaultValue("IfNotPresent")
                    .withDescription(
                            "Kubernetes image pull policy: Always, IfNotPresent or Never.");
    public static final Option<String> SERVICE_ACCOUNT =
            Options.key("kubernetes.service-account")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Existing service account authorized to create, inspect and delete worker pods.");
    public static final Option<String> SEATUNNEL_HOME =
            Options.key("kubernetes.seatunnel-home")
                    .stringType()
                    .defaultValue("/opt/seatunnel")
                    .withDescription("Absolute path of the distribution inside the image.");
    public static final Option<String> KUBE_CONFIG =
            Options.key("kubernetes.kubeconfig")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional local kubeconfig path; omitted in pods to use their service account.");
    public static final Option<Integer> RETENTION_SECONDS =
            Options.key("kubernetes.finished-job-retention-seconds")
                    .intType()
                    .defaultValue(86400)
                    .withDescription(
                            "Seconds to retain terminal Jobs and their configuration before Kubernetes TTL cleanup.");
    public static final Option<String> CHECKPOINT_PVC =
            Options.key("kubernetes.checkpoint-pvc")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing persistent volume claim mounted on the master at /opt/seatunnel/checkpoints; never deleted by application cleanup.");

    /**
     * Validates the deployment options before any Kubernetes resources are created.
     *
     * @param specification immutable application options to validate
     * @throws IllegalArgumentException if an image, container path, pull policy or claim is invalid
     */
    public static void validate(ApplicationSpecification specification) {
        String image = specification.getOption(IMAGE);
        if (image == null || image.trim().isEmpty()) {
            throw new IllegalArgumentException("kubernetes.image is required");
        }
        String home = specification.getOption(SEATUNNEL_HOME);
        if (home == null || !home.startsWith("/") || home.contains(":")) {
            throw new IllegalArgumentException(
                    "kubernetes.seatunnel-home must be an absolute path without ':'");
        }
        String policy = specification.getOption(IMAGE_PULL_POLICY);
        if (!"Always".equals(policy) && !"IfNotPresent".equals(policy) && !"Never".equals(policy)) {
            throw new IllegalArgumentException("Invalid kubernetes.image-pull-policy");
        }
        if (specification.getOption(RETENTION_SECONDS) < 1) {
            throw new IllegalArgumentException(
                    "kubernetes.finished-job-retention-seconds must be positive");
        }
        String claim = specification.getOption(CHECKPOINT_PVC);
        if (claim != null && claim.trim().isEmpty()) {
            throw new IllegalArgumentException("kubernetes.checkpoint-pvc must not be empty");
        }
    }
}
