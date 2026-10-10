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
    public static final Option<String> IMAGE_PULL_SECRETS =
            Options.key("kubernetes.image-pull-secrets")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Comma-separated names of existing Secrets used to pull the container image.");
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
    public static final Option<String> CONFIG_MAP =
            Options.key("kubernetes.config-map")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing ConfigMap mounted read-only at the SeaTunnel configuration directory in master and worker pods.");
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
    public static final Option<String> MASTER_LABELS =
            keyValueOption(
                    "kubernetes.master.labels",
                    "Additional labels on the master Pod, formatted as comma-separated key:value pairs.");
    public static final Option<String> WORKER_LABELS =
            keyValueOption(
                    "kubernetes.worker.labels",
                    "Additional labels on worker Pods, formatted as comma-separated key:value pairs.");
    public static final Option<String> MASTER_ANNOTATIONS =
            keyValueOption(
                    "kubernetes.master.annotations",
                    "Annotations on the master Pod, formatted as comma-separated key:value pairs.");
    public static final Option<String> WORKER_ANNOTATIONS =
            keyValueOption(
                    "kubernetes.worker.annotations",
                    "Annotations on worker Pods, formatted as comma-separated key:value pairs.");
    public static final Option<String> MASTER_NODE_SELECTOR =
            keyValueOption(
                    "kubernetes.master.node-selector",
                    "Node selector for the master Pod, formatted as comma-separated key:value pairs.");
    public static final Option<String> WORKER_NODE_SELECTOR =
            keyValueOption(
                    "kubernetes.worker.node-selector",
                    "Node selector for worker Pods, formatted as comma-separated key:value pairs.");

    private static Option<String> keyValueOption(String key, String description) {
        return Options.key(key).stringType().defaultValue("").withDescription(description);
    }
}
