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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

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
                            "Distribution image containing the Kubernetes resource manager and "
                                    + "required connectors.");
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
                            "Comma-separated names of existing Secrets used to pull the "
                                    + "container image.");
    public static final Option<String> SERVICE_ACCOUNT =
            Options.key("kubernetes.service-account")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Existing service account authorized to create, inspect and delete "
                                    + "worker pods.");
    public static final Option<String> SEATUNNEL_HOME =
            Options.key("kubernetes.seatunnel-home")
                    .stringType()
                    .defaultValue("/opt/seatunnel")
                    .withDescription("Absolute path of the distribution inside the image.");
    public static final Option<String> SECRET_REF =
            Options.key("kubernetes.secret-ref")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing Secret managed outside SeaTunnel. It is mounted read-only "
                                    + "and never created or deleted; a job.conf key replaces the "
                                    + "inline job configuration.");
    public static final Option<String> SECRET_NAME =
            Options.key("kubernetes.secret.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the Secret SeaTunnel creates for the configured "
                                    + "kubernetes.secret.data entries; defaults to the application "
                                    + "Job name.");
    public static final Option<String> SECRET_DIRECTORY =
            Options.key("kubernetes.secret-directory")
                    .stringType()
                    .defaultValue(KubernetesConstants.APPLICATION_SECRET_DIRECTORY)
                    .withDescription(
                            "Absolute directory where the configured application Secret is "
                                    + "mounted read-only; a job.conf key there replaces the inline "
                                    + "job configuration.");
    public static final Option<String> CHECKPOINT_DIRECTORY =
            Options.key("kubernetes.checkpoint-directory")
                    .stringType()
                    .defaultValue(KubernetesConstants.CHECKPOINT_DIRECTORY)
                    .withDescription(
                            "Absolute container path where the existing checkpoint PVC is mounted "
                                    + "on the master.");
    public static final Option<String> JOB_CONFIG_FILE =
            Options.key("kubernetes.job-config-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional job configuration file that replaces the inline job "
                                    + "configuration before parsing: a file name under "
                                    + "<seatunnel-home>/config (ConfigMap-friendly) or an absolute "
                                    + "path such as <secret-directory>/job.conf; unset keeps the "
                                    + "submitted -c file.");
    public static final Option<String> MASTER_HAZELCAST_CONFIG =
            Options.key("kubernetes.master.hazelcast-config")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hazelcast member configuration file for the master, either a file "
                                    + "name under <seatunnel-home>/config or an absolute "
                                    + ".yaml/.yml/.xml path; unset keeps the built-in "
                                    + "configuration.");
    public static final Option<String> WORKER_HAZELCAST_CONFIG =
            Options.key("kubernetes.worker.hazelcast-config")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hazelcast member configuration file for workers, either a file "
                                    + "name under <seatunnel-home>/config or an absolute "
                                    + ".yaml/.yml/.xml path; unset keeps the built-in "
                                    + "configuration.");
    public static final Option<String> JVM_OPTIONS =
            Options.key("kubernetes.jvm-options-file")
                    .stringType()
                    .defaultValue(KubernetesConstants.JVM_OPTIONS_FILE)
                    .withDescription(
                            "Common JVM options file applied to master and workers before the "
                                    + "role-specific file: a file name under "
                                    + "<seatunnel-home>/config, an absolute path, or '"
                                    + KubernetesConstants.DISABLED
                                    + "' to pass no argument file.");
    public static final Option<String> MASTER_JVM_OPTIONS =
            Options.key("kubernetes.master.jvm-options-file")
                    .stringType()
                    .defaultValue(KubernetesConstants.MASTER_JVM_OPTIONS_FILE)
                    .withDescription(
                            "JVM options file for the master: a file name under "
                                    + "<seatunnel-home>/config or an absolute path, applied "
                                    + "through the Java @argfile syntax; set to '"
                                    + KubernetesConstants.DISABLED
                                    + "' to pass no argument file.");
    public static final Option<String> WORKER_JVM_OPTIONS =
            Options.key("kubernetes.worker.jvm-options-file")
                    .stringType()
                    .defaultValue(KubernetesConstants.WORKER_JVM_OPTIONS_FILE)
                    .withDescription(
                            "JVM options file for workers: a file name under "
                                    + "<seatunnel-home>/config or an absolute path, applied "
                                    + "through the Java @argfile syntax; set to '"
                                    + KubernetesConstants.DISABLED
                                    + "' to pass no argument file.");
    public static final Option<String> SEATUNNEL_CONFIG_FILE =
            Options.key("kubernetes.seatunnel-config-file")
                    .stringType()
                    .defaultValue(KubernetesConstants.SEATUNNEL_CONFIG_FILE)
                    .withDescription(
                            "SeaTunnel engine configuration file passed as -Dseatunnel.config: "
                                    + "a path relative to <seatunnel-home> or an absolute "
                                    + ".yaml/.yml path.");
    public static final Option<String> LOG4J_CONFIG_FILE =
            Options.key("kubernetes.log4j-config-file")
                    .stringType()
                    .defaultValue(KubernetesConstants.LOG4J_CONFIG_FILE)
                    .withDescription(
                            "Log4j2 configuration file passed as -Dlog4j2.configurationFile: "
                                    + "a file name under <seatunnel-home>/config or an absolute "
                                    + "path.");
    public static final Option<String> KUBE_CONFIG =
            Options.key("kubernetes.kubeconfig")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional local kubeconfig path; omitted in pods to use their "
                                    + "service account.");
    public static final Option<Integer> RETENTION_SECONDS =
            Options.key("kubernetes.finished-job-retention-seconds")
                    .intType()
                    .defaultValue(86400)
                    .withDescription(
                            "Seconds to retain terminal Jobs and their configuration before "
                                    + "Kubernetes TTL cleanup.");
    public static final Option<Long> ACTIVE_DEADLINE_SECONDS =
            Options.key("kubernetes.active-deadline-seconds")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute deadline in seconds, counted from Job creation, for the "
                                    + "whole application Job including master startup; unset keeps "
                                    + "the Job active until the job finishes or is canceled. Set "
                                    + "it for unattended batch runs so a master that never starts "
                                    + "cannot leave the Job active forever.");
    public static final Option<String> CHECKPOINT_PVC =
            Options.key("kubernetes.checkpoint-pvc")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing persistent volume claim mounted on the master at "
                                    + "/opt/seatunnel/checkpoints; never deleted by application "
                                    + "cleanup.");
    public static final Option<String> MASTER_LABELS =
            textOption(
                    "kubernetes.master.labels",
                    "Additional labels on the master Pod, formatted as "
                            + "comma-separated key:value pairs.");
    public static final Option<String> WORKER_LABELS =
            textOption(
                    "kubernetes.worker.labels",
                    "Additional labels on worker Pods, formatted as "
                            + "comma-separated key:value pairs.");
    public static final Option<String> MASTER_ANNOTATIONS =
            textOption(
                    "kubernetes.master.annotations",
                    "Annotations on the master Pod, formatted as comma-separated key:value pairs.");
    public static final Option<String> WORKER_ANNOTATIONS =
            textOption(
                    "kubernetes.worker.annotations",
                    "Annotations on worker Pods, formatted as comma-separated key:value pairs.");
    public static final Option<String> MASTER_NODE_SELECTOR =
            textOption(
                    "kubernetes.master.node-selector",
                    "Node selector for the master Pod, formatted as "
                            + "comma-separated key:value pairs.");
    public static final Option<String> WORKER_NODE_SELECTOR =
            textOption(
                    "kubernetes.worker.node-selector",
                    "Node selector for worker Pods, formatted as comma-separated key:value pairs.");
    public static final Option<String> MASTER_TOLERATIONS =
            textOption(
                    "kubernetes.master.tolerations",
                    "Tolerations for the master Pod, formatted as key[=value][:Effect] entries "
                            + "separated by ';', for example dedicated=seatunnel:NoSchedule.");
    public static final Option<String> WORKER_TOLERATIONS =
            textOption(
                    "kubernetes.worker.tolerations",
                    "Tolerations for worker Pods, formatted as key[=value][:Effect] entries "
                            + "separated by ';'.");

    private static Option<String> textOption(String key, String description) {
        return Options.key(key).stringType().defaultValue("").withDescription(description);
    }
}
