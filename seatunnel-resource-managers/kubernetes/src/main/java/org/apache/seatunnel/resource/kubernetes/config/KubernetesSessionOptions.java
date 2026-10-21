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

/**
 * Session-only options for one Kubernetes Session Cluster.
 *
 * <p>Shared Kubernetes settings such as image, service account, image pull secrets, labels,
 * annotations, node selector, tolerations, Secret mounts and environment variables come from {@link
 * KubernetesOptions}. This class only declares the Session policy and the worker runtime fields
 * that application mode does not have: the worker pool bounds, the worker ConfigMap and the worker
 * JVM resource envelope.
 */
public final class KubernetesSessionOptions {

    private KubernetesSessionOptions() {}

    public static final Option<String> NAME =
            Options.key("session.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Session name used in Kubernetes labels and log messages.");

    public static final Option<String> CLUSTER_NAME =
            Options.key("session.cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hazelcast cluster name shared by the masters and workers; defaults "
                                    + "to seatunnel-session-<name>.");

    public static final Option<String> ROOT_CONFIG_MAP =
            Options.key("session.root-config-map")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing ConfigMap whose UID owns the worker Pods; defaults to "
                                    + "seatunnel-session-<name>-meta.");

    public static final Option<String> DATA_CLAIM =
            Options.key("session.data-claim")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing PersistentVolumeClaim mounted at the shared data "
                                    + "directory on masters and workers.");

    public static final Option<String> DATA_MOUNT =
            Options.key("session.data-mount")
                    .stringType()
                    .defaultValue("/opt/seatunnel/data")
                    .withDescription(
                            "Absolute worker container path where session.data-claim is "
                                    + "mounted; keep the master StatefulSet mount aligned.");

    public static final Option<Integer> MIN_WORKERS =
            Options.key("session.worker.min-workers")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Workers created when the Session starts.");

    public static final Option<Integer> MAX_WORKERS =
            Options.key("session.worker.max-workers")
                    .intType()
                    .defaultValue(32)
                    .withDescription("Upper bound for automatically provisioned workers.");

    public static final Option<String> WORKER_CONFIG_MAP =
            Options.key("session.worker.config-map")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Existing ConfigMap containing seatunnel.yaml, "
                                    + "hazelcast-worker.yaml, log4j2.properties and "
                                    + "jvm_worker_options; defaults to "
                                    + "seatunnel-session-<name>-config.");

    public static final Option<Integer> WORKER_CPU_CORES =
            Options.key("session.worker.cpu-cores")
                    .intType()
                    .defaultValue(1)
                    .withDescription("CPU cores requested for each worker.");

    public static final Option<Integer> WORKER_MEMORY_MB =
            Options.key("session.worker.memory-mb")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Total memory in MiB requested for each worker container.");

    public static final Option<Integer> WORKER_HEAP_MB =
            Options.key("session.worker.heap-mb")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "JVM heap in MiB; defaults to three quarters of "
                                    + "session.worker.memory-mb.");

    public static final Option<String> WORKER_JVM_OPTIONS =
            Options.key("session.worker.jvm-options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Additional JVM options passed by the worker entrypoint; defaults "
                                    + "to -Xms<heap>m -Xmx<heap>m.");

    public static final Option<Boolean> DYNAMIC_WORKER_ENABLED =
            Options.key("session.dynamic-worker.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether pending job slots may provision additional workers up to "
                                    + "max-workers.");

    public static final Option<Long> STARTUP_TIMEOUT_MILLIS =
            Options.key("session.dynamic-worker.startup-timeout-millis")
                    .longType()
                    .defaultValue(120000L)
                    .withDescription("Maximum wait for newly requested workers to register.");

    public static final Option<Long> RETRY_INTERVAL_MILLIS =
            Options.key("session.dynamic-worker.retry-interval-millis")
                    .longType()
                    .defaultValue(3000L)
                    .withDescription("Delay between worker registration checks.");
}
