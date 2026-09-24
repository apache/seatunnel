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

/** Stable Kubernetes names and field values shared by application resource factories. */
final class KubernetesConstants {
    /** API group versions used by core and batch resources. */
    static final String CORE_API_VERSION = "v1";

    static final String BATCH_API_VERSION = "batch/v1";

    /** Kubernetes resource kinds used in models and owner references. */
    static final String JOB_KIND = "Job";

    static final String POD_KIND = "Pod";
    static final String SECRET_KIND = "Secret";
    static final String SERVICE_KIND = "Service";

    /** Secret type used for the credential-bearing application specification. */
    static final String OPAQUE_SECRET_TYPE = "Opaque";

    /** Labels grouping all objects for an application and distinguishing their roles. */
    static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";

    static final String ROLE_LABEL = "seatunnel.apache.org/role";
    static final String MASTER_ROLE = "master";
    static final String WORKER_ROLE = "worker";
    static final String CONFIGURATION_ROLE = "configuration";

    /** Mounted application file and stable pod volume names. */
    static final String SPECIFICATION_FILE = "application.properties";

    static final String APPLICATION_VOLUME = "application";
    static final String CONFIG_VOLUME = "seatunnel-config";
    static final String CHECKPOINT_VOLUME = "checkpoints";

    /** Main container identity, paths, and environment exposed to the application entrypoint. */
    static final String CONTAINER_NAME = "seatunnel";

    static final String CONFIG_DIRECTORY = "/etc/seatunnel-application";
    static final String CHECKPOINT_DIRECTORY = "/opt/seatunnel/checkpoints";
    static final String MASTER_HOST_ENV = "SEATUNNEL_APPLICATION_MASTER_HOST";
    static final String SEATUNNEL_HOME_ENV = "SEATUNNEL_HOME";

    /** Kubernetes resource keys and units used for pod requests and limits. */
    static final String MEMORY_RESOURCE = "memory";

    static final String CPU_RESOURCE = "cpu";
    static final String MEBIBYTE_SUFFIX = "Mi";

    /** JVM command fragments shared by master and worker containers. */
    static final String JAVA_COMMAND = "java";

    static final int JVM_HEAP_NUMERATOR = 3;
    static final int JVM_HEAP_DENOMINATOR = 4;
    static final int MINIMUM_JVM_HEAP_MB = 1;
    static final String SEATUNNEL_CONFIG_FILE = "/config/seatunnel.yaml";
    static final String LOG4J_CONFIG_FILE = "/config/log4j2_client.properties";
    static final String KUBERNETES_CLASSPATH =
            "/starter/seatunnel-starter.jar:"
                    + "%s/starter/logging/*:"
                    + "%s/lib/*:"
                    + "%s/resource-managers/kubernetes/*:"
                    + "%s/config";

    /** Kubernetes field values used by master and worker pod specifications. */
    static final String RESTART_POLICY_NEVER = "Never";

    static final String POD_IP_FIELD_PATH = "status.podIP";
    static final String HEADLESS_CLUSTER_IP = "None";
    static final String HAZELCAST_PORT_NAME = "hazelcast";
    static final int APPLICATION_SECRET_MODE = 0400;
    static final long TERMINATION_GRACE_PERIOD_SECONDS = 120L;

    /** Probe timing and command values shared by application Pods. */
    static final int STARTUP_PROBE_PERIOD_MILLIS = 5000;

    static final int PROBE_PERIOD_SECONDS = 10;
    static final int PROBE_TIMEOUT_SECONDS = 2;
    static final int PROBE_FAILURE_THRESHOLD = 3;
    static final String SHELL_COMMAND = "/bin/sh";
    static final String PROCESS_PROBE_COMMAND = "kill -0 1";

    private KubernetesConstants() {}
}
