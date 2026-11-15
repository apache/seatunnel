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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Stable Kubernetes names and field values shared by the application client, the resource manager
 * and the decorator chain.
 */
public final class KubernetesConstants {
    /** API group versions used by core and batch resources. */
    public static final String CORE_API_VERSION = "v1";

    public static final String BATCH_API_VERSION = "batch/v1";

    /** Kubernetes resource kinds used in models and owner references. */
    public static final String JOB_KIND = "Job";

    public static final String POD_KIND = "Pod";
    public static final String SECRET_KIND = "Secret";
    public static final String CONFIG_MAP_KIND = "ConfigMap";
    public static final String SERVICE_KIND = "Service";

    /**
     * Secret type used for the application-owned Secret built from {@code kubernetes.secret.data}.
     */
    public static final String OPAQUE_SECRET_TYPE = "Opaque";

    /** Labels grouping all objects for an application and distinguishing their roles. */
    public static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";

    public static final String ROLE_LABEL = "seatunnel.apache.org/role";
    public static final String MASTER_ROLE = "master";
    public static final String WORKER_ROLE = "worker";
    public static final String CONFIGURATION_ROLE = "configuration";

    /** Prefix shared by every deployment option, for example {@code kubernetes.image}. */
    public static final String OPTION_PREFIX = "kubernetes.";

    /** Option name of the created Secret, used in messages as well as in keys. */
    public static final String SECRET_OPTION = OPTION_PREFIX + "secret";

    /** Option name of the created Secret's data entries, for example {@code ...data.KEY}. */
    public static final String SECRET_DATA_OPTION = SECRET_OPTION + ".data";

    /** Prefix of the created Secret's data entries, for example {@code ...data.KEY}. */
    public static final String SECRET_DATA_PREFIX = SECRET_DATA_OPTION + ".";

    /** Option name of the ConfigMap entries, for example {@code kubernetes.configmap.<entry>}. */
    public static final String CONFIG_MAP_OPTION = OPTION_PREFIX + "configmap";

    /** Prefix of the ConfigMap entries, for example {@code ...configmap.<entry>.ref}. */
    public static final String CONFIG_MAP_PREFIX = CONFIG_MAP_OPTION + ".";

    /** Prefix of one Pod environment variable, for example {@code kubernetes.env.NAME}. */
    public static final String ENV_PREFIX = OPTION_PREFIX + "env.";

    /** Prefix of the whole-object envFrom references. */
    public static final String ENV_FROM_PREFIX = OPTION_PREFIX + "env-from.";

    public static final String ENV_FROM_SECRET = ENV_FROM_PREFIX + "secret";

    public static final String ENV_FROM_CONFIGMAP = ENV_FROM_PREFIX + "configmap";

    /** Value schemes of one {@code kubernetes.env.*} reference. */
    public static final String ENV_VALUE_SECRET_PREFIX = "secret:";

    public static final String ENV_VALUE_CONFIGMAP_PREFIX = "configmap:";

    /** Java launcher tokens of the master and worker container command. */
    public static final String JVM_MAX_HEAP_PREFIX = "-Xmx";

    public static final String JVM_EXIT_ON_OUT_OF_MEMORY = "-XX:+ExitOnOutOfMemoryError";

    /** System properties the containers pass to the application JVM. */
    public static final String SYSPROP_SEATUNNEL_HOME = "-Dseatunnel.home=";

    public static final String SYSPROP_SEATUNNEL_CONFIG = "-Dseatunnel.config=";

    public static final String SYSPROP_LOG4J_CONFIG = "-Dlog4j2.configurationFile=";

    public static final String SYSPROP_HAZELCAST_CONFIG = "-Dhazelcast.config=";

    public static final String SYSPROP_LOGS_PATH = "-Dseatunnel.logs.path=";

    public static final String SYSPROP_LOGS_FILE_NAME = "-Dseatunnel.logs.file_name=";

    public static final String SYSPROP_HAZELCAST_LOGGING = "-Dhazelcast.logging.type=log4j2";

    /** Name of the reserved key whose content replaces the inline job configuration. */
    public static final String JOB_CONFIG_FILE = "job.conf";

    /** Distribution files whose JVM options are applied through the Java argument-file syntax. */
    public static final String JVM_OPTIONS_FILE = "jvm_options";

    public static final String MASTER_JVM_OPTIONS_FILE = "jvm_master_options";

    public static final String WORKER_JVM_OPTIONS_FILE = "jvm_worker_options";

    /**
     * Value of a defaulted runtime-file option that suppresses the file: the container command
     * passes no argument file for that role. Only the JVM-options options accept it, because only
     * they replace a distribution file that would otherwise be applied by default.
     */
    public static final String DISABLED = "disabled";

    /**
     * Mandatory JDK module flags for Java 11+, mirroring the cluster launcher scripts. Hazelcast
     * and the Kerberos configuration reload need this reflective access.
     */
    public static final List<String> MANDATORY_JVM_MODULE_FLAGS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "--add-opens=java.base/java.lang=ALL-UNNAMED",
                            "--add-opens=java.base/java.net=ALL-UNNAMED",
                            "--add-opens=java.base/java.nio=ALL-UNNAMED",
                            "--add-opens=java.base/java.util=ALL-UNNAMED",
                            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                            "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED"));

    /** Default directory where a configured application Secret is mounted read-only. */
    public static final String APPLICATION_SECRET_DIRECTORY = "/etc/seatunnel-secrets/application";

    /** Environment variable carrying the Base64 encoded localized application specification. */
    public static final String SPECIFICATION_ENV = "SEATUNNEL_APPLICATION_SPECIFICATION";

    public static final String APPLICATION_VOLUME = "application";
    public static final String CONFIG_VOLUME = "seatunnel-config";
    public static final String CHECKPOINT_VOLUME = "checkpoints";

    /** Main container identity, paths, and environment exposed to the application entrypoint. */
    public static final String CONTAINER_NAME = "seatunnel";

    /** Default container path where the existing checkpoint PVC is mounted on the master. */
    public static final String CHECKPOINT_DIRECTORY = "/opt/seatunnel/checkpoints";

    public static final String MASTER_HOST_ENV = "SEATUNNEL_APPLICATION_MASTER_HOST";
    public static final String SEATUNNEL_HOME_ENV = "SEATUNNEL_HOME";

    /** Kubernetes resource keys and units used for pod requests and limits. */
    public static final String MEMORY_RESOURCE = "memory";

    public static final String CPU_RESOURCE = "cpu";
    public static final String MEBIBYTE_SUFFIX = "Mi";

    /** JVM command fragments shared by master and worker containers. */
    public static final String JAVA_COMMAND = "java";

    public static final int JVM_HEAP_NUMERATOR = 3;
    public static final int JVM_HEAP_DENOMINATOR = 4;
    public static final int MINIMUM_JVM_HEAP_MB = 1;
    public static final String SEATUNNEL_CONFIG_FILE = "config/seatunnel.yaml";
    public static final String LOG4J_CONFIG_FILE = "log4j2_client.properties";
    /**
     * Classpath of the application containers.
     *
     * <p>Only jars are listed: the distribution's {@code config} directory deliberately stays off
     * the classpath so that engine and logging configuration come exclusively from the explicit
     * {@code -Dseatunnel.config} / {@code -Dlog4j2.configurationFile} paths. A classpath copy would
     * silently supply the distribution file even when a different one is configured.
     */
    public static final String KUBERNETES_CLASSPATH =
            "%s/starter/seatunnel-starter.jar:"
                    + "%s/starter/logging/*:"
                    + "%s/lib/*:"
                    + "%s/resource-managers/kubernetes/*";

    /** Kubernetes field values used by master and worker pod specifications. */
    public static final String RESTART_POLICY_NEVER = "Never";

    public static final String POD_IP_FIELD_PATH = "status.podIP";
    public static final String HAZELCAST_PORT_NAME = "hazelcast";
    /** Cluster IP of a headless Service: Kubernetes publishes the Pod addresses directly. */
    public static final String HEADLESS_CLUSTER_IP = "None";

    public static final int APPLICATION_SECRET_MODE = 0400;
    public static final long TERMINATION_GRACE_PERIOD_SECONDS = 120L;

    /** Probe timing and command values shared by application Pods. */
    public static final int STARTUP_PROBE_PERIOD_MILLIS = 5000;

    public static final long WATCH_TRANSIENT_FAILURE_BUDGET_MILLIS = 90_000;
    public static final int PROBE_PERIOD_SECONDS = 10;
    public static final int PROBE_TIMEOUT_SECONDS = 2;
    public static final int PROBE_FAILURE_THRESHOLD = 3;
    public static final String SHELL_COMMAND = "/bin/sh";

    private KubernetesConstants() {}
}
