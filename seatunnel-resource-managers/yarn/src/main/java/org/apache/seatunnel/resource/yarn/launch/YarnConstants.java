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

package org.apache.seatunnel.resource.yarn.launch;

import java.util.List;

/** Names shared by the submitter, the ApplicationMaster and the NodeManager launch commands. */
public final class YarnConstants {
    /** Container-relative directory under which the NodeManager unpacks the distribution. */
    public static final String LOCALIZED_DISTRIBUTION_NAME = "seatunnel";

    /**
     * File in remote staging that records the archive URL, size, timestamp and runtime settings.
     */
    public static final String DISTRIBUTION_MANIFEST = "distribution.properties";

    /** File the AM reads from its working directory to reconstruct the submitted application. */
    public static final String LOCALIZED_SPECIFICATION_NAME = "application.properties";

    /** AM-local filename for the job selected by {@code yarn.job-config-file}. */
    public static final String LOCALIZED_JOB_CONFIG_NAME = "job.conf";

    /** Merged Hadoop XML loaded by both roles before they access the staging filesystem. */
    public static final String LOCALIZED_HADOOP_CONFIG_NAME = "hadoop-conf.xml";

    /** Container-relative directory for shipped files and files generated from inline content. */
    public static final String SHIPPED_FILES_DIRECTORY = "files";

    /**
     * Deployment key prefix for literal container environment variables, e.g. {@code yarn.env.TZ}.
     */
    public static final String ENV_PREFIX = "yarn.env.";

    /**
     * Maps a file alias to its submitter-local path or HDFS URI, e.g. {@code
     * yarn.ship-files.rules}.
     */
    public static final String SHIP_FILES_PREFIX = "yarn.ship-files.";

    /** Maps a file alias to the UTF-8 text to write, e.g. {@code yarn.inline-files.rules}. */
    public static final String INLINE_FILES_PREFIX = "yarn.inline-files.";

    /** Introduces a named file group, e.g. {@code yarn.local-resources.runtime}. */
    public static final String LOCAL_RESOURCES_PREFIX = "yarn.local-resources.";

    /** Group field identifying the local or HDFS directory containing the source files. */
    public static final String SOURCE_DIR = "source-dir";

    /**
     * Group field prefix mapping a filename to its literal contents, e.g. {@code content.rules}.
     */
    public static final String CONTENT_PREFIX = "content.";

    /** Group field prefix mapping a filename to its container-relative destination. */
    public static final String LOCALIZE_PREFIX = "localize.";

    /** File-option value telling the launcher to skip that configuration file. */
    public static final String DISABLED = "disabled";

    /** Default JVM argument file under the distribution's config directory, read by both roles. */
    public static final String JVM_OPTIONS_FILE = "jvm_options";

    /** Default AM-specific JVM argument file, applied after the common JVM options. */
    public static final String MASTER_JVM_OPTIONS_FILE = "jvm_master_options";

    /** Default worker-specific JVM argument file, applied after the common JVM options. */
    public static final String WORKER_JVM_OPTIONS_FILE = "jvm_worker_options";

    /** Directory prefix relative to the extracted SeaTunnel home, including its trailing slash. */
    public static final String CONFIG_DIRECTORY = "config/";

    /** Default engine configuration path relative to the extracted SeaTunnel home. */
    public static final String SEATUNNEL_CONFIG_FILE = CONFIG_DIRECTORY + "seatunnel.yaml";

    /**
     * Remote directory the AM reads to launch workers and removes when the application finishes.
     */
    public static final String STAGING_DIRECTORY_ENV = "SEATUNNEL_YARN_STAGING";

    /** Absolute path to this container's extracted SeaTunnel home, exported to the process. */
    public static final String SEATUNNEL_HOME_ENV = "SEATUNNEL_HOME";

    /**
     * Submitter fallback for {@code yarn.config-dir}. In containers it points to the working
     * directory containing the localized Hadoop configuration.
     */
    public static final String HADOOP_CONF_DIR_ENV = "HADOOP_CONF_DIR";

    /** Hadoop user hint exported to containers when simple authentication is active. */
    public static final String HADOOP_USER_NAME_ENV = "HADOOP_USER_NAME";

    /** JVM property the AM and worker read to locate JARs in their own distribution trees. */
    public static final String SEATUNNEL_HOME_PROPERTY = "seatunnel.home";

    /** Launcher prefix for the maximum Java heap, followed by the computed size in MiB. */
    static final String JVM_MAX_HEAP_PREFIX = "-Xmx";

    /** Makes an out-of-memory JVM exit so YARN can report the container's termination. */
    static final String JVM_EXIT_ON_OUT_OF_MEMORY = "-XX:+ExitOnOutOfMemoryError";

    /** Launcher argument prefix that publishes the extracted home to the SeaTunnel JVM. */
    public static final String SYSPROP_SEATUNNEL_HOME = "-D" + SEATUNNEL_HOME_PROPERTY + "=";

    /** Launcher argument prefix that selects the engine configuration file for this container. */
    public static final String SYSPROP_SEATUNNEL_CONFIG = "-Dseatunnel.config=";

    /** JVM property the configuration loader reads to select the role's Hazelcast member file. */
    public static final String HAZELCAST_CONFIG_PROPERTY = "hazelcast.config";

    /** Launcher argument prefix that passes the role-specific Hazelcast file to the JVM. */
    public static final String SYSPROP_HAZELCAST_CONFIG = "-D" + HAZELCAST_CONFIG_PROPERTY + "=";

    /** JDK module access required by Hazelcast and Kerberos in AM and worker JVMs. */
    static final List<String> MANDATORY_JVM_MODULE_FLAGS =
            List.of(
                    "--add-opens=java.base/java.lang=ALL-UNNAMED",
                    "--add-opens=java.base/java.net=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/java.util=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED");

    /** Routes Hazelcast logs through the container's Log4j2 configuration. */
    public static final String SYSPROP_HAZELCAST_LOGGING = "-Dhazelcast.logging.type=log4j2";

    /** Launcher argument prefix that selects the Log4j2 configuration file. */
    public static final String SYSPROP_LOG4J_CONFIG = "-Dlog4j2.configurationFile=";

    /**
     * Launcher argument prefix that points the file appender at the YARN container log directory.
     */
    public static final String SYSPROP_LOGS_PATH = "-Dseatunnel.logs.path=";

    /** Sets the base filename used by SeaTunnel's file appender inside each container. */
    public static final String APPLICATION_LOG_FILE =
            "-Dseatunnel.logs.file_name=seatunnel-application";

    /** Numerator of the container-memory fraction used for the Java heap (3/4). */
    public static final int JVM_HEAP_NUMERATOR = 3;

    /** Denominator of the 3/4 heap fraction, applied before the minimum heap size. */
    public static final int JVM_HEAP_DENOMINATOR = 4;

    /** Lower bound in MiB when the launcher computes the AM or worker heap. */
    public static final int MINIMUM_JVM_HEAP_MB = 64;

    /** Default filename for {@code yarn.log4j-config-file}, relative to the config directory. */
    public static final String LOG4J_CONFIG_FILE_NAME = "log4j2_client.properties";

    /** Path to the default Log4j2 file relative to the extracted SeaTunnel home. */
    public static final String LOG4J_CONFIG_FILE = CONFIG_DIRECTORY + LOG4J_CONFIG_FILE_NAME;

    /**
     * Classpath template for the AM and worker JVMs. The leading dot includes localized files in
     * the working directory. Each {@code %s} expands to the container's SeaTunnel home with a
     * trailing slash.
     */
    public static final String YARN_CLASSPATH =
            ".:%sconfig:%sstarter/seatunnel-starter.jar:%sstarter/logging/*:%slib/*:%sresource-managers/yarn/*";

    private YarnConstants() {}
}
