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

package org.apache.seatunnel.resource.yarn.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

/** Options specific to a single, non-HA YARN application. */
public final class YarnOptions {

    private YarnOptions() {}

    public static final Option<YarnDeploymentTarget> DEPLOYMENT_TARGET =
            Options.key("yarn.deployment-target")
                    .enumType(YarnDeploymentTarget.class)
                    .defaultValue(YarnDeploymentTarget.APPLICATION)
                    .withDescription(
                            "YARN deployment topology. The first release supports APPLICATION only.");

    public static final Option<String> DISTRIBUTION =
            Options.key("yarn.distribution")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Local path or HDFS URI of a SeaTunnel .tar.gz, .tgz or .zip distribution containing the YARN resource-manager bundle and required connector jars.");

    public static final Option<String> JOB_CONFIG_FILE =
            Options.key("yarn.job-config-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Local path, HDFS URI or a localized resource path for the AM-only job. Takes precedence over --config.");

    public static final Option<String> JVM_OPTIONS =
            Options.key("yarn.jvm-options-file")
                    .stringType()
                    .defaultValue(YarnConstants.JVM_OPTIONS_FILE)
                    .withDescription(
                            "Common Java argument file applied before the role-specific file. Set to disabled to skip it.");

    public static final Option<String> MASTER_JVM_OPTIONS =
            Options.key("yarn.master.jvm-options-file")
                    .stringType()
                    .defaultValue(YarnConstants.MASTER_JVM_OPTIONS_FILE)
                    .withDescription(
                            "ApplicationMaster Java argument file. Set to disabled to skip it.");

    public static final Option<String> WORKER_JVM_OPTIONS =
            Options.key("yarn.worker.jvm-options-file")
                    .stringType()
                    .defaultValue(YarnConstants.WORKER_JVM_OPTIONS_FILE)
                    .withDescription("Worker Java argument file. Set to disabled to skip it.");

    public static final Option<String> MASTER_HAZELCAST_CONFIG =
            Options.key("yarn.master.hazelcast-config")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hazelcast member configuration for the ApplicationMaster (.yaml, .yml or .xml).");

    public static final Option<String> WORKER_HAZELCAST_CONFIG =
            Options.key("yarn.worker.hazelcast-config")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hazelcast member configuration for workers (.yaml, .yml or .xml).");

    public static final Option<Integer> MASTER_HTTP_PORT =
            Options.key("yarn.master.http-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Fixed HTTP port for the ApplicationMaster web UI and REST API. Omit to keep HTTP disabled.");

    public static final Option<String> LOG4J_CONFIG_FILE =
            Options.key("yarn.log4j-config-file")
                    .stringType()
                    .defaultValue(YarnConstants.LOG4J_CONFIG_FILE_NAME)
                    .withDescription("Log4j2 configuration file used by both roles.");

    public static final Option<String> SEATUNNEL_CONFIG_FILE =
            Options.key("yarn.seatunnel-config-file")
                    .stringType()
                    .defaultValue(YarnConstants.SEATUNNEL_CONFIG_FILE)
                    .withDescription(
                            "SeaTunnel engine configuration (.yaml or .yml), relative to the distribution home, shipped, or absolute.");

    public static final Option<Long> ACTIVE_DEADLINE_SECONDS =
            Options.key("yarn.active-deadline-seconds")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Application lifetime in seconds from submission, enforced by the YARN ResourceManager and queue policy.");

    /** Dynamic options with one entry per environment variable or staged file. */
    public static Option<String> environmentVariable(String name) {
        return Options.key(YarnConstants.ENV_PREFIX + name)
                .stringType()
                .noDefaultValue()
                .withDescription(
                        "Literal environment variable exported to both the ApplicationMaster and workers.");
    }

    public static Option<String> shipFile(String alias) {
        return Options.key(YarnConstants.SHIP_FILES_PREFIX + alias)
                .stringType()
                .noDefaultValue()
                .withDescription(
                        "Local path or HDFS URI of a file localized as files/<alias> in every container.");
    }

    public static Option<String> inlineFile(String alias) {
        return Options.key(YarnConstants.INLINE_FILES_PREFIX + alias)
                .stringType()
                .noDefaultValue()
                .withDescription(
                        "UTF-8 contents of a file created during staging. Refer to it as files/<alias>.");
    }

    public static Option<String> resourceDirectory(String group) {
        return resourceOption(
                group,
                YarnConstants.SOURCE_DIR,
                "Local or HDFS directory containing the files selected for localization.");
    }

    public static Option<String> resourceContent(String group, String file) {
        return resourceOption(
                group,
                YarnConstants.CONTENT_PREFIX + file,
                "UTF-8 contents of a file to create in this application's private staging directory.");
    }

    public static Option<String> resourceTarget(String group, String file) {
        return resourceOption(
                group,
                YarnConstants.LOCALIZE_PREFIX + file,
                "Container-relative destination under files/. Inline content defaults to files/<filename>.");
    }

    private static Option<String> resourceOption(String group, String field, String description) {
        return Options.key(YarnConstants.LOCAL_RESOURCES_PREFIX + group + "." + field)
                .stringType()
                .noDefaultValue()
                .withDescription(description);
    }

    public static final Option<String> CONFIG_DIRECTORY =
            Options.key("yarn.config-dir")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Hadoop configuration directory; falls back to HADOOP_CONF_DIR.");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("yarn.kerberos.principal")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Kerberos principal used with yarn.kerberos.keytab on the submission host. Empty uses the current ticket cache.");

    public static final Option<String> KERBEROS_KEYTAB =
            Options.key("yarn.kerberos.keytab")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Local keytab for the submission principal. Only the submitter reads this file.");

    public static final Option<String> STAGING_DIRECTORY =
            Options.key("yarn.staging-dir")
                    .stringType()
                    .defaultValue(".seatunnel/applications")
                    .withDescription(
                            "HDFS staging root; relative paths resolve under the submitting user's home directory.");

    public static final Option<String> QUEUE =
            Options.key("yarn.queue")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("YARN scheduling queue.");

    public static final Option<Integer> PRIORITY =
            Options.key("yarn.priority")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Application priority. Use -1 for the cluster default or set a nonnegative priority.");

    public static final Option<String> TAGS =
            Options.key("yarn.tags")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Comma-separated tags attached to the YARN application.");

    public static final Option<String> MASTER_NODE_LABEL =
            Options.key("yarn.master.node-label")
                    .stringType()
                    .defaultValue("")
                    .withDescription("YARN node-label expression used for the ApplicationMaster.");

    public static final Option<String> WORKER_NODE_LABEL =
            Options.key("yarn.worker.node-label")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "YARN node-label expression used for worker containers; empty inherits yarn.master.node-label.");

    public static final Option<String> HADOOP_USER_NAME =
            Options.key("yarn.hadoop-user-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hadoop user for simple-mode submission, staging and containers. Defaults to the current Hadoop user's short name.");
}
