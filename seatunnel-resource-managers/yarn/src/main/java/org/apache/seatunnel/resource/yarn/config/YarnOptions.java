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

/** Options specific to a single, non-HA YARN application. */
public final class YarnOptions {

    private YarnOptions() {}

    public static final Option<String> DISTRIBUTION =
            Options.key("yarn.distribution")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Local SeaTunnel .tar.gz, .tgz or .zip distribution, containing the YARN resource-manager bundle and required connector jars.");

    public static final Option<String> CONFIG_DIRECTORY =
            Options.key("yarn.config-dir")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Hadoop configuration directory; falls back to HADOOP_CONF_DIR.");

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
}
