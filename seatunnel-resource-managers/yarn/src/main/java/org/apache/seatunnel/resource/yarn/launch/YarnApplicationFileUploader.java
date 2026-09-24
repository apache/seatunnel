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

import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/** Uploads all application-private files localized by YARN containers. */
public final class YarnApplicationFileUploader {
    private YarnApplicationFileUploader() {}

    /**
     * Uploads the distribution, application specification and merged Hadoop configuration.
     *
     * @param fileSystem shared filesystem owning the application staging directory
     * @param staging application-private staging directory
     * @param distribution validated distribution archive layout
     * @param archive local SeaTunnel distribution archive
     * @param specification immutable application configuration
     * @param hadoopConfiguration merged Hadoop settings required inside containers
     * @throws Exception when any file cannot be uploaded completely
     */
    public static void upload(
            FileSystem fileSystem,
            Path staging,
            YarnDistribution distribution,
            File archive,
            ApplicationSpecification specification,
            Configuration hadoopConfiguration)
            throws Exception {
        distribution.stage(fileSystem, staging, archive);
        try (Writer specificationWriter =
                new OutputStreamWriter(
                        fileSystem.create(
                                new Path(staging, YarnConstants.LOCALIZED_SPECIFICATION_NAME),
                                false),
                        StandardCharsets.UTF_8)) {
            specification.write(specificationWriter);
        }
        try (FSDataOutputStream output =
                fileSystem.create(
                        new Path(staging, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME), false)) {
            hadoopConfiguration.writeXml(output);
        }
    }
}
