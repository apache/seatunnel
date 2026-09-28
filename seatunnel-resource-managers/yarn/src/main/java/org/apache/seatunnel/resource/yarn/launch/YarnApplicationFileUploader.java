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

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Owns application staging, file uploads and registration of YARN local resources. */
public final class YarnApplicationFileUploader implements Closeable {
    private static final FsPermission STAGING_PERMISSION = new FsPermission((short) 0700);

    private final Configuration configuration;
    private final YarnApplicationConfiguration deployment;
    private final YarnDistribution distribution;
    private final FileSystem fileSystem;
    private final Path applicationDir;
    private final Map<String, LocalResource> localResources = new LinkedHashMap<>();

    /**
     * Validates the distribution and opens an independently owned filesystem client. The
     * application directory is created only when {@link #upload()} is called.
     *
     * @param configuration merged Hadoop settings used for staging and localized into containers
     * @param deployment resolved application submission configuration
     * @param applicationId YARN-assigned application identifier
     * @param allowLocalStaging whether local staging is allowed for in-process tests
     * @throws IOException when the archive or staging filesystem cannot be opened
     */
    public YarnApplicationFileUploader(
            Configuration configuration,
            YarnApplicationConfiguration deployment,
            ApplicationId applicationId,
            boolean allowLocalStaging)
            throws IOException {
        this.configuration = configuration;
        this.deployment = deployment;
        this.distribution = YarnDistribution.inspect(deployment.getDistribution());
        this.fileSystem =
                FileSystem.newInstance(deployment.getStagingRoot().toUri(), configuration);
        try {
            if (!allowLocalStaging && "file".equals(fileSystem.getUri().getScheme())) {
                throw new IllegalArgumentException(
                        "yarn.staging-dir must resolve to a shared filesystem such as HDFS; local file staging is not supported");
            }
            this.applicationDir =
                    fileSystem.makeQualified(
                            new Path(deployment.getStagingRoot(), applicationId.toString()));
        } catch (RuntimeException failure) {
            try {
                fileSystem.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Uploads all application files and registers their localization metadata with the same
     * filesystem client. Removes only this upload's newly created directory on failure; after a
     * successful return, the application lifecycle owns directory cleanup.
     *
     * @return registered resources and distribution home for the master launch context
     * @throws Exception if uploading or registering any resource fails
     */
    public YarnLocalResourceDescriptor upload() throws Exception {
        boolean created = false;
        try {
            if (fileSystem.exists(applicationDir)) {
                throw new IllegalStateException(
                        "Application staging directory already exists: " + applicationDir);
            }
            if (!fileSystem.mkdirs(applicationDir, STAGING_PERMISSION)) {
                throw new IOException(
                        "Could not create application staging directory " + applicationDir);
            }
            created = true;
            fileSystem.setPermission(applicationDir, STAGING_PERMISSION);
            upload(
                    fileSystem,
                    applicationDir,
                    distribution,
                    deployment.getDistribution(),
                    deployment.getSpecification(),
                    configuration);
            registerLocalResource(
                    YarnConstants.LOCALIZED_DISTRIBUTION_NAME,
                    distribution.archive(applicationDir),
                    LocalResourceType.ARCHIVE);
            registerLocalResource(
                    YarnConstants.LOCALIZED_SPECIFICATION_NAME,
                    new Path(applicationDir, YarnConstants.LOCALIZED_SPECIFICATION_NAME),
                    LocalResourceType.FILE);
            registerLocalResource(
                    YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME,
                    new Path(applicationDir, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME),
                    LocalResourceType.FILE);
            return new YarnLocalResourceDescriptor(localResources, distribution.localizedHome());
        } catch (Exception failure) {
            if (created) {
                localResources.clear();
                try {
                    YarnStagingDirectory.cleanup(configuration, applicationDir);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /** Returns the qualified application directory retained for lifecycle cleanup. */
    public Path getApplicationDir() {
        return applicationDir;
    }

    private void registerLocalResource(String key, Path path, LocalResourceType type)
            throws IOException {
        localResources.put(key, YarnLocalResources.resource(fileSystem, path, type));
    }

    /** Closes only the owned filesystem client; submitted applications still need their files. */
    @Override
    public void close() throws IOException {
        fileSystem.close();
    }

    /**
     * Uploads the distribution, application specification and merged Hadoop configuration. The
     * caller owns the supplied filesystem and staging directory, including failure cleanup.
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
