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

import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.launch.factory.ApplicationMasterContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplicationStaging;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** Owns application staging, file uploads and registration of YARN local resources. */
public final class YarnApplicationFileUploader implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationFileUploader.class);

    private static final FsPermission STAGING_PERMISSION = new FsPermission((short) 0700);

    private final Configuration configuration;
    private final YarnApplicationConfiguration deployment;
    private final FileSystem fileSystem;
    private final Path applicationDir;

    /**
     * Opens an independently owned staging filesystem client. The application directory is created
     * by {@link #upload()}.
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
        List<YarnLocalResource> files = masterFiles(deployment);
        LOG.debug("Registering application resources in {}", applicationDir);
        if (fileSystem.exists(applicationDir)) {
            throw new IllegalStateException(
                    "Application staging directory already exists: " + applicationDir);
        }
        if (!fileSystem.mkdirs(applicationDir, STAGING_PERMISSION)) {
            throw new IOException(
                    "Could not create application staging directory " + applicationDir);
        }
        try {
            fileSystem.setPermission(applicationDir, STAGING_PERMISSION);
            FileStatus archive =
                    YarnDistributionSource.register(
                            deployment.getDistributionUri(),
                            fileSystem,
                            applicationDir,
                            configuration);
            YarnDistribution distribution = YarnDistribution.inspect(fileSystem, archive);
            YarnApplicationStaging staging =
                    new YarnApplicationStaging(
                            fileSystem, applicationDir, distribution, deployment, configuration);
            staging.stage(files);
            return YarnLocalResources.resolve(fileSystem, applicationDir, true);
        } catch (Exception failure) {
            try {
                YarnStagingDirectory.cleanup(configuration, applicationDir);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Returns the qualified application directory retained for lifecycle cleanup. */
    public Path getApplicationDir() {
        return applicationDir;
    }

    /** Closes only the owned filesystem client; submitted applications still need their files. */
    @Override
    public void close() throws IOException {
        fileSystem.close();
    }

    private static List<YarnLocalResource> masterFiles(YarnApplicationConfiguration deployment) {
        return YarnLocalResources.collect(
                ApplicationMasterContainerFactory.decorators(
                        deployment.getContainerSettings(), deployment.getJobFile() != null));
    }
}
