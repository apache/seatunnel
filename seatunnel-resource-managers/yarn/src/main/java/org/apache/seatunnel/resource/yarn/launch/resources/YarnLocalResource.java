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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnDistribution;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

/** A container-local name, the application file it writes and its YARN localization record. */
public final class YarnLocalResource extends YarnResource<LocalResource> {

    /** Submission-time writer for one declared file or archive. */
    @FunctionalInterface
    public interface Writer {
        /**
         * Writes the resource to the path selected for this distribution layout.
         *
         * @param staging application inputs and caller-owned filesystem
         * @param target destination inside the application's private directory
         * @throws IOException if the resource cannot be written
         */
        void write(YarnApplicationStaging staging, Path target) throws IOException;
    }

    private final String stagingName;
    private final Writer writer;

    private YarnLocalResource(
            String name, LocalResourceType type, String stagingName, Writer writer) {
        super(name, Records.newRecord(LocalResource.class));
        getInternalResource().setType(type);
        getInternalResource().setVisibility(LocalResourceVisibility.APPLICATION);
        this.stagingName = stagingName;
        this.writer = writer;
    }

    /**
     * Declares a file whose staging path and container-relative name are both given by {@code
     * name}.
     */
    public static YarnLocalResource file(String name, Writer writer) {
        return new YarnLocalResource(name, LocalResourceType.FILE, name, writer);
    }

    /**
     * Declares the archive to unpack under the distribution alias. The submitter writes its
     * manifest inside staging even when the archive itself belongs to an HDFS snapshot.
     */
    public static YarnLocalResource distribution(Writer writer) {
        return new YarnLocalResource(
                YarnConstants.LOCALIZED_DISTRIBUTION_NAME,
                LocalResourceType.ARCHIVE,
                YarnConstants.DISTRIBUTION_MANIFEST,
                writer);
    }

    /**
     * Writes this resource once during submission. Rebuilding a worker spec only reads metadata.
     */
    public void stage(YarnApplicationStaging staging) throws IOException {
        writer.write(staging, new Path(staging.getDirectory(), stagingName));
    }

    /**
     * Populates the Hadoop record from the archive manifest or the staged file's status.
     *
     * @param fs filesystem containing the staged copy, owned by the caller
     * @param directory application staging directory
     * @param distribution archive layout used to locate the staged distribution
     * @throws IOException if the file status cannot be read
     */
    public void resolve(FileSystem fs, Path directory, YarnDistribution distribution)
            throws IOException {
        if (getInternalResource().getType() == LocalResourceType.ARCHIVE) {
            setLocation(
                    distribution.getPath(), distribution.getSize(), distribution.getTimestamp());
            return;
        }
        FileStatus status = fs.getFileStatus(new Path(directory, stagingName));
        setLocation(status.getPath(), status.getLen(), status.getModificationTime());
    }

    private void setLocation(Path path, long size, long timestamp) {
        LocalResource resource = getInternalResource();
        resource.setResource(ConverterUtils.getYarnUrlFromPath(path));
        resource.setSize(size);
        resource.setTimestamp(timestamp);
    }
}
