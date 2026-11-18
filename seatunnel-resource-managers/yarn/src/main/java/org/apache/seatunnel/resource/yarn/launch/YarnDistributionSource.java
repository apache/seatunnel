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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.net.URI;

/** Selects the archive that the NodeManagers will actually localize. */
final class YarnDistributionSource {
    private YarnDistributionSource() {}

    /**
     * References a named snapshot on the staging HDFS directly. All other inputs are copied into
     * the private application directory before their layout and localization metadata are read.
     *
     * @return status of the archive on the staging filesystem, including its qualified path
     */
    static FileStatus register(
            URI location, FileSystem destination, Path staging, Configuration configuration)
            throws IOException {
        Path sourcePath = new Path(location);
        String archiveName = YarnDistribution.archiveName(sourcePath.getName());
        try (FileSystem source = FileSystem.newInstance(location, configuration)) {
            FileStatus status = source.getFileStatus(sourcePath);
            if (!status.isFile()) {
                throw new IOException("yarn.distribution must be a file: " + sourcePath);
            }
            if (HdfsConstants.HDFS_URI_SCHEME.equals(location.getScheme())
                    && source.getUri().equals(destination.getUri())
                    && isSnapshotPath(sourcePath)
                    && source.makeQualified(sourcePath).equals(status.getPath())) {
                return status;
            }
            Path target = new Path(staging, archiveName);
            if (!FileUtil.copy(source, status, destination, target, false, false, configuration)) {
                throw new IOException("Could not stage yarn.distribution at " + target);
            }
            return destination.getFileStatus(target);
        }
    }

    private static boolean isSnapshotPath(Path path) {
        return path.toUri().getPath().contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR);
    }
}
