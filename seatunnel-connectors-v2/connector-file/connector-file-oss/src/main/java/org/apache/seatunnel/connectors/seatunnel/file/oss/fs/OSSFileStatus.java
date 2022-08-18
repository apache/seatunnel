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

package org.apache.seatunnel.connectors.seatunnel.file.oss.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Wrapper for {@link org.apache.hadoop.fs.FileStatus},
 * provides simplified constructor and easy to check empty directory.
 */
public class OSSFileStatus extends FileStatus {
    private boolean isEmptyDirectory;

    // Directories
    public OSSFileStatus(boolean isDir, boolean isEmptyDir, Path path) {
        super(0, isDir, 1, 0, 0, path);
        isEmptyDirectory = isEmptyDir;
    }

    // Files
    public OSSFileStatus(long length, long modificationTime, Path path, long blockSize) {
        super(length, false, 1, blockSize, modificationTime, path);
        isEmptyDirectory = false;
    }

    public boolean isEmptyDirectory() {
        return isEmptyDirectory;
    }

    @Override
    public String toString() {
        return String.format("[EmptyDir]%b %s", isEmptyDirectory, super.toString());
    }

}
