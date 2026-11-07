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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A Parquet {@link OutputFile} bound to an already-open {@link FileSystem}.
 *
 * <p>{@code HadoopOutputFile.fromPath} resolves its own FileSystem through {@code
 * Path#getFileSystem}. The file connector disables the Hadoop FileSystem cache, so that call
 * allocates a FileSystem per file which nobody closes; its metrics source stays registered in the
 * static {@code DefaultMetricsSystem} for the life of the JVM. Reusing the caller's FileSystem
 * avoids the allocation entirely. Stream creation mirrors {@code HadoopOutputFile} exactly.
 */
public class FileSystemOutputFile implements OutputFile {

    /** Mirrors {@code HadoopOutputFile}: a buffer size must be supplied alongside a block size. */
    private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;

    private static final Set<String> BLOCK_FS_SCHEMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("hdfs", "webhdfs", "viewfs")));

    private final FileSystem fileSystem;
    private final Path path;

    public FileSystemOutputFile(FileSystem fileSystem, Path path) {
        this.fileSystem = fileSystem;
        this.path = fileSystem.makeQualified(path);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return create(false, blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return create(true, blockSizeHint);
    }

    private PositionOutputStream create(boolean overwrite, long blockSizeHint) throws IOException {
        return HadoopStreams.wrap(
                fileSystem.create(
                        path,
                        overwrite,
                        DFS_BUFFER_SIZE_DEFAULT,
                        fileSystem.getDefaultReplication(path),
                        Math.max(fileSystem.getDefaultBlockSize(path), blockSizeHint)));
    }

    @Override
    public boolean supportsBlockSize() {
        return BLOCK_FS_SCHEMES.contains(fileSystem.getUri().getScheme());
    }

    @Override
    public long defaultBlockSize() {
        return fileSystem.getDefaultBlockSize(path);
    }

    @Override
    public String getPath() {
        return path.toString();
    }

    @Override
    public String toString() {
        return path.toString();
    }
}
