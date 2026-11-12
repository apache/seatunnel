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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

/**
 * A Parquet {@link InputFile} bound to an already-open {@link FileSystem}.
 *
 * <p>Read-side counterpart of {@link FileSystemOutputFile}: {@code HadoopInputFile.fromPath}
 * resolves its own FileSystem, which leaks one per file while the Hadoop FileSystem cache is
 * disabled.
 */
public class FileSystemInputFile implements InputFile {

    private final FileSystem fileSystem;
    private final FileStatus status;

    public FileSystemInputFile(FileSystem fileSystem, FileStatus status) {
        this.fileSystem = fileSystem;
        this.status = status;
    }

    public static FileSystemInputFile fromPath(FileSystem fileSystem, Path path)
            throws IOException {
        return new FileSystemInputFile(fileSystem, fileSystem.getFileStatus(path));
    }

    @Override
    public long getLength() {
        return status.getLen();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return HadoopStreams.wrap(fileSystem.open(status.getPath()));
    }

    @Override
    public String toString() {
        return status.getPath().toString();
    }
}
