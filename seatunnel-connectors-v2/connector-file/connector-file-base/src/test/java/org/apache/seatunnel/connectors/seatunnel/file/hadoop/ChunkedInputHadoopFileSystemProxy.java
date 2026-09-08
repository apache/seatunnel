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

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileChecksum;

import java.io.IOException;
import java.nio.file.Path;

/** A test filesystem proxy that exposes configured content using bounded read chunk sizes. */
public final class ChunkedInputHadoopFileSystemProxy extends HadoopFileSystemProxy {
    private final Path sourceFile;
    private final byte[] sourceContent;
    private final int sourceChunkSize;
    private final Path targetFile;
    private final byte[] targetContent;
    private final int targetChunkSize;

    public ChunkedInputHadoopFileSystemProxy(
            HadoopConf hadoopConf,
            Path sourceFile,
            byte[] sourceContent,
            int sourceChunkSize,
            Path targetFile,
            byte[] targetContent,
            int targetChunkSize) {
        super(hadoopConf);
        this.sourceFile = sourceFile;
        this.sourceContent = sourceContent;
        this.sourceChunkSize = sourceChunkSize;
        this.targetFile = targetFile;
        this.targetContent = targetContent;
        this.targetChunkSize = targetChunkSize;
    }

    @Override
    public FileChecksum getFileChecksum(String filePath) {
        return null;
    }

    @Override
    public FSDataInputStream getInputStream(String filePath) throws IOException {
        String normalizedPath = new org.apache.hadoop.fs.Path(filePath).toUri().getPath();
        if (normalizedPath.equals(sourceFile.toString())) {
            return chunkedInputStream(sourceContent, sourceChunkSize);
        }
        if (normalizedPath.equals(targetFile.toString())) {
            return chunkedInputStream(targetContent, targetChunkSize);
        }
        return super.getInputStream(filePath);
    }

    private static FSDataInputStream chunkedInputStream(byte[] content, int chunkSize) {
        return new FSDataInputStream(new ChunkedFSInputStream(content, chunkSize));
    }

    private static final class ChunkedFSInputStream extends FSInputStream {
        private final byte[] content;
        private final int chunkSize;
        private int position;

        private ChunkedFSInputStream(byte[] content, int chunkSize) {
            this.content = content;
            this.chunkSize = chunkSize;
        }

        @Override
        public int read() {
            if (position >= content.length) {
                return -1;
            }
            return content[position++] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) {
            if (position >= content.length) {
                return -1;
            }
            int bytesToRead = Math.min(Math.min(length, chunkSize), content.length - position);
            System.arraycopy(content, position, buffer, offset, bytesToRead);
            position += bytesToRead;
            return bytesToRead;
        }

        @Override
        public void seek(long newPosition) {
            position = (int) newPosition;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public boolean seekToNewSource(long targetPosition) {
            return false;
        }
    }
}
