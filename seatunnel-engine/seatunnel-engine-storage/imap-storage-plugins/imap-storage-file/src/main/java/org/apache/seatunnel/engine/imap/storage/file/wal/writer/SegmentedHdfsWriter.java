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

package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.imap.storage.file.compaction.RegionCompactionPlanner;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Rotates on the WAL writer thread and exposes only successfully closed segments to the planner.
 */
public final class SegmentedHdfsWriter extends HdfsWriter {
    private final long segmentSize;
    private final RegionCompactionPlanner planner;
    private FileSystem fs;
    private Path directory;
    private Path current;
    private Serializer serializer;
    private long index;
    private long writtenBytes;
    private boolean closed;

    public SegmentedHdfsWriter(long segmentSize, RegionCompactionPlanner planner) {
        this.segmentSize = segmentSize;
        this.planner = planner;
    }

    @Override
    public void initialize(FileSystem fs, Path parentPath, Serializer serializer)
            throws IOException {
        this.fs = fs;
        this.directory = parentPath;
        this.serializer = serializer;
        openNextSegment();
    }

    private void openNextSegment() throws IOException {
        Path next = new Path(directory, String.format("segment-%020d-wal.txt", ++index));
        open(fs, next, serializer);
        current = next;
        writtenBytes = 0;
    }

    @Override
    public void write(IMapFileData mutation) throws IOException {
        if (closed) {
            throw new IOException("IMap WAL writer is closed: " + directory);
        }
        if (current == null) {
            openNextSegment();
        }
        byte[] bytes = serializer.serialize(mutation);
        try {
            super.write(bytes);
        } catch (IOException e) {
            Path failed = current;
            planner.failedSeal(failed);
            // A failed append may leave a partial frame. Never append another record to it, or
            // compact later segments across the resulting gap in the replay prefix.
            current = null;
            try {
                super.close();
            } catch (Exception closeFailure) {
                if (e != closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw new IOException("Cannot append IMap WAL segment " + failed, e);
        }
        writtenBytes += bytes.length + WALDataUtils.WAL_DATA_METADATA_LENGTH;
        if (writtenBytes >= segmentSize) {
            seal();
        }
    }

    private void seal() throws IOException {
        Path sealed = current;
        try {
            super.close();
        } catch (Exception e) {
            planner.failedSeal(sealed);
            throw new IOException("Cannot seal IMap WAL segment " + sealed, e);
        } finally {
            // Closing can fail after the stream is already closed. The following write must open
            // a fresh stream rather than permanently retrying the failed one.
            current = null;
        }
        if (writtenBytes > 0) {
            planner.sealed(sealed);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (current != null) {
                seal();
            }
        }
    }
}
