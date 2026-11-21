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

package org.apache.seatunnel.engine.imap.storage.file.compaction;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.imap.storage.file.config.CompactionConfig;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.DefaultReader;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.IFileReader.RecordConsumer;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.UUID;

/** Atomic snapshot publication for HDFS and local files. No object-store rename is assumed. */
@Slf4j
public class HdfsCompactionStorage implements CompactionStorage {
    // An equality token for detecting concurrent replay changes, not a snapshot-file pointer.
    // Readers enumerate all WAL files; they never resolve this token to a data-file path.
    public static final String CURRENT_FILE = ".compaction-current";
    // A snapshot replaces a prefix and must replay before the remaining segment files on ties.
    public static final String SNAPSHOT_PREFIX = "00000000000000000000-snapshot-";

    private final FileSystem fs;
    private final Serializer serializer;
    private final DefaultReader reader;
    private final long maxRecordBytes;

    public HdfsCompactionStorage(FileSystem fs, Serializer serializer) throws IOException {
        this(fs, serializer, CompactionConfig.MAX_MEMORY_BYTES.defaultValue());
    }

    public HdfsCompactionStorage(FileSystem fs, Serializer serializer, long maxRecordBytes)
            throws IOException {
        String scheme = fs.getUri().getScheme();
        if (!"hdfs".equalsIgnoreCase(scheme) && !"file".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(
                    "compaction.enabled currently supports only hdfs:// and file:///; scheme="
                            + scheme);
        }
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("Compaction record limit must be positive");
        }
        this.fs = fs;
        // Local atomic pointer replacement must not leave a checksum from its previous contents.
        fs.setWriteChecksum(false);
        this.serializer = serializer;
        this.maxRecordBytes = maxRecordBytes;
        reader = new DefaultReader();
        reader.initialize(fs, serializer);
    }

    /** Creates the replay token before the writer exposes any segment in a new scope. */
    public void initialize(Path directory) throws IOException {
        fs.mkdirs(directory);
        Path current = new Path(directory, CURRENT_FILE);
        if (!fs.exists(current)) {
            writeToken(directory, UUID.randomUUID().toString(), false);
        }
    }

    @Override
    public void read(Path input, RecordConsumer<IMapFileData> consumer) throws IOException {
        reader.readCompletedFile(input, consumer, maxRecordBytes);
    }

    /**
     * Publishes the complete snapshot, then advances a unique token before any input can be
     * deleted. Replaying leftover inputs is harmless because the snapshot keeps raw latest
     * mutations.
     */
    @Override
    public Path publish(CompactionPlan plan, Collection<IMapFileData> mutations)
            throws IOException {
        String id = UUID.randomUUID().toString();
        Path snapshot = new Path(plan.getOutputDirectory(), SNAPSHOT_PREFIX + id + "-wal.txt");
        Path temporary = new Path(plan.getOutputDirectory(), ".snapshot-" + id + ".tmp");
        boolean published = false;
        Exception failure = null;
        try {
            checkInterrupted(plan.getOutputDirectory());
            try (FSDataOutputStream output = fs.create(temporary, false)) {
                for (IMapFileData mutation : mutations) {
                    checkInterrupted(plan.getOutputDirectory());
                    output.write(WALDataUtils.wrapperBytes(serializer.serialize(mutation)));
                }
                checkInterrupted(plan.getOutputDirectory());
                output.hsync();
            }
            checkInterrupted(plan.getOutputDirectory());
            if (!fs.rename(temporary, snapshot)) {
                throw new IOException("Cannot publish IMap snapshot " + snapshot);
            }
            checkInterrupted(plan.getOutputDirectory());
            writeToken(plan.getOutputDirectory(), id, true);
            published = true;
            return snapshot;
        } catch (IOException | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            if (!published) {
                // No caller can reclaim plan inputs before this method returns successfully.
                // CURRENT_FILE is only a replay-change token, so a lost token acknowledgement
                // still leaves every input authoritative. Remove this attempt's unique output
                // so failed retries do not accumulate snapshots unknown to the planner.
                cleanupUnpublishedFile(snapshot, failure);
            }
            cleanupUnpublishedFile(temporary, failure);
        }
    }

    private void writeToken(Path directory, String token, boolean overwrite) throws IOException {
        Path temporary = new Path(directory, ".compaction-" + UUID.randomUUID() + ".tmp");
        Path current = new Path(directory, CURRENT_FILE);
        Exception failure = null;
        try {
            checkInterrupted(directory);
            try (FSDataOutputStream output = fs.create(temporary, false)) {
                output.writeUTF(token);
                output.hsync();
            }
            checkInterrupted(directory);
            if (overwrite) {
                replaceToken(temporary, current);
            } else if (!fs.rename(temporary, current)) {
                throw new IOException("Cannot initialize IMap compaction token " + current);
            }
        } catch (IOException | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            cleanupUnpublishedFile(temporary, failure);
        }
    }

    private static void checkInterrupted(Path directory) throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException(
                    "IMap compaction stopped before publication under " + directory);
        }
    }

    /** Cleanup must neither mask a publication failure nor invalidate a successful commit. */
    private void cleanupUnpublishedFile(Path path, Exception failure) {
        try {
            if (!fs.delete(path, false) && fs.exists(path)) {
                throw new IOException("Cannot remove unpublished IMap compaction file " + path);
            }
        } catch (IOException | RuntimeException cleanupFailure) {
            if (failure != null && failure != cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            log.warn(
                    "Failed to clean up unpublished IMap compaction file {}", path, cleanupFailure);
        }
    }

    /** Replaces the token atomically; delete-then-rename would let readers miss a whole commit. */
    protected void replaceToken(Path source, Path target) throws IOException {
        if ("file".equalsIgnoreCase(fs.getUri().getScheme())) {
            Files.move(
                    Paths.get(fs.makeQualified(source).toUri()),
                    Paths.get(fs.makeQualified(target).toUri()),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } else {
            FileContext.getFileContext(fs.getUri(), fs.getConf())
                    .rename(
                            fs.makeQualified(source),
                            fs.makeQualified(target),
                            Options.Rename.OVERWRITE);
        }
    }

    public static String readToken(FileSystem fs, Path path) throws IOException {
        try (FSDataInputStream input = fs.open(path)) {
            return input.readUTF();
        }
    }

    @Override
    public void delete(Path input) throws IOException {
        if (!fs.delete(input, false) && fs.exists(input)) {
            throw new IOException("Cannot delete compacted IMap WAL " + input);
        }
    }
}
