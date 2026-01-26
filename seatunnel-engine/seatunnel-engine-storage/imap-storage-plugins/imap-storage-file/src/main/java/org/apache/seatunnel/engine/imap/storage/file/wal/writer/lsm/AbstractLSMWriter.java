/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.wal.writer.lsm;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.imap.storage.file.wal.IMapFileIterator;
import org.apache.seatunnel.engine.imap.storage.file.wal.WALFileIterator;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.CompactionFile;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

@Slf4j
public abstract class AbstractLSMWriter implements IFileWriter<IMapFileData> {

    protected final AtomicLong index = new AtomicLong(0);
    protected final AtomicLong tmpIndex = new AtomicLong(0);
    protected long blockSize = 1L * 1024 * 1024;
    protected long compactionThreshold = 512L * 1024 * 1024;
    protected long maxSingleFileSize = 16L * 1024 * 1024;
    protected long compactionBatchSize = 16L * 1024 * 1024;
    protected long compactionInterval = 60L * 1000;

    protected final AtomicLong totalBytes = new AtomicLong(0);
    protected final BlockingQueue<CompactionFile> fileNames = new PriorityBlockingQueue<>();
    protected long compactionIndex = 0;

    protected List<IMapFileData> writeBatch = new ArrayList<>();
    protected ByteBuf bf = Unpooled.buffer(1024);

    protected FileSystem fs;
    protected Path parentPath;
    protected Path finalPath;
    protected Path currentTmpPath;
    protected Serializer serializer;

    protected AbstractLSMWriter(Map<String, Object> config) {
        if (config.get(FileConstants.FileInitProperties.COMPACTION_THRESHOLD) != null) {
            long threshold =
                    (long) config.get(FileConstants.FileInitProperties.COMPACTION_THRESHOLD);
            if (threshold > 0) {
                compactionThreshold = threshold;
            }
        }
        if (config.get(FileConstants.FileInitProperties.MAX_SINGLE_FILE_SIZE) != null) {
            long maxSize = (long) config.get(FileConstants.FileInitProperties.MAX_SINGLE_FILE_SIZE);
            if (maxSize > 0) {
                maxSingleFileSize = maxSize;
            }
        }
        if (config.get(FileConstants.FileInitProperties.COMPACTION_BATCH_SIZE) != null) {
            long batchSize =
                    (long) config.get(FileConstants.FileInitProperties.COMPACTION_BATCH_SIZE);
            if (batchSize > 0 && batchSize >= maxSingleFileSize) {
                compactionBatchSize = batchSize;
            } else {
                throw new IllegalArgumentException(
                        "compaction batch size must be >= max single file size");
            }
        }
        if (config.get(FileConstants.FileInitProperties.COMPACTION_INTERVAL) != null) {
            long interval = (long) config.get(FileConstants.FileInitProperties.COMPACTION_INTERVAL);
            if (interval > 0) {
                compactionInterval = interval;
            }
        }
    }

    @Override
    public final void write(IMapFileData data, boolean flush) throws IOException {
        byte[] bytes = serializer.serialize(data);
        writeInternal(bytes, data, flush);
    }

    @Override
    public void setBlockSize(Long blockSize) {
        if (blockSize != null && blockSize > DEFAULT_BLOCK_SIZE) {
            this.blockSize = blockSize;
        }
    }

    protected abstract void writeInternal(byte[] bytes, IMapFileData data, boolean flush);

    protected boolean checkAndSetNextScheduleRotation(long sizeSoFar, boolean flush) {
        return sizeSoFar > blockSize || flush;
    }

    protected final synchronized void sortFlush() throws IOException {
        sortFlush(null);
    }

    protected final synchronized void sortFlush(Path tmpPath) throws IOException {
        if (writeBatch.isEmpty()) {
            return;
        }

        Collections.sort(writeBatch);

        long written = writeWithBatch(finalPath);

        fileNames.add(new CompactionFile(finalPath, written));
        finalPath = createNewPath();
        totalBytes.addAndGet(written);
        writeBatch.clear();
        if (tmpPath == null) {
            fs.delete(currentTmpPath, false);
            currentTmpPath = createNewTmpPath();
        } else {
            fs.delete(tmpPath, false);
        }
    }

    protected abstract long writeWithBatch(Path path) throws IOException;

    protected Path createNewPath() {
        return new Path(parentPath, "data_" + index.incrementAndGet());
    }

    protected Path createNewTmpPath() {
        return new Path(parentPath, "tmp_" + tmpIndex.incrementAndGet() + "_" + FILE_NAME);
    }

    @Override
    public void compaction(boolean force) throws IOException {
        if (totalBytes.get() < compactionThreshold && !force) return;

        long batchSize = 0;
        while (fileNames.size() > 1 && (totalBytes.get() >= compactionThreshold || force)) {
            CompactionFile f1 = fileNames.poll();
            CompactionFile f2 = fileNames.poll();

            long totalSize = f1.getSize() + f2.getSize();
            if (totalSize > maxSingleFileSize) {
                fileNames.add(f1);
                fileNames.add(f2);
                break;
            }

            Path outPath =
                    new Path(parentPath, "compaction_" + compactionIndex++ + "_" + FILE_NAME);
            long written = compactTwoFiles(f1.getPath(), f2.getPath(), outPath);

            totalBytes.addAndGet(written - totalSize);
            batchSize += totalSize;
            fileNames.add(new CompactionFile(outPath, written));

            fs.delete(f1.getPath(), false);
            fs.delete(f2.getPath(), false);

            force = false;
            if (batchSize >= compactionBatchSize) {
                try {
                    Thread.sleep(compactionInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                batchSize = 0;
            }
        }
    }

    protected final long compactTwoFiles(Path p1, Path p2, Path out) {
        long writtenBytes = 0L;
        try (IMapFileIterator it1 = openIterator(p1);
                IMapFileIterator it2 = openIterator(p2);
                FSDataOutputStream outStream = fs.create(out, true)) {

            IMapFileData d1 = it1.hasNext() ? it1.next() : null;
            IMapFileData d2 = it2.hasNext() ? it2.next() : null;

            byte[] lastKey = null;

            while (d1 != null || d2 != null) {
                IMapFileData current;
                if (d1 == null) {
                    current = d2;
                    d2 = it2.hasNext() ? it2.next() : null;
                } else if (d2 == null) {
                    current = d1;
                    d1 = it1.hasNext() ? it1.next() : null;
                } else {
                    if (d1.compareTo(d2) <= 0) {
                        current = d1;
                        d1 = it1.hasNext() ? it1.next() : null;
                    } else {
                        current = d2;
                        d2 = it2.hasNext() ? it2.next() : null;
                    }
                }

                byte[] key = current.getKey();
                if (lastKey != null && Arrays.equals(lastKey, key)) {
                    continue;
                }
                lastKey = Arrays.copyOf(key, key.length);

                byte[] ser = serializer.serialize(current);
                byte[] wrapper = WALDataUtils.wrapperBytes(ser);
                outStream.write(wrapper);
                writtenBytes += wrapper.length;
            }
            outStream.hflush();
        } catch (Exception e) {
            throw new IMapStorageException("compact files failed", e);
        }
        return writtenBytes;
    }

    protected final void recoverFromCrash() throws IOException, InterruptedException {
        FileStatus[] tmpFiles =
                fs.listStatus(parentPath, path -> path.getName().startsWith("tmp_"));

        if (tmpFiles == null) return;

        for (FileStatus fsStatus : tmpFiles) {
            Path tmp = fsStatus.getPath();
            long len = fsStatus.getLen();
            if (len <= 0) {
                fs.delete(tmp, false);
                continue;
            }
            byte[] all = new byte[(int) len];
            try (FSDataInputStream in = fs.open(tmp)) {
                in.readFully(all);
            }
            int pos = 0;
            List<IMapFileData> batch = new ArrayList<>();
            while (pos + WAL_DATA_METADATA_LENGTH < all.length) {
                byte[] meta = new byte[WAL_DATA_METADATA_LENGTH];
                System.arraycopy(all, pos, meta, 0, WAL_DATA_METADATA_LENGTH);
                int dataLen = WALDataUtils.byteArrayToInt(meta);
                pos += WAL_DATA_METADATA_LENGTH;
                if (pos + dataLen > all.length) break;
                byte[] data = new byte[dataLen];
                System.arraycopy(all, pos, data, 0, dataLen);
                IMapFileData fileData = serializer.deserialize(data, IMapFileData.class);
                batch.add(fileData);
                pos += dataLen;
            }
            if (!batch.isEmpty()) {
                writeBatch.addAll(batch);
                sortFlush(tmp);
                log.info(
                        "Recovered tmp file {} ({} records) -> data file written",
                        tmp,
                        batch.size());
                writeBatch.clear();
            }
        }
    }

    private IMapFileIterator openIterator(Path path) throws IOException {
        return new WALFileIterator(fs.open(path), serializer);
    }
}
