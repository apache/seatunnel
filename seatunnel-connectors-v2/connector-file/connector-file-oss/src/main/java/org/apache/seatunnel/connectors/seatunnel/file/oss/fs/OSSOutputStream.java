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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.event.ProgressEvent;
import com.aliyun.oss.event.ProgressListener;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Buffer write to local temp file system , and upload to OSS once stream is closed.
 */
public class OSSOutputStream extends OutputStream {
    private OutputStream backupStream;
    private File backupFile;
    private boolean closed;
    private String key;
    private String bucket;
    private Progressable progress;
    private OSSFileSystem fs;
    private FileSystem.Statistics statistics;
    private LocalDirAllocator lDirAlloc;

    public static final Logger LOG = OSSFileSystem.LOG;

    public OSSOutputStream(Configuration conf,
                           OSSFileSystem fs, String bucket, String key, Progressable progress,
                           FileSystem.Statistics statistics)
            throws IOException {
        this.bucket = bucket;
        this.key = key;
        this.progress = progress;
        this.fs = fs;
        this.statistics = statistics;

        lDirAlloc = new LocalDirAllocator(SmartOSSClientConfig.BUFFER_DIR);
        backupFile = lDirAlloc.createTmpFileForWrite("output-", LocalDirAllocator.SIZE_UNKNOWN, conf);
        closed = false;

        if (LOG.isDebugEnabled()) {
            LOG.debug("OutputStream for key '" + key + "' writing to tempfile: " +
                    this.backupFile);
        }

        this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    }

    @Override
    public void flush() throws IOException {
        backupStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        backupStream.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("OutputStream for key '" + key + "' closed. Now beginning upload");
        }
        try {
            final ObjectMetadata om = new ObjectMetadata();
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, backupFile);
            putObjectRequest.setMetadata(om);
            putObjectRequest.setProgressListener(new ProgressListener() {
                public void progressChanged(ProgressEvent progressEvent) {
                    switch (progressEvent.getEventType()) {
                        case TRANSFER_PART_COMPLETED_EVENT:
                            statistics.incrementWriteOps(1);
                            break;
                        default:
                            break;
                    }
                }
            });

            fs.getOSSClient().putObject(putObjectRequest);

            // This will delete unnecessary fake parent directories
            fs.finishedWrite(key);
        } catch (OSSException | ClientException e) {
            throw new IOException(e);
        } finally {
            if (!backupFile.delete()) {
                LOG.warn("Could not delete temporary oss file: {}", backupFile);
            }
            super.close();
            closed = true;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("OutputStream for key '" + key + "' upload complete");
        }
    }

    @Override
    public void write(int b) throws IOException {
        backupStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        backupStream.write(b, off, len);
    }
}
