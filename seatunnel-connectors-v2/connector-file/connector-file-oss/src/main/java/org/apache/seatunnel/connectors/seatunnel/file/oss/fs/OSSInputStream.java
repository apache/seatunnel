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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * InputStream implementation for OSS Filesystem
 */
public class OSSInputStream extends FSInputStream {
    private long pos;
    private boolean closed;
    private FileSystem.Statistics stats;
    private String bucket;
    private String key;
    private long contentLength;
    public static final Logger LOG = OSSFileSystem.LOG;
    private OSSClient ossClient;
    private InputStream inputStream;

    public OSSInputStream(String bucket, String key, long contentLength, OSSClient client,
                          FileSystem.Statistics stats) {
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.ossClient = client;
        this.stats = stats;
        this.pos = 0;
        this.closed = false;
        this.inputStream = null;
    }

    private void openIfNeeded() throws IOException {
        if (inputStream == null) {
            reopen(0);
        }
    }

    private synchronized void reopen(long pos) throws IOException {

        if (inputStream != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Aborting old stream " + "to open at pos " + pos);
            }
            inputStream.close();
        }

        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
        }

        if (contentLength > 0 && pos > contentLength - 1) {
            throw new EOFException(
                    FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + pos);
        }

        LOG.debug("Actually opening file " + key + " at pos " + pos);

        GetObjectRequest request = new GetObjectRequest(bucket, key);
        request.setRange(pos, contentLength - 1);

        inputStream = ossClient.getObject(request).getObjectContent();

        if (inputStream == null) {
            throw new IOException("Null IO stream");
        }

        this.pos = pos;
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();

        if (this.pos == pos) {
            return;
        }

        LOG.debug("Reopening " + this.key + " to seek to new offset " + (pos - this.pos));
        reopen(pos);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();

        openIfNeeded();

        int byteRead;
        try {
            byteRead = inputStream.read();
        } catch (SocketTimeoutException e) {
            LOG.info("Got timeout while trying to read from stream, trying to recover " + e);
            reopen(pos);
            byteRead = inputStream.read();
        } catch (SocketException e) {
            LOG.info("Got socket exception while trying to read from stream, trying to recover " + e);
            reopen(pos);
            byteRead = inputStream.read();
        }

        if (byteRead >= 0) {
            pos++;
        }

        if (stats != null && byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        checkNotClosed();

        openIfNeeded();

        int byteRead;
        try {
            byteRead = inputStream.read(buf, off, len);
        } catch (SocketTimeoutException e) {
            LOG.info("Got timeout while trying to read from stream, trying to recover " + e);
            reopen(pos);
            byteRead = inputStream.read(buf, off, len);
        } catch (SocketException e) {
            LOG.info("Got socket exception while trying to read from stream, trying to recover " + e);
            reopen(pos);
            byteRead = inputStream.read(buf, off, len);
        }

        if (byteRead > 0) {
            pos += byteRead;
        }

        if (stats != null && byteRead > 0) {
            stats.incrementBytesRead(byteRead);
        }

        return byteRead;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        closed = true;
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();

        long remaining = this.contentLength - this.pos;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
