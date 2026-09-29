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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.system;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import com.jcraft.jsch.ChannelSftp;

import java.io.IOException;
import java.io.InputStream;

/** SFTP FileSystem input stream. */
public class SFTPInputStream extends FSInputStream {

    public static final String E_SEEK_NOT_SUPPORTED = "Seek not supported";
    public static final String E_CLIENT_NULL = "SFTP client null or not connected";
    public static final String E_NULL_INPUT_STREAM = "Null InputStream";
    public static final String E_STREAM_CLOSED = "Stream closed";
    public static final String E_CLIENT_NOT_CONNECTED = "Client not connected";

    private InputStream wrappedStream;
    private ChannelSftp channel;
    private SFTPConnectionPool connectionPool;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SFTPInputStream(
            InputStream stream,
            ChannelSftp channel,
            SFTPConnectionPool connectionPool,
            FileSystem.Statistics stats) {

        if (stream == null) {
            throw new IllegalArgumentException(E_NULL_INPUT_STREAM);
        }
        if (channel == null || !channel.isConnected()) {
            throw new IllegalArgumentException(E_CLIENT_NULL);
        }
        this.wrappedStream = stream;
        this.channel = channel;
        this.connectionPool = connectionPool;
        this.stats = stats;

        this.pos = 0;
        this.closed = false;
    }

    @Override
    public void seek(long position) throws IOException {
        throw new IOException(E_SEEK_NOT_SUPPORTED);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException(E_SEEK_NOT_SUPPORTED);
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int byteRead = wrappedStream.read();
        if (byteRead >= 0) {
            pos++;
        }
        if (stats != null & byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int result = wrappedStream.read(buf, off, len);
        if (result > 0) {
            pos += result;
        }
        if (stats != null & result > 0) {
            stats.incrementBytesRead(result);
        }

        return result;
    }

    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        IOException closeFailure = null;
        try {
            wrappedStream.close();
            super.close();
        } catch (IOException e) {
            closeFailure = e;
        } finally {
            closed = true;
            try {
                connectionPool.disconnect(channel);
            } catch (IOException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
