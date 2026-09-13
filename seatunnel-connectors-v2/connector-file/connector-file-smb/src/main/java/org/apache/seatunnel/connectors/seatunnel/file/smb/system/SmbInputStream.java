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

package org.apache.seatunnel.connectors.seatunnel.file.smb.system;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

import java.io.IOException;
import java.io.InputStream;

public class SmbInputStream extends FSInputStream {

    private InputStream wrappedStream;
    private File smbFile;
    private DiskShare diskShare;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SmbInputStream(
            InputStream stream, File smbFile, DiskShare diskShare, FileSystem.Statistics stats) {
        if (stream == null) {
            throw new IllegalArgumentException("Null InputStream");
        }
        this.wrappedStream = stream;
        this.smbFile = smbFile;
        this.diskShare = diskShare;
        this.stats = stats;
        this.pos = 0;
        this.closed = false;
    }

    @Override
    public void seek(long position) throws IOException {
        throw new IOException("Seek not supported");
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException("Seek not supported");
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        int byteRead = wrappedStream.read();
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
        if (closed) {
            throw new IOException("Stream closed");
        }
        int result = wrappedStream.read(buf, off, len);
        if (result > 0) {
            pos += result;
        }
        if (stats != null && result > 0) {
            stats.incrementBytesRead(result);
        }
        return result;
    }

    @Override
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
                if (smbFile != null) {
                    smbFile.close();
                }
            } catch (Exception e) {
                if (closeFailure == null) {
                    closeFailure = new IOException(e);
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
            try {
                if (diskShare != null) {
                    diskShare.close();
                }
            } catch (Exception e) {
                if (closeFailure == null) {
                    closeFailure = new IOException(e);
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
