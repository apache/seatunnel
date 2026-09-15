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

import com.hierynomus.smbj.share.File;

import java.io.IOException;

public class SmbInputStream extends FSInputStream {

    private final File smbFile;
    private final SmbConnection smbConnection;
    private final FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SmbInputStream(File smbFile, SmbConnection smbConnection, FileSystem.Statistics stats) {
        if (smbFile == null) {
            throw new IllegalArgumentException("Null SMB File");
        }
        this.smbFile = smbFile;
        this.smbConnection = smbConnection;
        this.stats = stats;
        this.pos = 0;
        this.closed = false;
    }

    @Override
    public synchronized void seek(long position) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (position < 0) {
            throw new IOException("Negative seek position: " + position);
        }
        this.pos = position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        byte[] buf = new byte[1];
        int result = smbFile.read(buf, pos);
        if (result <= 0) {
            return -1;
        }
        pos++;
        if (stats != null) {
            stats.incrementBytesRead(1);
        }
        return buf[0] & 0xFF;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (len == 0) {
            return 0;
        }
        byte[] tmp = new byte[len];
        int result = smbFile.read(tmp, pos);
        if (result <= 0) {
            return -1;
        }
        System.arraycopy(tmp, 0, buf, off, result);
        pos += result;
        if (stats != null) {
            stats.incrementBytesRead(result);
        }
        return result;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException closeFailure = null;
        try {
            super.close();
        } catch (IOException e) {
            closeFailure = e;
        }
        try {
            smbFile.close();
        } catch (Exception e) {
            if (closeFailure == null) {
                closeFailure = new IOException(e);
            } else {
                closeFailure.addSuppressed(e);
            }
        }
        try {
            smbConnection.close();
        } catch (Exception e) {
            if (closeFailure == null) {
                closeFailure = new IOException(e);
            } else {
                closeFailure.addSuppressed(e);
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
