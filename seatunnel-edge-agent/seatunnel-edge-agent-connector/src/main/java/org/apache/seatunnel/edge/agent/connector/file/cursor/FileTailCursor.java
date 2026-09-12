/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.connector.file.cursor;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileTailCursor implements Closeable {

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private final Path path;
    private final Charset charset;
    private RandomAccessFile raf;
    private long currentOffset;
    private long inode;
    private long lastActivityMs;

    private final byte[] readBuffer = new byte[DEFAULT_BUFFER_SIZE];
    private int bufPos;
    private int bufLen;

    public FileTailCursor(Path path, Charset charset) {
        this.path = path;
        this.charset = charset;
        this.currentOffset = 0L;
        this.inode = 0L;
        this.lastActivityMs = 0L;
        this.bufPos = 0;
        this.bufLen = 0;
    }

    /** Open the file and detect inode. If {@code seekOffset > 0}, seek to that position. */
    public void open(long seekOffset) throws IOException {
        this.raf = new RandomAccessFile(path.toFile(), "r");
        this.inode = detectInode(path);
        if (seekOffset > 0) {
            raf.seek(seekOffset);
            this.currentOffset = seekOffset;
        }
        this.bufPos = 0;
        this.bufLen = 0;
        this.lastActivityMs = System.currentTimeMillis();
    }

    /**
     * Read the next complete line. Returns null at EOF or when a partial line is buffered without a
     * trailing newline (tail-follow semantics).
     *
     * <p>Updates {@code currentOffset} after a successful read of a line terminated by {@code \n}
     * or {@code \r\n}.
     */
    public String readLine() throws IOException {
        if (raf == null) {
            throw new IOException("FileTailCursor is not open: " + path);
        }
        long lineStartOffset = raf.getFilePointer() - (bufLen - bufPos);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (true) {
            if (bufPos >= bufLen) {
                bufLen = raf.read(readBuffer, 0, readBuffer.length);
                bufPos = 0;
                if (bufLen <= 0) {
                    bufLen = 0;
                    if (baos.size() == 0) {
                        return null;
                    }
                    // Partial line at EOF: seek back to line start
                    raf.seek(lineStartOffset);
                    return null;
                }
            }
            byte b = readBuffer[bufPos++];
            if (b == '\n') {
                byte[] arr = baos.toByteArray();
                int len = arr.length;
                if (len > 0 && arr[len - 1] == '\r') {
                    len--;
                }
                this.currentOffset = raf.getFilePointer() - (bufLen - bufPos);
                this.lastActivityMs = System.currentTimeMillis();
                return new String(arr, 0, len, charset);
            }
            baos.write(b);
        }
    }

    /** Check if the file has been rotated (inode changed). */
    public boolean hasRotated() throws IOException {
        long currentInode = detectInode(path);
        return currentInode != this.inode && currentInode != 0;
    }

    /** Reopen the file after rotation (new inode, seek to 0). */
    public void reopen() throws IOException {
        closeQuietly();
        this.raf = new RandomAccessFile(path.toFile(), "r");
        this.inode = detectInode(path);
        this.currentOffset = 0;
        this.bufPos = 0;
        this.bufLen = 0;
        this.lastActivityMs = System.currentTimeMillis();
    }

    public long offset() {
        return currentOffset;
    }

    public long inode() {
        return inode;
    }

    public Path path() {
        return path;
    }

    public long lastActivityMs() {
        return lastActivityMs;
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
            raf = null;
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // ignore
        }
    }

    /**
     * Detect file inode using unix:ino attribute.
     *
     * @return inode number, or 0 on Windows or if attribute is unavailable
     */
    private static long detectInode(Path path) {
        try {
            Object ino = Files.getAttribute(path, "unix:ino");
            if (ino instanceof Number) {
                return ((Number) ino).longValue();
            }
        } catch (Exception ignored) {
            // not Unix or attribute unsupported
        }
        return 0L;
    }
}
