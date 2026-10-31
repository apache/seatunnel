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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;

/** Bounded write-back pages over a private ephemeral file; caller serializes access. */
final class BoundedSnapshotFile {
    private final FileChannel file;
    private final int pageSize;
    private final int maxPages;
    private final LinkedHashMap<Long, Page> pages = new LinkedHashMap<>(16, .75F, true);
    private long length;
    private long physicalLength;
    private IOException failure;

    BoundedSnapshotFile(RandomAccessFile file, int pageSize, int maxPages) throws IOException {
        this.file = file.getChannel();
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        length = physicalLength = this.file.size();
    }

    long length() throws IOException {
        ensureOpen();
        return length;
    }

    private void ensureOpen() throws IOException {
        if (failure != null) throw new IOException("Previous spool I/O failed", failure);
        if (!file.isOpen()) throw new IOException("Spool file closed");
    }

    void read(long offset, byte[] target, int start, int count) throws IOException {
        ensureOpen();
        if (offset < 0 || count < 0 || offset > length - count)
            throw new EOFException("Invalid spool extent");
        while (count > 0) {
            long pageId = offset / pageSize;
            int within = (int) (offset % pageSize);
            int part = Math.min(count, pageSize - within);
            System.arraycopy(page(pageId).bytes, within, target, start, part);
            offset += part;
            start += part;
            count -= part;
        }
    }

    void write(long offset, byte[] source, int start, int count) throws IOException {
        ensureOpen();
        if (offset < 0 || count < 0 || offset > Long.MAX_VALUE - count)
            throw new IOException("Invalid spool extent");
        long end = offset + count;
        while (count > 0) {
            long pageId = offset / pageSize;
            int within = (int) (offset % pageSize);
            int part = Math.min(count, pageSize - within);
            Page page = page(pageId);
            System.arraycopy(source, start, page.bytes, within, part);
            page.valid = Math.max(page.valid, within + part);
            page.dirty = true;
            offset += part;
            start += part;
            count -= part;
        }
        length = Math.max(length, end);
    }

    private Page page(long id) throws IOException {
        Page page = pages.get(id);
        if (page != null) return page;
        if (pages.size() == maxPages) {
            Map.Entry<Long, Page> eldest = pages.entrySet().iterator().next();
            persist(eldest.getKey(), eldest.getValue());
            pages.remove(eldest.getKey());
        }
        page = new Page(pageSize);
        long offset = id * pageSize;
        if (offset < physicalLength) {
            int expected = (int) Math.min(pageSize, physicalLength - offset);
            ByteBuffer destination = ByteBuffer.wrap(page.bytes, 0, expected);
            int read = 0;
            while (destination.hasRemaining()) {
                int part = file.read(destination, offset + read);
                if (part <= 0) throw new EOFException("Truncated private spool page");
                read += part;
            }
            page.valid = expected;
        }
        pages.put(id, page);
        return page;
    }

    private void persist(long id, Page page) throws IOException {
        if (!page.dirty) return;
        long offset = id * pageSize;
        ByteBuffer source = ByteBuffer.wrap(page.bytes, 0, page.valid);
        int written = 0;
        try {
            while (source.hasRemaining()) {
                int part = file.write(source, offset + written);
                if (part <= 0) throw new IOException("No progress writing private spool page");
                written += part;
            }
            physicalLength = Math.max(physicalLength, offset + written);
            page.dirty = false;
        } catch (IOException e) {
            failure = e;
            throw e;
        }
    }

    void flush() throws IOException {
        ensureOpen();
        if (file.size() < physicalLength) throw new EOFException("Truncated private spool file");
        for (Map.Entry<Long, Page> entry : pages.entrySet())
            persist(entry.getKey(), entry.getValue());
    }

    void invalidateCleanPages() throws IOException {
        flush();
        pages.clear();
    }

    void discard() {
        pages.clear();
    }

    private static final class Page {
        final byte[] bytes;
        int valid;
        boolean dirty;

        Page(int size) {
            bytes = new byte[size];
        }
    }
}
