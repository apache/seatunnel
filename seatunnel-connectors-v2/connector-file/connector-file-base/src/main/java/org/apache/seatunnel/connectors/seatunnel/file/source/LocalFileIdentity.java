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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

/** Resolves the stable identity used to follow a local file across path changes. */
public final class LocalFileIdentity {

    private LocalFileIdentity() {}

    /**
     * Returns an identity that remains stable when the file is renamed.
     *
     * <p>A file key is required because it identifies the underlying filesystem object directly.
     * Creation time is not a safe fallback: some providers preserve it when a different file
     * replaces the path.
     */
    public static String read(String filePath) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(toNioPath(filePath), BasicFileAttributes.class);
        return fromAttributes(filePath, attributes);
    }

    /** Reads the bounded content sample used by local tail split checkpoints. */
    public static String contentAnchor(String filePath, long offset) throws IOException {
        try (FileChannel channel = FileChannel.open(toNioPath(filePath), StandardOpenOption.READ)) {
            return contentAnchor(
                    new DataInputStream(Channels.newInputStream(channel)),
                    channel::position,
                    offset);
        }
    }

    /** Uses the same anchor encoding for discovery and reader-side validation. */
    public static String contentAnchor(FSDataInputStream input, long offset) throws IOException {
        return contentAnchor(input, input::seek, offset);
    }

    private static String contentAnchor(DataInput input, Seek seek, long offset)
            throws IOException {
        int prefixLength = (int) Math.min(2048L, offset);
        int suffixLength = (int) Math.min(2048L, Math.max(0L, offset - prefixLength));
        byte[] anchor = new byte[prefixLength + suffixLength];
        if (prefixLength > 0) {
            seek.to(0L);
            input.readFully(anchor, 0, prefixLength);
        }
        if (suffixLength > 0) {
            seek.to(offset - suffixLength);
            input.readFully(anchor, prefixLength, suffixLength);
        }
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[anchor.length * 2];
        for (int i = 0; i < anchor.length; i++) {
            int current = anchor[i] & 0xff;
            encoded[i * 2] = digits[current >>> 4];
            encoded[i * 2 + 1] = digits[current & 0x0f];
        }
        return new String(encoded);
    }

    @FunctionalInterface
    private interface Seek {
        void to(long offset) throws IOException;
    }

    static String fromAttributes(String filePath, BasicFileAttributes attributes)
            throws IOException {
        Object fileKey = attributes.fileKey();
        if (fileKey == null) {
            throw new IOException(
                    "Local filesystem does not expose BasicFileAttributes.fileKey() for "
                            + filePath);
        }
        return "file-key:" + fileKey;
    }

    private static java.nio.file.Path toNioPath(String filePath) {
        URI uri = new Path(filePath).toUri();
        if (uri.getScheme() == null) {
            return Paths.get(filePath);
        }
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Not a local file path: " + filePath);
        }
        return Paths.get(uri);
    }
}
