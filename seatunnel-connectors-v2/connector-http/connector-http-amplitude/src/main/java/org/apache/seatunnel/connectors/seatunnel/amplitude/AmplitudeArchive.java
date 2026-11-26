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

package org.apache.seatunnel.connectors.seatunnel.amplitude;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/** Bound central-directory allocation before handing an archive to the JDK ZIP reader. */
final class AmplitudeArchive {
    private static final long MAX_DIRECTORY_BYTES = 4L * 1024 * 1024;
    private static final int MAX_ENTRIES = 10000;

    private AmplitudeArchive() {}

    static void validateDirectory(Path archive) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(archive.toFile(), "r")) {
            int tailSize = (int) Math.min(file.length(), 65557);
            byte[] tail = new byte[tailSize];
            file.seek(file.length() - tailSize);
            file.readFully(tail);
            ByteBuffer bytes = ByteBuffer.wrap(tail).order(ByteOrder.LITTLE_ENDIAN);
            int end = tailSize - 22;
            for (; end >= 0; end--) {
                if (bytes.getInt(end) == 0x06054b50
                        && end + 22 + unsignedShort(bytes, end + 20) == tailSize) {
                    break;
                }
            }
            require(end >= 0);
            // The JDK also accepts some end records with inconsistent comment lengths. Reject
            // competing signatures so our preflight and ZipFile cannot select different records.
            for (int i = end + 1; i <= tailSize - 4; i++) {
                require(bytes.getInt(i) != 0x06054b50);
            }
            require(unsignedShort(bytes, end + 4) == 0 && unsignedShort(bytes, end + 6) == 0);
            long count = unsignedShort(bytes, end + 10);
            require(unsignedShort(bytes, end + 8) == count);
            long length = unsignedInt(bytes, end + 12);
            long offset = unsignedInt(bytes, end + 16);
            long directoryEnd = file.length() - tailSize + end;
            if (count == 65535 || length == 0xffffffffL || offset == 0xffffffffL) {
                long classicCount = count;
                long classicLength = length;
                long classicOffset = offset;
                require(directoryEnd >= 20);
                ByteBuffer locator = read(file, directoryEnd - 20, 20);
                require(
                        locator.getInt(0) == 0x07064b50
                                && locator.getInt(4) == 0
                                && locator.getInt(16) == 1);
                long zip64Offset = locator.getLong(8);
                require(zip64Offset >= 0 && zip64Offset <= directoryEnd - 76);
                ByteBuffer zip64 = read(file, zip64Offset, 56);
                long recordLength = zip64.getLong(4);
                require(
                        zip64.getInt(0) == 0x06064b50
                                && recordLength >= 44
                                && recordLength <= MAX_DIRECTORY_BYTES
                                && recordLength == directoryEnd - 20 - zip64Offset - 12);
                require(zip64.getInt(16) == 0 && zip64.getInt(20) == 0);
                count = zip64.getLong(32);
                require(zip64.getLong(24) == count);
                length = zip64.getLong(40);
                offset = zip64.getLong(48);
                require(
                        (classicCount == 65535 || classicCount == count)
                                && (classicLength == 0xffffffffL || classicLength == length)
                                && (classicOffset == 0xffffffffL || classicOffset == offset));
                directoryEnd = zip64Offset;
            }
            require(
                    count >= 0
                            && count <= MAX_ENTRIES
                            && length >= 0
                            && length <= MAX_DIRECTORY_BYTES
                            && offset >= 0
                            && offset <= directoryEnd
                            && length == directoryEnd - offset);
            long position = offset;
            for (long entry = 0; entry < count; entry++) {
                require(directoryEnd - position >= 46);
                ByteBuffer central = read(file, position, 46);
                require(central.getInt(0) == 0x02014b50 && unsignedShort(central, 34) == 0);
                long size =
                        46L
                                + unsignedShort(central, 28)
                                + unsignedShort(central, 30)
                                + unsignedShort(central, 32);
                require(size <= directoryEnd - position);
                position += size;
            }
            require(position == directoryEnd);
        }
    }

    private static ByteBuffer read(RandomAccessFile file, long position, int size)
            throws IOException {
        byte[] bytes = new byte[size];
        file.seek(position);
        file.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static int unsignedShort(ByteBuffer bytes, int offset) {
        return Short.toUnsignedInt(bytes.getShort(offset));
    }

    private static long unsignedInt(ByteBuffer bytes, int offset) {
        return Integer.toUnsignedLong(bytes.getInt(offset));
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) {
            throw new IOException("Invalid or excessive Amplitude ZIP central directory");
        }
    }
}
