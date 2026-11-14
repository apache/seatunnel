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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AmplitudeArchiveTest {
    @TempDir Path directory;

    private Path write(byte[] bytes) throws IOException {
        Path path = directory.resolve("export.zip");
        Files.write(path, bytes);
        return path;
    }

    @Test
    void acceptsClassicAndEmptyArchives() throws Exception {
        assertDoesNotThrow(
                () ->
                        AmplitudeArchive.validateDirectory(
                                write(
                                        AmplitudeSourceReaderTest.zip(
                                                AmplitudeSourceReaderTest.gzip("")))));
        assertDoesNotThrow(
                () -> AmplitudeArchive.validateDirectory(write(AmplitudeSourceReaderTest.zip())));
    }

    @Test
    void acceptsZip64DirectoryAndRejectsInvalidLocator() throws Exception {
        byte[] bytes = zip64();
        assertDoesNotThrow(() -> AmplitudeArchive.validateDirectory(write(bytes)));
        bytes[bytes.length - 42] ^= 1;
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(write(bytes)));
    }

    static byte[] zip64() throws IOException {
        byte[] classic = AmplitudeSourceReaderTest.zip(AmplitudeSourceReaderTest.gzip(""));
        ByteBuffer end = ByteBuffer.wrap(classic).order(ByteOrder.LITTLE_ENDIAN);
        int position = classic.length - 22;
        long length = Integer.toUnsignedLong(end.getInt(position + 12));
        long offset = Integer.toUnsignedLong(end.getInt(position + 16));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(classic, 0, position);
        ByteBuffer zip64 = ByteBuffer.allocate(76).order(ByteOrder.LITTLE_ENDIAN);
        zip64.putInt(0x06064b50).putLong(44).putShort((short) 45).putShort((short) 45);
        zip64.putInt(0).putInt(0).putLong(1).putLong(1).putLong(length).putLong(offset);
        zip64.putInt(0x07064b50).putInt(0).putLong(position).putInt(1);
        output.write(zip64.array());
        end.putShort(position + 8, (short) 0xffff).putShort(position + 10, (short) 0xffff);
        end.putInt(position + 12, -1).putInt(position + 16, -1);
        output.write(classic, position, 22);
        return output.toByteArray();
    }

    @Test
    void rejectsTruncationAndMultipleDisks() throws Exception {
        byte[] bytes = AmplitudeSourceReaderTest.zip(AmplitudeSourceReaderTest.gzip(""));
        assertThrows(
                IOException.class,
                () ->
                        AmplitudeArchive.validateDirectory(
                                write(Arrays.copyOf(bytes, bytes.length - 1))));
        bytes[bytes.length - 18] = 1;
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(write(bytes)));
    }

    @Test
    void rejectsForgedCountOffsetAndZip64Range() throws Exception {
        byte[] bytes = AmplitudeSourceReaderTest.zip(AmplitudeSourceReaderTest.gzip(""));
        ByteBuffer end = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        end.putShort(bytes.length - 14, (short) 10001);
        end.putShort(bytes.length - 12, (short) 10001);
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(write(bytes)));
        byte[] zip64 = zip64();
        ByteBuffer.wrap(zip64)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(zip64.length - 34, Long.MAX_VALUE);
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(write(zip64)));
    }

    @Test
    void rejectsClassicZip64DisagreementAndAmbiguousEndInComment() throws Exception {
        byte[] inconsistent = zip64();
        ByteBuffer.wrap(inconsistent)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(inconsistent.length - 10, 200000000);
        assertThrows(
                IOException.class, () -> AmplitudeArchive.validateDirectory(write(inconsistent)));
        byte[] classic = AmplitudeSourceReaderTest.zip(AmplitudeSourceReaderTest.gzip(""));
        byte[] ambiguous = Arrays.copyOf(classic, classic.length + 22);
        ByteBuffer bytes = ByteBuffer.wrap(ambiguous).order(ByteOrder.LITTLE_ENDIAN);
        bytes.putShort(classic.length - 2, (short) 22);
        bytes.putInt(classic.length, 0x06054b50);
        bytes.putShort(ambiguous.length - 2, (short) 1);
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(write(ambiguous)));
    }

    @Test
    void boundsLargeMetadataUsingConstantSizeReads() throws Exception {
        Path archive = directory.resolve("many-comments.zip");
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
            for (int i = 0; i < 1000; i++) {
                ZipEntry entry = new ZipEntry(i + ".gz");
                entry.setComment(String.join("", Collections.nCopies(8192, "x")));
                zip.putNextEntry(entry);
                zip.write(AmplitudeSourceReaderTest.gzip(""));
                zip.closeEntry();
            }
        }
        assertThrows(IOException.class, () -> AmplitudeArchive.validateDirectory(archive));
    }
}
