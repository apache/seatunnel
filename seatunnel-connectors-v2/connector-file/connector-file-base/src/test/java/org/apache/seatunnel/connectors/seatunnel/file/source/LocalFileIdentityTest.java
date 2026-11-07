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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

/** Verifies local file identity across rename and replacement operations. */
class LocalFileIdentityTest {

    @TempDir private Path tempDir;

    @Test
    void testReaderAndDiscoveryUseIdenticalBoundedContentAnchors() throws Exception {
        assumeStableFileIdentity();
        Path file = tempDir.resolve("application.log");
        byte[] content = new byte[6000];
        java.util.Arrays.fill(content, (byte) 'a');
        content[0] = 'b';
        content[5999] = 'c';
        Files.write(file, content);
        try (RawLocalFileSystem fs = new RawLocalFileSystem()) {
            fs.initialize(java.net.URI.create("file:///"), new Configuration());
            for (long offset : new long[] {0L, 4L, 3000L, 6000L}) {
                try (FSDataInputStream input =
                        fs.open(new org.apache.hadoop.fs.Path(file.toUri()))) {
                    String anchor = LocalFileIdentity.contentAnchor(input, offset);
                    Assertions.assertEquals(
                            anchor, LocalFileIdentity.contentAnchor(file.toString(), offset));
                    Assertions.assertEquals(
                            anchor,
                            LocalFileIdentity.contentAnchor(file.toUri().toString(), offset));
                    Assertions.assertEquals(Math.min(offset, 4096L) * 2L, anchor.length());
                }
            }
        }
        Assertions.assertEquals("62616161", LocalFileIdentity.contentAnchor(file.toString(), 4L));
        Assertions.assertTrue(
                LocalFileIdentity.contentAnchor(file.toString(), 6000L).endsWith("63"));
    }

    @Test
    void testIdentityRemainsStableAcrossRename() throws Exception {
        assumeStableFileIdentity();
        Path activeFile = tempDir.resolve("application.log");
        Path rotatedFile = tempDir.resolve("application.log.1");
        Files.write(activeFile, "first\n".getBytes());

        String identity = LocalFileIdentity.read(activeFile.toString());
        Files.move(activeFile, rotatedFile);

        Assertions.assertEquals(identity, LocalFileIdentity.read(rotatedFile.toString()));
    }

    @Test
    void testIdentityChangesWhenPathIsReplaced() throws Exception {
        assumeStableFileIdentity();
        Path activeFile = tempDir.resolve("application.log");
        Path replacementFile = tempDir.resolve("replacement.log");
        Files.write(activeFile, "first\n".getBytes());
        Files.write(replacementFile, "replacement\n".getBytes());

        String identity = LocalFileIdentity.read(activeFile.toString());
        Files.move(replacementFile, activeFile, StandardCopyOption.REPLACE_EXISTING);

        Assertions.assertNotEquals(identity, LocalFileIdentity.read(activeFile.toString()));
    }

    @Test
    void testRejectsProviderWithoutStableFileIdentity() {
        BasicFileAttributes attributes = Mockito.mock(BasicFileAttributes.class);

        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () -> LocalFileIdentity.fromAttributes("application.log", attributes));

        Assertions.assertTrue(exception.getMessage().contains("fileKey"));
    }

    private void assumeStableFileIdentity() {
        try {
            LocalFileIdentity.read(tempDir.toString());
        } catch (IOException e) {
            Assumptions.assumeTrue(false, "The filesystem does not expose a stable file key");
        }
    }
}
