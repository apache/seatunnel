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

package org.apache.seatunnel.engine.server.rest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class LogContentReaderTest {

    @Test
    void readReturnsAFileWithinTheLimitUnchanged(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("small.log");
        String content = "first line\nsecond line\n";
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(content, LogContentReader.read(file, 1024));
    }

    @Test
    void readReturnsAFileUnchangedWhenThereIsNoLimit(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("unlimited.log");
        String content = "first line\nsecond line\n";
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(content, LogContentReader.read(file, -1));
        Assertions.assertEquals(content, LogContentReader.read(file, 0));
    }

    @Test
    void readAnnouncesThatALargeFileWasTruncated(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("large.log");
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            content.append("line ").append(i).append('\n');
        }
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));
        long size = Files.size(file);

        String response = LogContentReader.read(file, 40);

        String[] lines = response.split("\n", 2);
        Assertions.assertTrue(
                lines[0].startsWith("[SeaTunnel] Log truncated:"),
                "a truncated response has to say so on its first line, got: " + lines[0]);
        Assertions.assertTrue(
                lines[0].contains("last 40 bytes of " + size),
                "the notice has to name both sizes, got: " + lines[0]);
        Assertions.assertTrue(
                lines[0].contains("log-response-max-size-mb"),
                "the notice has to name the option that controls the limit, got: " + lines[0]);
        // Everything after the notice is the log itself, unchanged.
        Assertions.assertTrue(
                content.toString().endsWith(lines[1]), "the body has to be the file's own tail");
        Assertions.assertTrue(lines[1].startsWith("line "), "the body starts at a line boundary");
    }
}
