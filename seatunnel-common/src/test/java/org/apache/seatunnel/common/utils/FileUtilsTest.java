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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.NonNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtilsTest {

    /** The character a UTF-8 decoder emits when it is handed a partial character. */
    private static final String REPLACEMENT_CHAR = "\uFFFD";

    @Test
    public void testGetFileLineNumber() throws Exception {
        String filePath = "/tmp/test/file_utils/file1.txt";
        filePath = filePath.replace("/", File.separator);
        writeTestDataToFile(filePath);

        Long fileLineNumber = FileUtils.getFileLineNumber(filePath);
        Assertions.assertEquals(100, fileLineNumber);
    }

    @Test
    public void testGetFileLineNumberFromDir() throws Exception {
        String rootPath = "/tmp/test/file_utils1";
        String dirPath1 = rootPath + "/dir1";
        String dirPath2 = rootPath + "/dir2";

        String file1 = dirPath1 + "/file1.txt";
        String file2 = dirPath1 + "/file2.txt";
        String file3 = dirPath2 + "/file3.txt";
        String file4 = dirPath2 + "/file4.txt";

        file1 = file1.replace("/", File.separator);
        file2 = file2.replace("/", File.separator);
        file3 = file3.replace("/", File.separator);
        file4 = file4.replace("/", File.separator);

        FileUtils.createNewFile(file1);
        FileUtils.createNewFile(file2);
        FileUtils.createNewFile(file3);
        FileUtils.createNewFile(file4);

        writeTestDataToFile(file1);
        writeTestDataToFile(file2);
        writeTestDataToFile(file3);
        writeTestDataToFile(file4);

        Long lines = FileUtils.getFileLineNumberFromDir(rootPath);
        Assertions.assertEquals(100 * 4, lines);
    }

    @Test
    void throwExpectedException() {
        String root = System.getProperty("java.io.tmpdir");
        Path path = Paths.get(root, "not", "existed", "path");
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> FileUtils.writeStringToFile(path.toString(), ""));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-22], ErrorDescription:[SeaTunnel write file '"
                        + path
                        + "' failed, because it not existed.]",
                exception.getMessage());

        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> FileUtils.readFileToStr(path));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-01], ErrorDescription:[SeaTunnel read file '"
                        + path
                        + "' failed.]",
                exception2.getMessage());
        Assertions.assertInstanceOf(NoSuchFileException.class, exception2.getCause());
        Assertions.assertEquals(path.toString(), exception2.getCause().getMessage());

        Path path2 = Paths.get(root, "not", "existed", "path2");
        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> FileUtils.getFileLineNumber(path2.toString()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-01], ErrorDescription:[SeaTunnel read file '"
                        + path2
                        + "' failed.]",
                exception3.getMessage());
        Assertions.assertInstanceOf(NoSuchFileException.class, exception3.getCause());
        Assertions.assertEquals(path2.toString(), exception3.getCause().getMessage());
    }

    public void writeTestDataToFile(@NonNull String filePath) throws IOException {
        FileUtils.createNewFile(filePath);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = 0; i < 100; i++) {
                bw.write(i + "");
                bw.newLine();
            }
        }
    }

    @Test
    public void createNewFile() throws IOException {
        // create new file
        FileUtils.createNewFile("/tmp/test.txt");
        Assertions.assertEquals("", FileUtils.readFileToStr(Paths.get("/tmp/test.txt")));

        // delete exist file and create new file
        FileUtils.writeStringToFile("/tmp/test2.txt", "test");
        Path test2 = Paths.get("/tmp/test2.txt");
        Assertions.assertEquals("test", FileUtils.readFileToStr(test2).trim());
        FileUtils.createNewFile("/tmp/test2.txt");
        Assertions.assertEquals("", FileUtils.readFileToStr(test2));

        // create new file with not exist folder
        FileUtils.createNewFile("/tmp/newfolder/test.txt");
        Assertions.assertEquals("", FileUtils.readFileToStr(Paths.get("/tmp/newfolder/test.txt")));

        FileUtils.createNewFile("/tmp/newfolder/newfolder2/newfolde3/test.txt");
        Assertions.assertEquals(
                "",
                FileUtils.readFileToStr(Paths.get("/tmp/newfolder/newfolder2/newfolde3/test.txt")));
    }

    @Test
    void readFileTailToStrReturnsWholeFileWhenItFitsTheLimit(@TempDir Path tempDir)
            throws IOException {
        Path file = tempDir.resolve("small.log");
        String content = "first line\nsecond line\n";
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        long size = Files.size(file);

        Assertions.assertEquals(content, FileUtils.readFileTailToStr(file, size + 1));
        // The limit is inclusive, so a file of exactly the limit is still returned whole.
        Assertions.assertEquals(content, FileUtils.readFileTailToStr(file, size));
    }

    @Test
    void readFileTailToStrTreatsNonPositiveLimitAsUnlimited(@TempDir Path tempDir)
            throws IOException {
        Path file = tempDir.resolve("unlimited.log");
        String content = "first line\nsecond line\n";
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(content, FileUtils.readFileTailToStr(file, 0));
        Assertions.assertEquals(content, FileUtils.readFileTailToStr(file, -1));
    }

    @Test
    void readFileTailToStrHandlesEmptyFile(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("empty.log");
        Files.write(file, new byte[0]);

        Assertions.assertEquals("", FileUtils.readFileTailToStr(file, 16));
    }

    @Test
    void readFileTailToStrKeepsTheTailAlignedToALineBoundary(@TempDir Path tempDir)
            throws IOException {
        Path file = tempDir.resolve("aligned.log");
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            content.append("line ").append(i).append('\n');
        }
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));

        String tail = FileUtils.readFileTailToStr(file, 40);

        Assertions.assertTrue(content.toString().endsWith(tail), "tail must be a suffix");
        Assertions.assertTrue(tail.length() < 40, "tail must be smaller than the limit");
        Assertions.assertTrue(tail.startsWith("line "), "tail must start at a line boundary");
        Assertions.assertTrue(tail.endsWith("line 99\n"), "tail must reach the end of the file");
    }

    @Test
    void readFileTailToStrNeverSplitsAMultiByteCharacter(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("cjk.log");
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            content.append("第").append(i).append("行日志内容").append('\n');
        }
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(Files.size(file) > 400, "test data must exceed the limits swept");

        // Every character here is three bytes wide, so sweeping the limit guarantees that some of
        // these cut points land inside a character.
        for (long maxBytes = 100; maxBytes <= 400; maxBytes++) {
            String tail = FileUtils.readFileTailToStr(file, maxBytes);
            Assertions.assertFalse(
                    tail.contains(REPLACEMENT_CHAR), "character split at maxBytes=" + maxBytes);
            Assertions.assertTrue(
                    content.toString().endsWith(tail), "not a suffix at maxBytes=" + maxBytes);
            Assertions.assertTrue(
                    tail.startsWith("第"), "not aligned to a line at maxBytes=" + maxBytes);
        }
    }

    @Test
    void readFileTailToStrHandlesALineLongerThanTheLimit(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("single-line.log");
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            content.append("啊");
        }
        Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));

        // There is no line break to align to. 100 is not a multiple of the 3-byte character width,
        // so the raw slice would begin on a continuation byte.
        String tail = FileUtils.readFileTailToStr(file, 100);

        Assertions.assertFalse(
                tail.contains(REPLACEMENT_CHAR), "character split without a line boundary");
        Assertions.assertTrue(content.toString().endsWith(tail), "tail must be a suffix");
        Assertions.assertEquals(33, tail.length());
    }
}
