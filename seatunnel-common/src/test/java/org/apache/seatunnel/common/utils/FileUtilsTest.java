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

import lombok.NonNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtilsTest {
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
}
