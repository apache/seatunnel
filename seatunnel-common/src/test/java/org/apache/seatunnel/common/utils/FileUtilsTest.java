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

import lombok.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

    public void writeTestDataToFile(@NonNull String filePath) throws IOException {
        FileUtils.createNewFile(filePath);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = 0; i < 100; i++) {
                bw.write(i + "");
                bw.newLine();
            }
        }
    }
}
