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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public class FileSystemUtilsTest {
    private final Configuration conf = new Configuration();
    private final URL resource = FileSystemUtils.class.getResource("/test.txt");

    @BeforeEach
    void init() {
        FileSystemUtils.CONF = conf;
    }

    @Test
    void testGetFileSystem() throws IOException {
        assert resource != null;
        FileSystem fileSystem = FileSystemUtils.getFileSystem(resource.getPath());
        fileSystem.close();
    }

    @Test
    void testGetOutputString() throws IOException {
        assert resource != null;
        FSDataOutputStream outputStream = FileSystemUtils.getOutputStream(resource.getPath());
        outputStream.close();
    }

    @Test
    void testCreateAndDeleteFile() throws IOException {
        assert resource != null;
        String newFileName = "test.file";
        String path = resource.getPath();
        String parentPath = new File(path).getParent();
        String newFilePath = String.join(File.separator, parentPath, newFileName);
        FileSystemUtils.createFile(newFilePath);
        Assertions.assertTrue(FileSystemUtils.fileExist(newFilePath));
        FileSystemUtils.deleteFile(newFilePath);
        Assertions.assertFalse(FileSystemUtils.fileExist(newFilePath));
    }

    @Test
    void testRenameFile() throws IOException {
        assert resource != null;
        String oldFileName = "test.file";
        String newFileName = "test.new.file";
        String path = resource.getPath();
        String parentPath = new File(path).getParent();
        String oldFilePath = String.join(File.separator, parentPath, oldFileName);
        String newFilePath = String.join(File.separator, parentPath, newFileName);
        FileSystemUtils.createFile(oldFilePath);
        FileSystemUtils.renameFile(oldFilePath, newFilePath, true);
        Assertions.assertTrue(FileSystemUtils.fileExist(newFilePath));
        FileSystemUtils.deleteFile(newFilePath);
    }

    @Test
    void testCreateDirectory() throws IOException {
        assert resource != null;
        String newDirectoryName = "testDir";
        String path = resource.getPath();
        String parentPath = new File(path).getParent();
        String newDirPath = String.join(File.separator, parentPath, newDirectoryName);
        FileSystemUtils.createDir(newDirPath);
        Assertions.assertTrue(FileSystemUtils.fileExist(newDirPath));
        FileSystemUtils.deleteFile(newDirPath);
    }

    @Test
    void testDirList() throws IOException {
        assert resource != null;
        String path = resource.getPath();
        String parentPath = new File(path).getParent();
        List<Path> paths = FileSystemUtils.dirList(parentPath);
        Assertions.assertNotNull(paths);
        Assertions.assertNotEquals(paths.size(), 0);
    }
}
