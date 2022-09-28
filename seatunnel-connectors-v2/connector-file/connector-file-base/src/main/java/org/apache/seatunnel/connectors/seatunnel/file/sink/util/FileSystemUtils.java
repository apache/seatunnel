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

package org.apache.seatunnel.connectors.seatunnel.file.sink.util;

import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class FileSystemUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemUtils.class);

    public static final int WRITE_BUFFER_SIZE = 2048;

    public static Configuration CONF;

    public static FileSystem getFileSystem(@NonNull String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(path), CONF);
        fileSystem.setWriteChecksum(false);
        return fileSystem;
    }

    public static FSDataOutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FileSystem fileSystem = getFileSystem(outFilePath);
        Path path = new Path(outFilePath);
        return fileSystem.create(path, true, WRITE_BUFFER_SIZE);
    }

    public static void createFile(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path path = new Path(filePath);
        if (!fileSystem.createNewFile(path)) {
            throw new IOException("create file " + filePath + " error");
        }
    }

    public static void deleteFile(@NonNull String file) throws IOException {
        FileSystem fileSystem = getFileSystem(file);
        Path path = new Path(file);
        if (fileSystem.exists(path)) {
            if (!fileSystem.delete(path, true)) {
                throw new IOException("delete file " + file + " error");
            }
        }
    }

    /**
     * rename file
     *
     * @param oldName     old file name
     * @param newName     target file name
     * @param rmWhenExist if this is true, we will delete the target file when it already exists
     * @throws IOException throw IOException
     */
    public static void renameFile(@NonNull String oldName, @NonNull String newName, boolean rmWhenExist) throws IOException {
        FileSystem fileSystem = getFileSystem(newName);
        LOGGER.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        if (rmWhenExist) {
            if (fileExist(newName) && fileExist(oldName)) {
                fileSystem.delete(newPath, true);
                LOGGER.info("Delete already file: {}", newPath);
            }
        }
        if (!fileExist(newName.substring(0, newName.lastIndexOf("/")))) {
            createDir(newName.substring(0, newName.lastIndexOf("/")));
        }

        if (fileSystem.rename(oldPath, newPath)) {
            LOGGER.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw new IOException("rename file :[" + oldPath + "] to [" + newPath + "] error");
        }
    }

    public static void createDir(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path dfs = new Path(filePath);
        if (!fileSystem.mkdirs(dfs)) {
            throw new IOException("create dir " + filePath + " error");
        }
    }

    public static boolean fileExist(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path fileName = new Path(filePath);
        return fileSystem.exists(fileName);
    }

    /**
     * get the dir in filePath
     */
    public static List<Path> dirList(@NonNull String filePath) throws FileNotFoundException, IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        List<Path> pathList = new ArrayList<Path>();
        Path fileName = new Path(filePath);
        FileStatus[] status = fileSystem.listStatus(fileName);
        if (status != null && status.length > 0) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    pathList.add(fileStatus.getPath());
                }
            }
        }
        return pathList;
    }
}
