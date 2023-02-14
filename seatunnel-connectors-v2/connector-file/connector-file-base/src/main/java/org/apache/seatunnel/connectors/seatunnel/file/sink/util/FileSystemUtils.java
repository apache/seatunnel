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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FileSystemUtils implements Serializable {
    private static final int WRITE_BUFFER_SIZE = 2048;

    private final HadoopConf hadoopConf;

    private transient Configuration configuration;

    public FileSystemUtils(HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
        configuration.set(
                String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getFsHdfsImpl());
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    public FileSystem getFileSystem(@NonNull String path) throws IOException {
        if (configuration == null) {
            configuration = getConfiguration(hadoopConf);
        }
        FileSystem fileSystem =
                FileSystem.get(URI.create(path.replaceAll("\\\\", "/")), configuration);
        fileSystem.setWriteChecksum(false);
        return fileSystem;
    }

    public FSDataOutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FileSystem fileSystem = getFileSystem(outFilePath);
        Path path = new Path(outFilePath);
        return fileSystem.create(path, true, WRITE_BUFFER_SIZE);
    }

    public void createFile(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path path = new Path(filePath);
        if (!fileSystem.createNewFile(path)) {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED, "create file " + filePath + " error");
        }
    }

    public void deleteFile(@NonNull String file) throws IOException {
        FileSystem fileSystem = getFileSystem(file);
        Path path = new Path(file);
        if (fileSystem.exists(path)) {
            if (!fileSystem.delete(path, true)) {
                throw new FileConnectorException(
                        CommonErrorCode.FILE_OPERATION_FAILED, "delete file " + file + " error");
            }
        }
    }

    /**
     * rename file
     *
     * @param oldName old file name
     * @param newName target file name
     * @param rmWhenExist if this is true, we will delete the target file when it already exists
     * @throws IOException throw IOException
     */
    public void renameFile(@NonNull String oldName, @NonNull String newName, boolean rmWhenExist)
            throws IOException {
        FileSystem fileSystem = getFileSystem(newName);
        log.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);

        if (!fileExist(oldPath.toString())) {
            log.warn(
                    "rename file :["
                            + oldPath
                            + "] to ["
                            + newPath
                            + "] already finished in the last commit, skip");
            return;
        }

        if (rmWhenExist) {
            if (fileExist(newName) && fileExist(oldName)) {
                fileSystem.delete(newPath, true);
                log.info("Delete already file: {}", newPath);
            }
        }
        if (!fileExist(newPath.getParent().toString())) {
            createDir(newPath.getParent().toString());
        }

        if (fileSystem.rename(oldPath, newPath)) {
            log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    "rename file :[" + oldPath + "] to [" + newPath + "] error");
        }
    }

    public void createDir(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path dfs = new Path(filePath);
        if (!fileSystem.mkdirs(dfs)) {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED, "create dir " + filePath + " error");
        }
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path fileName = new Path(filePath);
        return fileSystem.exists(fileName);
    }

    /** get the dir in filePath */
    public List<Path> dirList(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        List<Path> pathList = new ArrayList<>();
        if (!fileExist(filePath)) {
            return pathList;
        }
        Path fileName = new Path(filePath);
        FileStatus[] status = fileSystem.listStatus(fileName);
        if (status != null) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    pathList.add(fileStatus.getPath());
                }
            }
        }
        return pathList;
    }
}
