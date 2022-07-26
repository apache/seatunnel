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

package org.apache.seatunnel.connectors.seatunnel.file.sink.hdfs;

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

public class HdfsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

    public static final int WRITE_BUFFER_SIZE = 2048;

    public static final Configuration CONF = new Configuration();

    // make the configuration object static, so orc and parquet reader can get it
    static {
        LOGGER.info(System.getenv("HADOOP_CONF_DIR"));
        CONF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));
        CONF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        CONF.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static FileSystem getHdfsFs(@NonNull String path)
            throws IOException {
        return FileSystem.get(URI.create(path), CONF);
    }

    public static FSDataOutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FileSystem hdfsFs = getHdfsFs(outFilePath);
        Path path = new Path(outFilePath);
        FSDataOutputStream fsDataOutputStream = hdfsFs.create(path, true, WRITE_BUFFER_SIZE);
        return fsDataOutputStream;
    }

    public static void createFile(@NonNull String filePath) throws IOException {
        FileSystem hdfsFs = getHdfsFs(filePath);
        Path path = new Path(filePath);
        if (!hdfsFs.createNewFile(path)) {
            throw new IOException("create file " + filePath + " error");
        }
    }

    public static void deleteFile(@NonNull String file) throws IOException {
        FileSystem hdfsFs = getHdfsFs(file);
        if (!hdfsFs.delete(new Path(file), true)) {
            throw new IOException("delete file " + file + " error");
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
        FileSystem hdfsFs = getHdfsFs(newName);
        LOGGER.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        if (rmWhenExist) {
            if (fileExist(newName) && fileExist(oldName)) {
                hdfsFs.delete(newPath, true);
            }
        }
        if (!fileExist(newName.substring(0, newName.lastIndexOf("/")))) {
            createDir(newName.substring(0, newName.lastIndexOf("/")));
        }

        if (hdfsFs.rename(oldPath, newPath)) {
            LOGGER.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw new IOException("rename file :[" + oldPath + "] to [" + newPath + "] error");
        }
    }

    public static void createDir(@NonNull String filePath)
            throws IOException {

        FileSystem hdfsFs = getHdfsFs(filePath);
        Path dfs = new Path(filePath);
        if (!hdfsFs.mkdirs(dfs)) {
            throw new IOException("create dir " + filePath + " error");
        }
    }

    public static boolean fileExist(@NonNull String filePath)
            throws IOException {
        FileSystem hdfsFs = getHdfsFs(filePath);
        Path fileName = new Path(filePath);
        return hdfsFs.exists(fileName);
    }

    /**
     * get the dir in filePath
     */
    public static List<Path> dirList(@NonNull String filePath)
            throws FileNotFoundException, IOException {
        FileSystem hdfsFs = getHdfsFs(filePath);
        List<Path> pathList = new ArrayList<Path>();
        Path fileName = new Path(filePath);
        FileStatus[] status = hdfsFs.listStatus(fileName);
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
