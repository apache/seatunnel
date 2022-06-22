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

package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.seatunnel.shade.org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.shade.org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.seatunnel.shade.org.apache.hadoop.fs.FileSystem;
import org.apache.seatunnel.shade.org.apache.hadoop.fs.Path;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

    public static final int WRITE_BUFFER_SIZE = 2048;

    public static FileSystem getHdfsFs(@NonNull String path)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.seatunnel.shade.org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFs", path);
        return FileSystem.get(conf);
    }

    public static FSDataOutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FileSystem hdfsFs = getHdfsFs(outFilePath);
        Path path = new Path(outFilePath);
        FSDataOutputStream fsDataOutputStream = hdfsFs.create(path, true, WRITE_BUFFER_SIZE);
        return fsDataOutputStream;
    }

    public static boolean deleteFile(@NonNull String file) throws IOException {
        FileSystem hdfsFs = getHdfsFs(file);
        return hdfsFs.delete(new Path(file), true);
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
        LOGGER.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");

        hdfsFs.rename(oldPath, newPath);
    }

    public static boolean createDir(@NonNull String filePath)
        throws IOException {

        FileSystem hdfsFs = getHdfsFs(filePath);
        Path dfs = new Path(filePath);
        return hdfsFs.mkdirs(dfs);
    }

    public static boolean fileExist(@NonNull String filePath)
        throws IOException {
        FileSystem hdfsFs = getHdfsFs(filePath);
        Path fileName = new Path(filePath);
        return hdfsFs.exists(fileName);
    }
}
