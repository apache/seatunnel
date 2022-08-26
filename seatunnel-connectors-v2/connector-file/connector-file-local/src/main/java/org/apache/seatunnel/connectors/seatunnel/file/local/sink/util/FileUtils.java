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

package org.apache.seatunnel.connectors.seatunnel.file.local.sink.util;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
    public static File createDir(@NonNull String dirPath) {
        if (dirPath == null || "".equals(dirPath)) {
            return null;
        }
        File file = new File(dirPath);
        if (!file.exists() || !file.isDirectory()) {
            file.mkdirs();
        }
        return file;
    }

    public static File createFile(@NonNull String filePath) throws IOException {
        if (filePath == null || "".equals(filePath)) {
            return null;
        }
        File file = new File(filePath);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        if (!file.exists() || !file.isFile()) {
            file.createNewFile();
        }
        return file;
    }

    public static boolean fileExist(@NonNull String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static void renameFile(@NonNull String oldName, @NonNull String newName) throws IOException {
        LOGGER.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");
        File oldPath = new File(oldName);
        File newPath = new File(newName);

        if (!newPath.getParentFile().exists()) {
            newPath.getParentFile().mkdirs();
        }

        if (oldPath.renameTo(newPath)) {
            LOGGER.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw new IOException("rename file :[" + oldPath + "] to [" + newPath + "] error");
        }
    }

    public static void deleteFile(@NonNull String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                deleteFiles(file);
            }
            file.delete();
        }
    }

    private static boolean deleteFiles(@NonNull File file) {
        try {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                File thisFile = files[i];
                if (thisFile.isDirectory()) {
                    deleteFiles(thisFile);
                }
                thisFile.delete();
            }
            file.delete();

        } catch (Exception e) {
            LOGGER.error("delete file [" + file.getPath() + "] error");
            return false;
        }
        return true;
    }
}
