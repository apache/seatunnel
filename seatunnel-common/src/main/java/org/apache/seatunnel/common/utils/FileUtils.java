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

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class FileUtils {

    public static String readFileToStr(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new String(bytes);
        } catch (IOException e) {
            log.error(ExceptionUtils.getMessage(e));
            throw new RuntimeException(e);
        }
    }

    public static void writeStringToFile(String filePath, String str) {
        PrintStream ps = null;
        try {
            File file = new File(filePath);
            ps = new PrintStream(new FileOutputStream(file));
            ps.println(str);
        } catch (FileNotFoundException e) {
            log.error(ExceptionUtils.getMessage(e));
            throw new RuntimeException(e);
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    public static void createParentFile(File file) {
        File parentFile = file.getParentFile();
        if (null != parentFile && !parentFile.exists()) {
            parentFile.mkdirs();
            createParentFile(parentFile);
        }
    }

    /**
     * create a new file, delete the old one if it is exists.
     *
     * @param filePath filePath
     */
    public static void createNewFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        if (!file.getParentFile().exists()) {
            createParentFile(file);
        }
    }
}
