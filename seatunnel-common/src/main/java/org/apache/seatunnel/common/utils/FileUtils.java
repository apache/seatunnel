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

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static List<URL> searchJarFiles(@NonNull Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory, FileVisitOption.FOLLOW_LINKS)) {
            return paths.filter(path -> path.toString().endsWith(".jar"))
                    .map(
                            path -> {
                                try {
                                    return path.toUri().toURL();
                                } catch (MalformedURLException e) {
                                    throw new SeaTunnelRuntimeException(
                                            CommonErrorCodeDeprecated
                                                    .REFLECT_CLASS_OPERATION_FAILED,
                                            e);
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    public static String readFileToStr(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new String(bytes);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("SeaTunnel", "read", path.toString(), e);
        }
    }

    public static void writeStringToFile(String filePath, String str) {
        PrintStream ps = null;
        try {
            File file = new File(filePath);
            ps = new PrintStream(new FileOutputStream(file));
            ps.println(str);
        } catch (FileNotFoundException e) {
            throw CommonError.fileNotExistFailed("SeaTunnel", "write", filePath);
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
    public static void createNewFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        if (!file.getParentFile().exists()) {
            createParentFile(file);
        }
        file.createNewFile();
    }

    /**
     * return the line number of file
     *
     * @param filePath The file need be read
     * @return The file line number
     */
    public static Long getFileLineNumber(@NonNull String filePath) {
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            return lines.count();
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("SeaTunnel", "read", filePath, e);
        }
    }

    public static boolean isFileExist(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    /**
     * return the line number of all files in the dirPath
     *
     * @param dirPath dirPath
     * @return The file line number of dirPath
     */
    public static Long getFileLineNumberFromDir(@NonNull String dirPath) {
        File file = new File(dirPath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return 0L;
            }
            return Arrays.stream(files)
                    .map(
                            currFile -> {
                                if (currFile.isDirectory()) {
                                    return getFileLineNumberFromDir(currFile.getPath());
                                } else {
                                    return getFileLineNumber(currFile.getPath());
                                }
                            })
                    .mapToLong(Long::longValue)
                    .sum();
        }
        return getFileLineNumber(file.getPath());
    }

    /**
     * create a dir, if the dir exists, clear the files and sub dirs in the dir.
     *
     * @param dirPath dirPath
     */
    public static void createNewDir(@NonNull String dirPath) {
        deleteFile(dirPath);
        File file = new File(dirPath);
        file.mkdirs();
    }

    /**
     * clear dir and the sub dir
     *
     * @param filePath filePath
     */
    public static void deleteFile(@NonNull String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                deleteFiles(file);
            }
            file.delete();
        }
    }

    private static void deleteFiles(@NonNull File file) {
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
            throw CommonError.fileOperationFailed("SeaTunnel", "delete", file.toString(), e);
        }
    }

    public static List<File> listFile(String dirPath) {
        try {
            File file = new File(dirPath);
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files == null) {
                    return null;
                }
                return Arrays.stream(files)
                        .map(
                                currFile -> {
                                    if (currFile.isDirectory()) {
                                        return null;
                                    } else {
                                        return Arrays.asList(currFile);
                                    }
                                })
                        .filter(Objects::nonNull)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
            }
            return Arrays.asList(file);
        } catch (Exception e) {
            throw CommonError.fileOperationFailed("SeaTunnel", "list", dirPath, e);
        }
    }
}
