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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static List<URL> searchJarFiles(@NonNull Path directory) throws IOException {
        if (!directory.toFile().exists()) {
            return new ArrayList<>();
        }
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

    /**
     * Reads a file, keeping at most {@code maxBytes} bytes from the end of it.
     *
     * <p>Reading a file whole materialises it twice on the heap, once as a byte array and once as a
     * string. For files that can grow without bound - engine log files being the case this was
     * written for - that turns a single read into a node-wide memory problem. When the file is
     * larger than the limit its tail is returned instead, the tail being the part that matters when
     * diagnosing a failure.
     *
     * <p>The tail starts at the first line break after the cut point, so it never begins with half
     * a line and never splits a multi-byte UTF-8 character. Content is decoded as UTF-8 rather than
     * with the platform default charset used by {@link #readFileToStr(Path)}, because aligning on
     * character boundaries is only meaningful against a known encoding.
     *
     * @param path file to read
     * @param maxBytes maximum number of bytes to keep from the end; a value <= 0 means unlimited
     * @return the whole file, or its tail when the file is larger than {@code maxBytes}
     */
    public static String readFileTailToStr(Path path, long maxBytes) {
        if (maxBytes <= 0) {
            return readFileToStr(path);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size <= maxBytes) {
                return readFileToStr(path);
            }
            // A String cannot hold more than Integer.MAX_VALUE chars anyway, so a limit above that
            // can never take effect and clamping keeps the cast below safe.
            int keep = (int) Math.min(maxBytes, Integer.MAX_VALUE - 8);
            ByteBuffer buffer = ByteBuffer.allocate(keep);
            channel.position(size - keep);
            while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                // Keep reading until the requested tail is filled or the file ends.
            }
            return new String(
                    tailFromLineStart(buffer.array(), buffer.position()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("SeaTunnel", "read", path.toString(), e);
        }
    }

    private static byte[] tailFromLineStart(byte[] bytes, int length) {
        for (int i = 0; i < length; i++) {
            if (bytes[i] == '\n') {
                return Arrays.copyOfRange(bytes, i + 1, length);
            }
        }
        // A single line longer than the limit leaves no boundary to align to, so drop just the
        // leading UTF-8 continuation bytes to avoid starting in the middle of a character.
        int start = 0;
        while (start < length && (bytes[start] & 0xC0) == 0x80) {
            start++;
        }
        return Arrays.copyOfRange(bytes, start, length);
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
