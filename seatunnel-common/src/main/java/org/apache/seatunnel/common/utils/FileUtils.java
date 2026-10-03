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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static List<URL> searchJarFiles(@NonNull Path directory) throws IOException {
        return searchJarFiles(directory, Integer.MAX_VALUE);
    }

    private static List<URL> searchJarFiles(@NonNull Path directory, int maxDepth)
            throws IOException {
        if (!directory.toFile().exists()) {
            return new ArrayList<>();
        }
        try (Stream<Path> paths = Files.walk(directory, maxDepth, FileVisitOption.FOLLOW_LINKS)) {
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

    /**
     * Valid split-layout storage directory names. storage.type is user-supplied configuration;
     * restricting it to a bare lowercase identifier is what keeps {@link #searchJarFilesForStorage}
     * from resolving outside the starter/zeta root.
     */
    private static final Pattern STORAGE_TYPE_PATTERN = Pattern.compile("[a-z0-9_-]+");

    /**
     * Returns true only when {@code directory} exists and its real path (symlinks resolved) is
     * still inside {@code rootRealPath}. A subdirectory that is a symlink pointing outside the
     * starter/zeta root must not be scanned: the JAR walk follows links, so an escaping link would
     * load engine plugins from unrelated filesystem locations.
     */
    private static boolean isContainedDirectory(Path directory, Path rootRealPath)
            throws IOException {
        if (!Files.isDirectory(directory)) {
            return false;
        }
        Path realPath = directory.toRealPath();
        if (!realPath.startsWith(rootRealPath)) {
            log.warn(
                    "Ignoring storage jar directory {} because it resolves to {} outside {}",
                    directory,
                    realPath,
                    rootRealPath);
            return false;
        }
        return true;
    }

    /**
     * Search Zeta storage JAR files from a split starter layout.
     *
     * <p>When {@code storageType} is configured, SeaTunnel loads jars from {@code common/} and the
     * matching storage-specific subdirectory (for example, {@code s3/} or {@code oss/}). If the
     * split layout is not available, this method falls back to scanning the whole Zeta directory to
     * preserve compatibility with the legacy layout.
     *
     * @param zetaDirectory The starter/zeta directory
     * @param storageType The storage type configured in plugin-config, such as {@code hdfs}, {@code
     *     s3}, or {@code oss}
     * @return List of URLs pointing to JAR files
     * @throws IOException if there is an error reading JAR files
     */
    public static List<URL> searchJarFilesForStorage(
            @NonNull Path zetaDirectory, String storageType) throws IOException {
        if (!Files.isDirectory(zetaDirectory)) {
            log.debug("Zeta directory does not exist: {}", zetaDirectory);
            return new ArrayList<>();
        }

        if (storageType == null || storageType.trim().isEmpty()) {
            return searchJarFiles(zetaDirectory);
        }

        String normalizedStorageType = storageType.trim().toLowerCase(Locale.ROOT);
        // storage.type comes from user configuration, so it must stay a plain directory name.
        // Anything path-shaped (separators, "..", drive/scheme prefixes, absolute paths) could
        // make resolve() escape the starter/zeta root and load arbitrary JARs into the engine.
        // Fail closed to the legacy whole-directory scan, which never leaves zetaDirectory.
        if (!STORAGE_TYPE_PATTERN.matcher(normalizedStorageType).matches()) {
            log.warn(
                    "Ignoring illegal storage type '{}' for split storage-jar loading: only "
                            + "[a-z0-9_-] names are allowed. Falling back to scanning {}",
                    storageType,
                    zetaDirectory);
            return searchJarFiles(zetaDirectory);
        }

        List<URL> jars = new ArrayList<>();
        boolean splitLayoutDetected = false;
        Path zetaRealPath = zetaDirectory.toRealPath();

        Path commonDir = zetaDirectory.resolve("common");
        if (isContainedDirectory(commonDir, zetaRealPath)) {
            splitLayoutDetected = true;
            jars.addAll(searchJarFiles(commonDir));
        }

        Path storageDir = zetaDirectory.resolve(normalizedStorageType);
        if (isContainedDirectory(storageDir, zetaRealPath)) {
            splitLayoutDetected = true;
            jars.addAll(searchJarFiles(storageDir));
        }

        if (splitLayoutDetected) {
            // Keep root-level jars visible for installations transitioning from the legacy layout.
            jars.addAll(searchJarFiles(zetaDirectory, 1));
            log.info(
                    "Loaded {} storage JAR(s) for storage type '{}' from {}",
                    jars.size(),
                    normalizedStorageType,
                    zetaDirectory);
            return jars;
        }

        log.info(
                "No split Zeta storage directory found for storage type '{}', scan all JAR(s) under {} instead",
                normalizedStorageType,
                zetaDirectory);
        return searchJarFiles(zetaDirectory);
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
