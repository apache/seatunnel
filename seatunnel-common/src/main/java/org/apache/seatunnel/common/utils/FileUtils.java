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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
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
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    /**
     * The largest tail {@link #readFileTailToStr(Path, long)} can return. A byte array cannot hold
     * more than {@link Integer#MAX_VALUE} entries and some JVMs reserve a few of those for the
     * array header, so a limit above this one cannot be honoured however much heap is available.
     */
    public static final long MAX_TAIL_BYTES = Integer.MAX_VALUE - 8;

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

    /**
     * Reads a file, keeping at most {@code maxBytes} bytes from the end of it.
     *
     * <p>Reading a file whole materialises it twice on the heap, once as a byte array and once as a
     * string. For files that can grow without bound - engine log files being the case this was
     * written for - that turns a single read into a node-wide memory problem. When the file is
     * larger than the limit its tail is returned instead, the tail being the part that matters when
     * diagnosing a failure.
     *
     * <p>The tail starts at the first line break after the cut point. If a single line exceeds the
     * limit, a partial tail is returned without splitting its first multi-byte UTF-8 character.
     * Content is decoded as UTF-8 rather than with the platform default charset used by {@link
     * #readFileToStr(Path)} - aligning on character boundaries is only meaningful against a known
     * encoding, and a file whose encoding changed as it grew past the limit would be worse than one
     * that is consistently wrong.
     *
     * <p>The limit that actually applies is {@link #effectiveTailLimit(long)} rather than {@code
     * maxBytes} itself, so a caller asking for more than a byte array can hold still gets a bounded
     * read instead of an {@link OutOfMemoryError}.
     *
     * @param path file to read
     * @param maxBytes maximum number of bytes to keep from the end; a value <= 0 means unlimited
     * @return the whole file, or its tail when the file is larger than the effective limit
     */
    public static String readFileTailToStr(Path path, long maxBytes) {
        return readFileTail(path, maxBytes).getContent();
    }

    /**
     * Reads UTF-8 content and truncation metadata from one open file and one size snapshot.
     * Positive limits also bound reads of files that grow or are replaced while being read.
     */
    public static FileTail readFileTail(Path path, long maxBytes) {
        try {
            if (maxBytes <= 0) {
                byte[] bytes = Files.readAllBytes(path);
                return new FileTail(bytes, 0, bytes.length, bytes.length, false);
            }
            long keep = effectiveTailLimit(maxBytes);
            try (SeekableByteChannel channel =
                    Files.newByteChannel(path, StandardOpenOption.READ)) {
                long size = channel.size();
                boolean truncated = size > keep;
                ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(size, keep));
                channel.position(truncated ? size - keep : 0);
                while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                    // Never read beyond the snapshot, even if the file grows during this read.
                }
                byte[] tail = buffer.array();
                int length = buffer.position();
                int start = truncated ? lineStartOffset(tail, length) : 0;
                return new FileTail(tail, start, length - start, size, truncated);
            }
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("SeaTunnel", "read", path.toString(), e);
        }
    }

    /**
     * Returns the number of bytes {@link #readFileTailToStr(Path, long)} keeps for the given
     * positive limit, which is {@code maxBytes} clamped to {@link #MAX_TAIL_BYTES}.
     *
     * <p>Comparing the file size against this instead of against {@code maxBytes} is what keeps the
     * read bounded. A limit above {@link #MAX_TAIL_BYTES} cannot be honoured, so a file sized
     * between the two has to be read as a tail rather than whole - reading it whole would fail with
     * {@code OutOfMemoryError: Required array size too large}, which is the very failure the tail
     * read exists to prevent.
     */
    public static long effectiveTailLimit(long maxBytes) {
        return Math.min(maxBytes, MAX_TAIL_BYTES);
    }

    /** An immutable read result whose metadata stays valid after the file is rotated or removed. */
    public static final class FileTail {
        private final byte[] bytes;
        private final int offset;
        private final int length;
        private final long fileSize;
        private final boolean truncated;

        private FileTail(byte[] bytes, int offset, int length, long fileSize, boolean truncated) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
            this.fileSize = fileSize;
            this.truncated = truncated;
        }

        /** Returns the size observed at the start of the read, before any later rotation. */
        public long getFileSize() {
            return fileSize;
        }

        /** Returns retained file bytes after line/character alignment, excluding any prefix. */
        public int getReturnedBytes() {
            return length;
        }

        /** Reports whether the file-size snapshot exceeded the effective positive read limit. */
        public boolean isTruncated() {
            return truncated;
        }

        /** Decodes only the retained window, with no intermediate byte-array copy. */
        public String getContent() {
            return new String(bytes, offset, length, StandardCharsets.UTF_8);
        }

        /**
         * Decodes into the final response builder after its prefix, avoiding a full intermediate
         * content String when a caller adds a truncation notice.
         */
        public String getContentWithPrefix(String prefix) throws IOException {
            StringBuilder response =
                    new StringBuilder(
                            (int) Math.min(MAX_TAIL_BYTES, (long) prefix.length() + length));
            response.append(prefix);
            try (Reader reader =
                    new InputStreamReader(
                            new ByteArrayInputStream(bytes, offset, length),
                            StandardCharsets.UTF_8)) {
                char[] buffer = new char[8192];
                int count;
                while ((count = reader.read(buffer)) != -1) {
                    response.append(buffer, 0, count);
                }
            }
            return response.toString();
        }
    }

    /**
     * Returns the offset of the first byte to keep in a retained tail window, which is the start of
     * the first complete line in it.
     */
    private static int lineStartOffset(byte[] bytes, int length) {
        // A '\n' on the final byte is the terminator of the line before it, not the start of
        // another line, so it is not a boundary to align to. Stopping short of it is what keeps a
        // window holding a single newline-terminated line - the shape produced by a log whose last
        // entry is a large stack trace - from being reported as empty.
        for (int i = 0; i < length - 1; i++) {
            if (bytes[i] == '\n') {
                return i + 1;
            }
        }
        // A single line longer than the limit leaves no boundary to align to, so drop just the
        // leading UTF-8 continuation bytes to avoid starting in the middle of a character.
        int start = 0;
        while (start < length && (bytes[start] & 0xC0) == 0x80) {
            start++;
        }
        return start;
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
