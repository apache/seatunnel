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

package org.apache.seatunnel.tools.x2seatunnel.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Utility class for file operations. */
public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Read the content of a file.
     *
     * @param filePath The path to the file.
     * @return The content of the file.
     */
    public static String readFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new RuntimeException("File path cannot be empty");
        }

        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("File does not exist: " + filePath);
        }

        if (!file.isFile()) {
            throw new RuntimeException("Invalid file: " + filePath);
        }

        try {
            logger.debug("Reading file: {}", filePath);
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            String content = new String(bytes, StandardCharsets.UTF_8);
            logger.debug("File read successfully, content length: {}", content.length());
            return content;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + filePath, e);
        }
    }

    /**
     * Write content to a file.
     *
     * @param filePath The path to the file.
     * @param content The content to write.
     */
    public static void writeFile(String filePath, String content) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new RuntimeException("File path cannot be empty");
        }

        if (content == null) {
            content = "";
        }

        try {
            File file = new File(filePath);
            // Create directory
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (!parentDir.mkdirs()) {
                    throw new RuntimeException(
                            "Failed to create directory: " + parentDir.getAbsolutePath());
                }
            }
            logger.debug("Writing file: {}", filePath);
            Files.write(Paths.get(filePath), content.getBytes(StandardCharsets.UTF_8));
            logger.debug("File written successfully, content length: {}", content.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write file: " + filePath, e);
        }
    }

    /**
     * Check if a file exists.
     *
     * @param filePath The path to the file.
     * @return True if the file exists, false otherwise.
     */
    public static boolean exists(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        return new File(filePath).exists();
    }

    /**
     * Create a directory.
     *
     * @param dirPath The path to the directory.
     */
    public static void createDirectory(String dirPath) {
        if (dirPath == null || dirPath.trim().isEmpty()) {
            throw new RuntimeException("Directory path cannot be empty");
        }
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
                logger.debug("Directory created successfully: {}", dirPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory: " + dirPath, e);
            }
        }
    }

    /**
     * Get the file extension.
     *
     * @param filePath The path to the file.
     * @return The file extension or an empty string if there is none.
     */
    public static String getFileExtension(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return "";
        }
        int lastDotIndex = filePath.lastIndexOf('.');
        if (lastDotIndex == -1 || lastDotIndex == filePath.length() - 1) {
            return "";
        }
        return filePath.substring(lastDotIndex + 1).toLowerCase();
    }

    /**
     * Get the file name without the extension.
     *
     * @param filePath The path to the file.
     * @return The file name without the extension.
     */
    public static String getFileNameWithoutExtension(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return "";
        }

        String fileName = Paths.get(filePath).getFileName().toString();
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return fileName;
        }

        return fileName.substring(0, lastDotIndex);
    }

    /**
     * Read a resource file from the classpath.
     *
     * @param resourcePath The path to the resource (relative to the classpath root).
     * @return The content of the resource file, or null if the file does not exist.
     */
    public static String readResourceFile(String resourcePath) {
        if (resourcePath == null || resourcePath.trim().isEmpty()) {
            throw new RuntimeException("Resource path cannot be empty");
        }

        try {
            logger.debug("Reading classpath resource: {}", resourcePath);

            // Get the resource input stream
            InputStream inputStream = FileUtils.class.getResourceAsStream(resourcePath);
            if (inputStream == null) {
                logger.debug("Classpath resource does not exist: {}", resourcePath);
                return null;
            }

            // Read the stream content using a BufferedReader (Java 8 compatible)
            try (java.io.BufferedReader reader =
                    new java.io.BufferedReader(
                            new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(line);
                }

                String content = sb.toString();
                logger.debug(
                        "Resource file read successfully, content length: {}", content.length());
                return content;
            }

        } catch (IOException e) {
            logger.warn("Failed to read classpath resource: {}", resourcePath, e);
            return null;
        }
    }
}
