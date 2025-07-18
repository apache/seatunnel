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

/** 文件工具类 */
public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 读取文件内容
     *
     * @param filePath 文件路径
     * @return 文件内容
     */
    public static String readFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new RuntimeException("文件路径不能为空");
        }

        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("文件不存在: " + filePath);
        }

        if (!file.isFile()) {
            throw new RuntimeException("不是有效的文件: " + filePath);
        }

        try {
            logger.debug("正在读取文件: {}", filePath);
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            String content = new String(bytes, StandardCharsets.UTF_8);
            logger.debug("文件读取成功，内容长度: {}", content.length());
            return content;
        } catch (IOException e) {
            throw new RuntimeException("读取文件失败: " + filePath, e);
        }
    }

    /**
     * 写入文件内容
     *
     * @param filePath 文件路径
     * @param content 文件内容
     */
    public static void writeFile(String filePath, String content) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new RuntimeException("文件路径不能为空");
        }

        if (content == null) {
            content = "";
        }

        try {
            File file = new File(filePath);
            // 创建目录
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (!parentDir.mkdirs()) {
                    throw new RuntimeException("创建目录失败: " + parentDir.getAbsolutePath());
                }
            }

            logger.debug("正在写入文件: {}", filePath);
            Files.write(Paths.get(filePath), content.getBytes(StandardCharsets.UTF_8));
            logger.debug("文件写入成功，内容长度: {}", content.length());
        } catch (IOException e) {
            throw new RuntimeException("写入文件失败: " + filePath, e);
        }
    }

    /**
     * 检查文件是否存在
     *
     * @param filePath 文件路径
     * @return 是否存在
     */
    public static boolean exists(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        return new File(filePath).exists();
    }

    /**
     * 创建目录
     *
     * @param dirPath 目录路径
     */
    public static void createDirectory(String dirPath) {
        if (dirPath == null || dirPath.trim().isEmpty()) {
            throw new RuntimeException("目录路径不能为空");
        }

        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
                logger.debug("目录创建成功: {}", dirPath);
            } catch (IOException e) {
                throw new RuntimeException("创建目录失败: " + dirPath, e);
            }
        }
    }

    /**
     * 获取文件扩展名
     *
     * @param filePath 文件路径
     * @return 扩展名（不包含点号）
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
     * 获取文件名（不包含扩展名）
     *
     * @param filePath 文件路径
     * @return 文件名
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
     * 从classpath读取资源文件
     *
     * @param resourcePath 资源路径（从classpath根目录开始）
     * @return 文件内容，如果文件不存在返回null
     */
    public static String readResourceFile(String resourcePath) {
        if (resourcePath == null || resourcePath.trim().isEmpty()) {
            throw new RuntimeException("资源路径不能为空");
        }

        try {
            logger.debug("正在读取classpath资源: {}", resourcePath);

            // 获取资源输入流
            InputStream inputStream = FileUtils.class.getResourceAsStream(resourcePath);
            if (inputStream == null) {
                logger.debug("classpath资源不存在: {}", resourcePath);
                return null;
            }

            // 使用BufferedReader读取流内容（Java 8兼容）
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
                logger.debug("资源文件读取成功，内容长度: {}", content.length());
                return content;
            }

        } catch (IOException e) {
            logger.warn("读取classpath资源失败: {}", resourcePath, e);
            return null;
        }
    }
}
