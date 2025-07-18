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
import java.net.URL;
import java.nio.file.Paths;

/** X2SeaTunnel 智能路径解析器 */
public class PathResolver {

    private static final Logger logger = LoggerFactory.getLogger(PathResolver.class);

    private static final String X2SEATUNNEL_HOME_PROPERTY = "X2SEATUNNEL_HOME";
    private static final String CONFIG_TEMPLATES_DIR = "templates";
    private static final String RESOURCE_TEMPLATES_PREFIX = "/templates";

    private static String cachedHomePath = null;

    /**
     * 获取 X2SeaTunnel 的主目录
     *
     * @return X2SeaTunnel 主目录路径
     */
    public static String getHomePath() {
        if (cachedHomePath != null) {
            return cachedHomePath;
        }

        // 1. 优先使用系统属性（脚本设置）
        String homePath = System.getProperty(X2SEATUNNEL_HOME_PROPERTY);
        if (homePath != null && !homePath.trim().isEmpty()) {
            cachedHomePath = new File(homePath).getAbsolutePath();
            logger.info("使用系统属性 X2SEATUNNEL_HOME: {}", cachedHomePath);
            return cachedHomePath;
        }

        // 2. 自动检测JAR包位置推导
        homePath = autoDetectHomePath();
        if (homePath != null) {
            cachedHomePath = homePath;
            logger.info("自动检测到 X2SEATUNNEL_HOME: {}", cachedHomePath);
            return cachedHomePath;
        }

        // 3. 回退到当前工作目录
        cachedHomePath = System.getProperty("user.dir");
        logger.warn("无法检测 X2SEATUNNEL_HOME，使用当前工作目录: {}", cachedHomePath);
        return cachedHomePath;
    }

    /** 自动检测主目录路径（基于JAR包位置） */
    private static String autoDetectHomePath() {
        try {
            // 获取当前类所在的JAR包位置
            URL classUrl = PathResolver.class.getProtectionDomain().getCodeSource().getLocation();
            if (classUrl != null) {
                File jarFile = new File(classUrl.toURI()); // 如果是JAR包，获取其父目录的父目录作为主目录
                if (jarFile.isFile() && jarFile.getName().endsWith(".jar")) {
                    File parentDir = jarFile.getParentFile(); // lib/ 或 bin/
                    if (parentDir != null) {
                        if ("lib".equals(parentDir.getName())
                                || "bin".equals(parentDir.getName())) {
                            return parentDir.getParentFile().getAbsolutePath(); // x2seatunnel/
                        }
                    }
                }

                // 如果是开发环境（target/classes），查找 x2seatunnel 模块根目录
                if (jarFile.getPath().contains("target" + File.separator + "classes")) {
                    File current = jarFile;
                    while (current != null) {
                        // 查找 x2seatunnel 模块根目录
                        if (isX2SeaTunnelModuleRoot(current)) {
                            return current.getAbsolutePath();
                        }
                        current = current.getParentFile();
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("自动检测主目录失败: {}", e.getMessage());
        }

        return null;
    }

    /** 判断是否是 X2SeaTunnel 模块根目录 */
    private static boolean isX2SeaTunnelModuleRoot(File dir) {
        if (dir == null || !dir.isDirectory()) {
            return false;
        }

        // 检查是否存在 X2SeaTunnel 模块的特征文件/目录
        return new File(dir, "pom.xml").exists()
                && new File(dir, "src").exists()
                && (new File(dir, "config").exists()
                        || new File(dir, "examples").exists()
                        || dir.getName().equals("x2seatunnel"));
    }

    /** 判断是否是 SeaTunnel 项目根目录（保留用于兼容性） */
    private static boolean isSeaTunnelProjectRoot(File dir) {
        if (dir == null || !dir.isDirectory()) {
            return false;
        }

        // 检查是否存在 SeaTunnel 项目的特征文件/目录
        return new File(dir, "pom.xml").exists()
                && (new File(dir, "seatunnel-tools").exists()
                        || new File(dir, "bin").exists()
                        || dir.getName().toLowerCase().contains("seatunnel"));
    }

    /**
     * 解析模板文件路径
     *
     * @param templatePath 模板文件路径（可以是绝对路径或相对路径）
     * @return 解析后的完整路径
     */
    public static String resolveTemplatePath(String templatePath) {
        if (templatePath == null || templatePath.trim().isEmpty()) {
            throw new IllegalArgumentException("模板路径不能为空");
        }

        templatePath = templatePath.trim();

        // 1. 如果是绝对路径，直接返回
        if (Paths.get(templatePath).isAbsolute()) {
            return templatePath;
        }

        // 2. 相对于当前工作目录查找
        File currentDirFile = new File(templatePath);
        if (currentDirFile.exists()) {
            String absolutePath = currentDirFile.getAbsolutePath();
            logger.info("从当前目录找到模板: {}", absolutePath);
            return absolutePath;
        }

        // 3. 相对于 X2SEATUNNEL_HOME/templates 查找
        String homePath = getHomePath();
        String homeTemplatePath =
                Paths.get(homePath, CONFIG_TEMPLATES_DIR, templatePath).toString();
        File homeTemplateFile = new File(homeTemplatePath);
        if (homeTemplateFile.exists()) {
            logger.info("从主目录配置找到模板: {}", homeTemplatePath);
            return homeTemplatePath;
        }

        // 4. 尝试开发环境路径（seatunnel/config/x2seatunnel/templates）
        String devTemplatePath =
                Paths.get(homePath, "config/x2seatunnel/templates", templatePath).toString();
        File devTemplateFile = new File(devTemplatePath);
        if (devTemplateFile.exists()) {
            logger.info("从开发环境配置找到模板: {}", devTemplatePath);
            return devTemplatePath;
        }

        // 5. 如果都找不到，返回null，让调用方处理classpath查找
        logger.debug("在文件系统中未找到模板文件: {}", templatePath);
        return null;
    }

    /**
     * 构建资源路径（用于classpath查找）
     *
     * @param templatePath 模板路径
     * @return classpath资源路径
     */
    public static String buildResourcePath(String templatePath) {
        // 确保以/开头
        if (!templatePath.startsWith("/")) {
            templatePath = "/" + templatePath;
        }

        // 如果已经包含完整路径，直接返回
        if (templatePath.startsWith(RESOURCE_TEMPLATES_PREFIX)) {
            return templatePath;
        }

        // 否则拼接标准前缀
        return RESOURCE_TEMPLATES_PREFIX + templatePath;
    }

    /**
     * 获取配置模板目录路径
     *
     * @return 配置模板目录的绝对路径
     */
    public static String getConfigTemplatesDir() {
        return Paths.get(getHomePath(), CONFIG_TEMPLATES_DIR).toString();
    }

    /**
     * 检查路径是否存在
     *
     * @param path 要检查的路径
     * @return 如果路径存在返回true，否则返回false
     */
    public static boolean exists(String path) {
        return path != null && new File(path).exists();
    }
}
