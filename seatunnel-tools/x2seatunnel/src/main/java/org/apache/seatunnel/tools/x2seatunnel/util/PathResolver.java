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

/** X2SeaTunnel Intelligent Path Resolver */
public class PathResolver {

    private static final Logger logger = LoggerFactory.getLogger(PathResolver.class);

    private static final String X2SEATUNNEL_HOME_PROPERTY = "X2SEATUNNEL_HOME";
    private static final String CONFIG_TEMPLATES_DIR = "templates";
    private static final String RESOURCE_TEMPLATES_PREFIX = "/templates";

    private static String cachedHomePath = null;

    public static String getHomePath() {
        if (cachedHomePath != null) {
            return cachedHomePath;
        }

        // 1. Priority: use system property (set by script)
        String homePath = System.getProperty(X2SEATUNNEL_HOME_PROPERTY);
        if (homePath != null && !homePath.trim().isEmpty()) {
            cachedHomePath = new File(homePath).getAbsolutePath();
            logger.info("Using system property X2SEATUNNEL_HOME: {}", cachedHomePath);
            return cachedHomePath;
        }

        // 2. Automatically detect the JAR location to infer the home directory
        homePath = autoDetectHomePath();
        if (homePath != null) {
            cachedHomePath = homePath;
            logger.info("Auto-detected X2SEATUNNEL_HOME: {}", cachedHomePath);
            return cachedHomePath;
        }

        // 3. Fallback to the current working directory
        cachedHomePath = System.getProperty("user.dir");
        logger.warn(
                "Unable to detect X2SEATUNNEL_HOME, using current working directory: {}",
                cachedHomePath);
        return cachedHomePath;
    }

    /** Automatically detect the home directory path (based on JAR location) */
    private static String autoDetectHomePath() {
        try {
            // Get the location of the JAR file where the current class is located
            URL classUrl = PathResolver.class.getProtectionDomain().getCodeSource().getLocation();
            if (classUrl != null) {
                File jarFile = new File(classUrl.toURI());
                if (jarFile.isFile() && jarFile.getName().endsWith(".jar")) {
                    File parentDir = jarFile.getParentFile(); // lib/ or bin/
                    if (parentDir != null) {
                        if ("lib".equals(parentDir.getName())
                                || "bin".equals(parentDir.getName())) {
                            return parentDir.getParentFile().getAbsolutePath(); // x2seatunnel/
                        }
                    }
                }

                // If it is a development environment (target/classes), find the root directory of
                // the x2seatunnel module
                if (jarFile.getPath().contains("target" + File.separator + "classes")) {
                    File current = jarFile;
                    while (current != null) {
                        // Find the root directory of the x2seatunnel module
                        if (isX2SeaTunnelModuleRoot(current)) {
                            return current.getAbsolutePath();
                        }
                        current = current.getParentFile();
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to auto-detect home directory: {}", e.getMessage());
        }

        return null;
    }

    private static boolean isX2SeaTunnelModuleRoot(File dir) {
        if (dir == null || !dir.isDirectory()) {
            return false;
        }

        return new File(dir, "pom.xml").exists()
                && new File(dir, "src").exists()
                && (new File(dir, "config").exists()
                        || new File(dir, "examples").exists()
                        || dir.getName().equals("x2seatunnel"));
    }

    /**
     * Resolve the template file path
     *
     * @param templatePath The template file path (can be an absolute or relative path)
     * @return The resolved full path
     */
    public static String resolveTemplatePath(String templatePath) {
        if (templatePath == null || templatePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Template path cannot be empty");
        }

        templatePath = templatePath.trim();

        // 1. If it is an absolute path, return it directly
        if (Paths.get(templatePath).isAbsolute()) {
            return templatePath;
        }

        // 2. Look for it relative to the current working directory
        File currentDirFile = new File(templatePath);
        if (currentDirFile.exists()) {
            String absolutePath = currentDirFile.getAbsolutePath();
            logger.info("Found template from current directory: {}", absolutePath);
            return absolutePath;
        }

        // 3. Look for it relative to X2SEATUNNEL_HOME/templates
        String homePath = getHomePath();
        String homeTemplatePath =
                Paths.get(homePath, CONFIG_TEMPLATES_DIR, templatePath).toString();
        File homeTemplateFile = new File(homeTemplatePath);
        if (homeTemplateFile.exists()) {
            logger.info("Found template from home directory configuration: {}", homeTemplatePath);
            return homeTemplatePath;
        }

        // 4. Try the development environment path (seatunnel/config/x2seatunnel/templates)
        String devTemplatePath =
                Paths.get(homePath, "config/x2seatunnel/templates", templatePath).toString();
        File devTemplateFile = new File(devTemplatePath);
        if (devTemplateFile.exists()) {
            logger.info(
                    "Found template from development environment configuration: {}",
                    devTemplatePath);
            return devTemplatePath;
        }

        // 5. If not found, return null, let the caller handle classpath lookup
        logger.warn("Template file not found in the file system: {}", templatePath);
        return null;
    }

    /**
     * Build the resource path (for classpath lookup)
     *
     * @param templatePath The template path
     * @return The classpath resource path
     */
    public static String buildResourcePath(String templatePath) {
        if (!templatePath.startsWith("/")) {
            templatePath = "/" + templatePath;
        }

        // If it already contains the full path, return it directly
        if (templatePath.startsWith(RESOURCE_TEMPLATES_PREFIX)) {
            return templatePath;
        }

        // Otherwise, concatenate the standard prefix
        return RESOURCE_TEMPLATES_PREFIX + templatePath;
    }

    public static String getConfigTemplatesDir() {
        return Paths.get(getHomePath(), CONFIG_TEMPLATES_DIR).toString();
    }

    public static boolean exists(String path) {
        return path != null && new File(path).exists();
    }
}
