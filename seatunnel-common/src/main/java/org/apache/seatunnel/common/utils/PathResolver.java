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

import org.apache.seatunnel.common.config.Common;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PathResolver {

    private static final String SEATUNNEL_HOME_VAR = "$SEATUNNEL_HOME";

    /**
     * Replaces the absolute path of SEATUNNEL_HOME in the given URLs with a logical variable. The
     * modification happens in-place on the provided collection.
     *
     * @param urls The collection of absolute URLs
     */
    public static void replacePathWithEnv(Collection<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return;
        }
        List<URL> replaced =
                urls.stream().map(PathResolver::replacePathWithEnv).collect(Collectors.toList());
        urls.clear();
        urls.addAll(replaced);
    }

    /**
     * Replaces the absolute path of SEATUNNEL_HOME in the given URL with a logical variable.
     *
     * @param url The absolute URL
     * @return A URL with the logical variable, or the original URL if it's not within
     *     SEATUNNEL_HOME
     */
    public static URL replacePathWithEnv(URL url) {
        String path = url.getPath();
        String home = Common.getSeaTunnelHome();
        // Normalize paths for comparison (handle Windows backslashes)
        String normalizedPath = new File(path).getAbsolutePath();
        String normalizedHome = new File(home).getAbsolutePath();

        if (normalizedPath.startsWith(normalizedHome)) {
            String relativePath = normalizedPath.substring(normalizedHome.length());
            // Ensure leading slash for URL construction
            String newPath = SEATUNNEL_HOME_VAR + relativePath.replace(File.separatorChar, '/');
            try {
                return new URL(url.getProtocol(), url.getHost(), url.getPort(), newPath);
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to create logical URL for: " + url, e);
            }
        }
        return url;
    }

    /**
     * Resolves a collection of URLs containing the logical SEATUNNEL_HOME variable to absolute
     * paths. The modification happens in-place on the provided collection.
     *
     * @param urls The collection of logical URLs to resolve
     */
    public static void resolvePathEnv(Collection<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return;
        }
        List<URL> resolved =
                urls.stream().map(PathResolver::resolvePathEnv).collect(Collectors.toList());
        urls.clear();
        urls.addAll(resolved);
    }

    /**
     * Resolves a URL containing the logical SEATUNNEL_HOME variable to an absolute path.
     *
     * @param url The logical URL
     * @return The resolved absolute URL
     */
    public static URL resolvePathEnv(URL url) {
        String path = url.getPath();

        if (path.contains(SEATUNNEL_HOME_VAR)) {
            String home = Common.getSeaTunnelHome();
            // Replace the variable with the actual home path
            // We need to handle the case where path might start with / or not
            String cleanPath = path;
            if (path.startsWith("/")) {
                cleanPath = path.substring(1);
            }

            String relativePath = cleanPath.replace(SEATUNNEL_HOME_VAR, "");

            // Remove leading slashes from relative path
            if (relativePath.startsWith("/") || relativePath.startsWith("\\")) {
                relativePath = relativePath.substring(1);
            }

            Path fullPath = Paths.get(home, relativePath);
            try {
                return fullPath.toUri().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to resolve logical URL for: " + url, e);
            }
        }
        return url;
    }
}
