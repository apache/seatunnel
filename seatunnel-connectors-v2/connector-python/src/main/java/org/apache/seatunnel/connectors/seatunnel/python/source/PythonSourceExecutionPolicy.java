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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/** Operator-controlled policy for unsandboxed Python source process execution. */
final class PythonSourceExecutionPolicy {

    static final String PYTHON_SOURCE_ENABLED_PROPERTY = "seatunnel.source.python.enabled";
    static final String PYTHON_ALLOWED_EXECUTABLES_PROPERTY =
            "seatunnel.source.python.allowed-executables";

    private PythonSourceExecutionPolicy() {}

    /** Resolves the job-selected command and verifies it against the server-side allowlist. */
    static Path resolveExecutable(String configuredExecutable) throws IOException {
        ensureEnabled();
        List<Path> allowedExecutables = parseAllowedExecutables();
        Path resolvedExecutable = resolveConfiguredExecutable(configuredExecutable);
        for (Path allowedExecutable : allowedExecutables) {
            if (sameExecutablePath(resolvedExecutable, allowedExecutable)) {
                return resolvedExecutable;
            }
        }
        throw new IllegalStateException(
                "Python source executable "
                        + resolvedExecutable
                        + " is not listed in server property "
                        + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                        + "="
                        + allowedExecutables);
    }

    private static void ensureEnabled() {
        if (Boolean.parseBoolean(
                System.getProperty(PYTHON_SOURCE_ENABLED_PROPERTY, Boolean.FALSE.toString()))) {
            return;
        }
        throw new IllegalStateException(
                "Python source is disabled by the server-side security policy. Set -D"
                        + PYTHON_SOURCE_ENABLED_PROPERTY
                        + "=true and configure -D"
                        + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                        + " with absolute interpreter paths on every worker node.");
    }

    private static List<Path> parseAllowedExecutables() {
        String rawAllowlist = System.getProperty(PYTHON_ALLOWED_EXECUTABLES_PROPERTY, "");
        if (rawAllowlist.trim().isEmpty()) {
            throw new IllegalStateException(
                    "Server property "
                            + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                            + " must contain at least one absolute executable path");
        }
        Set<Path> allowedExecutables = new LinkedHashSet<>();
        for (String rawEntry : rawAllowlist.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            Path path = Paths.get(entry);
            if (!path.isAbsolute()) {
                throw new IllegalStateException(
                        "Python source allowlist entry must be an absolute path: " + entry);
            }
            allowedExecutables.add(normalize(path));
        }
        if (allowedExecutables.isEmpty()) {
            throw new IllegalStateException(
                    "Server property "
                            + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                            + " does not contain a usable absolute path");
        }
        return new ArrayList<>(allowedExecutables);
    }

    private static Path resolveConfiguredExecutable(String configuredExecutable)
            throws IOException {
        Path configuredPath = Paths.get(configuredExecutable);
        Path resolvedPath;
        if (configuredPath.isAbsolute()) {
            resolvedPath = normalize(configuredPath);
        } else if (configuredPath.getParent() == null) {
            resolvedPath = resolveCommandFromPath(configuredExecutable);
            if (resolvedPath == null) {
                throw new IOException(
                        "Unable to resolve python.executable from PATH: " + configuredExecutable);
            }
        } else {
            throw new IllegalStateException(
                    "python.executable must be an absolute path or a bare command name: "
                            + configuredExecutable);
        }
        if (!Files.isRegularFile(resolvedPath) || !Files.isExecutable(resolvedPath)) {
            throw new IOException(
                    "python.executable does not resolve to an executable file: " + resolvedPath);
        }
        return resolvedPath;
    }

    private static Path resolveCommandFromPath(String command) {
        String pathValue = System.getenv("PATH");
        if (pathValue == null || pathValue.trim().isEmpty()) {
            return null;
        }
        for (String directory : pathValue.split(Pattern.quote(File.pathSeparator))) {
            if (directory == null || directory.trim().isEmpty()) {
                continue;
            }
            for (String commandName : expandCommandNames(command)) {
                Path candidate = Paths.get(directory, commandName);
                if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
                    return normalize(candidate);
                }
            }
        }
        return null;
    }

    private static List<String> expandCommandNames(String command) {
        Set<String> commandNames = new LinkedHashSet<>();
        commandNames.add(command);
        if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("windows")) {
            return new ArrayList<>(commandNames);
        }
        String pathExt = System.getenv("PATHEXT");
        if (pathExt == null || pathExt.trim().isEmpty() || command.contains(".")) {
            return new ArrayList<>(commandNames);
        }
        for (String extension : pathExt.split(Pattern.quote(File.pathSeparator))) {
            if (!extension.trim().isEmpty()) {
                commandNames.add(command + extension.trim());
            }
        }
        return new ArrayList<>(commandNames);
    }

    private static boolean sameExecutablePath(Path left, Path right) {
        if (normalize(left).equals(normalize(right))) {
            return true;
        }
        try {
            return Files.exists(left) && Files.exists(right) && Files.isSameFile(left, right);
        } catch (IOException ignored) {
            return false;
        }
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }
}
