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

package org.apache.seatunnel.edge.agent.connector.file.glob;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class GlobPathResolver {

    private final List<String> patterns;
    private final Set<Path> knownPaths;

    public GlobPathResolver(List<String> patterns) {
        this.patterns = Objects.requireNonNull(patterns, "patterns");
        this.knownPaths = new LinkedHashSet<>();
    }

    /**
     * Resolve all glob patterns and return ALL matching files.
     *
     * <p>Results are sorted by last modified time ascending (oldest first, so older files are read
     * before newer ones). Only regular files are included (directories are excluded).
     */
    public List<Path> resolveAll() throws IOException {
        Set<Path> unique = new LinkedHashSet<>();
        for (String rawPattern : patterns) {
            if (rawPattern == null || rawPattern.isEmpty()) {
                continue;
            }
            collectForPattern(rawPattern.trim(), unique);
        }
        List<Path> sorted = new ArrayList<>(unique);
        sorted.sort(
                Comparator.comparingLong(
                        p -> {
                            try {
                                return Files.getLastModifiedTime(p).toMillis();
                            } catch (IOException e) {
                                return Long.MAX_VALUE;
                            }
                        }));
        return sorted;
    }

    /**
     * Resolve glob patterns and return only NEW files (not previously seen).
     *
     * <p>This is intended to be called periodically during directory monitoring.
     */
    public List<Path> resolveNew() throws IOException {
        List<Path> all = resolveAll();
        List<Path> newPaths = new ArrayList<>();
        for (Path p : all) {
            if (!knownPaths.contains(p)) {
                newPaths.add(p);
            }
        }
        knownPaths.addAll(newPaths);
        return newPaths;
    }

    /** Remove a path from known set (e.g., when file is finished and cursor closed). */
    public void forget(Path path) {
        knownPaths.remove(path);
    }

    public Set<Path> getKnownPaths() {
        return Collections.unmodifiableSet(knownPaths);
    }

    private void collectForPattern(String pattern, Set<Path> out) throws IOException {
        int globStart = indexOfFirstGlobChar(pattern);
        if (globStart < 0) {
            Path candidate = Paths.get(pattern);
            if (Files.isRegularFile(candidate)) {
                out.add(candidate.normalize().toAbsolutePath());
            } else if (Files.isDirectory(candidate)) {
                walkAndMatch(
                        candidate.toAbsolutePath().normalize(), normalizeGlobSuffix("**/*"), out);
            }
            return;
        }

        String basePrefix = pattern.substring(0, globStart);
        while (basePrefix.length() > 1
                && (basePrefix.endsWith("/") || basePrefix.endsWith(File.separator))) {
            basePrefix = basePrefix.substring(0, basePrefix.length() - 1);
        }
        Path basePath =
                (basePrefix.isEmpty() ? Paths.get(".") : Paths.get(basePrefix))
                        .toAbsolutePath()
                        .normalize();
        String globSuffix = normalizeGlobSuffix(pattern.substring(globStart));
        walkAndMatch(basePath, globSuffix, out);
    }

    private static void walkAndMatch(Path basePath, String globSuffix, Set<Path> out)
            throws IOException {
        if (!Files.exists(basePath) || !Files.isDirectory(basePath)) {
            return;
        }
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globSuffix);
        Files.walkFileTree(
                basePath,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        if (!Files.isRegularFile(file)) {
                            return FileVisitResult.CONTINUE;
                        }
                        Path absoluteFile = file.normalize().toAbsolutePath();
                        Path relative = basePath.relativize(absoluteFile);
                        Path matchPath = Paths.get(relative.toString().replace('\\', '/'));
                        if (matcher.matches(matchPath)) {
                            out.add(absoluteFile);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    private static int indexOfFirstGlobChar(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                return i;
            }
        }
        return -1;
    }

    /** Glob patterns use forward slashes per {@code FileSystem.getPathMatcher}. */
    private static String normalizeGlobSuffix(String suffix) {
        if (suffix == null || suffix.isEmpty()) {
            return "**/*";
        }
        String trimmed = suffix;
        while (trimmed.startsWith("/") || trimmed.startsWith("\\")) {
            trimmed = trimmed.substring(1);
        }
        return trimmed.replace('\\', '/');
    }
}
