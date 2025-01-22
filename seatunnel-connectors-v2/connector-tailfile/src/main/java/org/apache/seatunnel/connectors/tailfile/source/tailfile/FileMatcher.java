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

package org.apache.seatunnel.connectors.tailfile.source.tailfile;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.connectors.tailfile.source.Utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class FileMatcher {

    private final boolean cachePatternMatching;
    private final File parentDir;
    private final int scanDepth;
    private final Pattern fileNamePattern;
    private final FileFilter fileFilter;

    private long lastSeenParentDirModifyTime = -1;
    private long lastCheckedTime = -1;
    // todo 集合淘汰策略
    private List<FileNode> lastMatchedFiles;

    public FileMatcher(boolean cachePatternMatching, String dir, String path) {
        this(cachePatternMatching, dir, path, Collections.emptyList());
    }

    public FileMatcher(
            boolean cachePatternMatching, String dir, String path, List<FileNode> matchedFiles) {
        this.cachePatternMatching = cachePatternMatching;
        this.lastMatchedFiles = new ArrayList<>(matchedFiles);
        this.parentDir = new File(dir);
        Preconditions.checkState(
                this.parentDir.exists(),
                "Directory does not exist: " + this.parentDir.getAbsolutePath());

        this.scanDepth = Math.max(1, path.split("/").length - 1 - dir.split("/").length);

        this.fileNamePattern = Pattern.compile(path);
        this.fileFilter =
                new FileFilter() {
                    public boolean accept(File f) {
                        String fileName = f.getName();
                        if (fileNamePattern.pattern().contains("/")) {
                            if (f.isFile()) {
                                return fileNamePattern.matcher(f.getAbsolutePath()).matches();
                            }
                            return true;
                        }
                        return !f.isDirectory() && fileNamePattern.matcher(fileName).matches();
                    }
                };
    }

    public ChangedFiles getChangedFiles() {
        long now =
                TimeUnit.SECONDS.toMillis(
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
        long currentParentDirModifyTime = parentDir.lastModified();

        ChangedFiles changedFiles = ChangedFiles.EMPTY;
        if (!cachePatternMatching
                || lastSeenParentDirModifyTime < currentParentDirModifyTime
                || !(currentParentDirModifyTime < lastCheckedTime)) {
            List<FileNode> currentMatchedFiles = sortFiles(scanFiles(parentDir, scanDepth));
            if (lastMatchedFiles.isEmpty()) {
                changedFiles = ChangedFiles.of(currentMatchedFiles);
            } else {
                changedFiles = ChangedFiles.changes(lastMatchedFiles, currentMatchedFiles);
            }
            lastMatchedFiles = currentMatchedFiles;
            lastSeenParentDirModifyTime = currentParentDirModifyTime;
            lastCheckedTime = now;
        }
        return changedFiles;
    }

    private List<FileNode> scanFiles(File parentDir, int depth) {
        if (fileNamePattern.pattern().indexOf("/") != -1) {
            List<FileNode> allFiles = new ArrayList<>();
            File[] files = parentDir.listFiles(fileFilter);
            if (files == null) {
                return Collections.emptyList();
            }
            for (File file : files) {
                if (file.isDirectory()) {
                    if (depth > 0) {
                        List<FileNode> subFiles = scanFiles(file, depth - 1);
                        allFiles.addAll(subFiles);
                    }
                } else {
                    FileNode fileMetadata = convert(file);
                    if (fileMetadata != null) {
                        allFiles.add(fileMetadata);
                    }
                }
            }
            return allFiles;
        }

        File[] files = parentDir.listFiles(fileFilter);
        return files == null
                ? Collections.emptyList()
                : Arrays.stream(files)
                        .filter(File::isFile)
                        .map(this::convert)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
    }

    private List<FileNode> sortFiles(List<FileNode> files) {
        Collections.sort(files);
        return files;
    }

    private FileNode convert(File file) {
        try {
            return new FileNode(file, Utils.getInode(file));
        } catch (IOException e) {
            log.warn("Failed to get inode for file: " + file.getAbsolutePath(), e);
            return null;
        }
    }
}
