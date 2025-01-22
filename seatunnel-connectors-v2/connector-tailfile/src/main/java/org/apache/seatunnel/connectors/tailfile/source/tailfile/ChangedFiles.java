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

import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class ChangedFiles {
    public static final ChangedFiles EMPTY = of(Collections.emptyList(), Collections.emptyList());

    private final List<FileNode> addedFiles;
    private final List<FileNode> removedFiles;

    public static ChangedFiles of(List<FileNode> addedFiles, List<FileNode> removedFiles) {
        return new ChangedFiles(addedFiles, removedFiles);
    }

    public static ChangedFiles of(List<FileNode> addedFiles) {
        return new ChangedFiles(addedFiles, Collections.emptyList());
    }

    public static ChangedFiles changes(List<FileNode> oldFiles, List<FileNode> newFiles) {
        Map<Long, FileNode> lastInodeFiles =
                oldFiles.stream()
                        .collect(
                                Collectors.toMap(
                                        FileNode::getInode,
                                        f -> f,
                                        (o, n) -> o,
                                        LinkedHashMap::new));
        Map<Long, FileNode> currentInodeFiles =
                newFiles.stream()
                        .collect(
                                Collectors.toMap(
                                        FileNode::getInode,
                                        f -> f,
                                        (o, n) -> o,
                                        LinkedHashMap::new));
        List<FileNode> addedFiles = new ArrayList<>();
        List<FileNode> removedFiles = new ArrayList<>();
        for (Long inode : currentInodeFiles.keySet()) {
            if (!lastInodeFiles.containsKey(inode)) {
                addedFiles.add(currentInodeFiles.get(inode));
            }
        }
        for (Long inode : lastInodeFiles.keySet()) {
            if (!currentInodeFiles.containsKey(inode)) {
                removedFiles.add(lastInodeFiles.get(inode));
            }
        }
        return new ChangedFiles(addedFiles, removedFiles);
    }
}
