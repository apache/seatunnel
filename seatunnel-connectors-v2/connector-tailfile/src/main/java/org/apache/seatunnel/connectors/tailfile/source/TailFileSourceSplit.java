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

package org.apache.seatunnel.connectors.tailfile.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.FileHarvester;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.FileNode;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@ToString
@RequiredArgsConstructor
public class TailFileSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String filepath;
    private final long inode;
    private final long lastModified;
    private final long pos;
    private final boolean needTail;

    @Override
    public String splitId() {
        return String.valueOf(inode);
    }

    public static TailFileSourceSplit of(FileNode fileNode) {
        return new TailFileSourceSplit(
                fileNode.getPath(), fileNode.getInode(), fileNode.getLastModified(), 0, true);
    }

    public static TailFileSourceSplit of(FileHarvester fileHarvester) {
        return new TailFileSourceSplit(
                fileHarvester.getPath(),
                fileHarvester.getInode(),
                fileHarvester.getLastUpdated(),
                fileHarvester.getFileStartPos(),
                fileHarvester.isNeedTail());
    }
}
