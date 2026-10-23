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

package org.apache.seatunnel.connectors.seatunnel.file.source.state;

import lombok.Getter;

import java.io.Serializable;

/** Checkpoint state for one local file followed by continuous text tailing. */
@Getter
public class FileTailState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final String fileIdentity;
    private final String filePath;
    private final long committedOffset;
    private final String contentAnchor;
    private final boolean discardUntilDelimiter;
    private final long lastSeenScanGeneration;

    public FileTailState(
            String tableId,
            String fileIdentity,
            String filePath,
            long committedOffset,
            String contentAnchor,
            boolean discardUntilDelimiter,
            long lastSeenScanGeneration) {
        this.tableId = tableId;
        this.fileIdentity = fileIdentity;
        this.filePath = filePath;
        this.committedOffset = committedOffset;
        this.contentAnchor = contentAnchor;
        this.discardUntilDelimiter = discardUntilDelimiter;
        this.lastSeenScanGeneration = lastSeenScanGeneration;
    }
}
