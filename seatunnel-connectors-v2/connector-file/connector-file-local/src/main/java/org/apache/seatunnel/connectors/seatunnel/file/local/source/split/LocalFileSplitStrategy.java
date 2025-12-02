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
package org.apache.seatunnel.connectors.seatunnel.file.local.source.split;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LocalFileSplitStrategy extends DefaultFileSplitStrategy {

    private final String rowDelimiter;
    private final long skipHeaderRowNumber;
    private final String encodingName;
    private final long splitSize;

    public LocalFileSplitStrategy(
            String rowDelimiter, long skipHeaderRowNumber, String encodingName, long splitSize) {
        this.rowDelimiter = rowDelimiter;
        this.skipHeaderRowNumber = skipHeaderRowNumber;
        this.encodingName = encodingName;
        this.splitSize = splitSize;
    }

    public List<FileSourceSplit> split(String tableId, String filePath) {
        List<FileSourceSplit> splits = new ArrayList<>();
        Path path = toLocalNioPath(filePath);
        long fileSize;
        try {
            fileSize = Files.size(path);
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    "Cannot read file size: " + filePath,
                    e);
        }
        if (fileSize == 0) {
            return splits;
        }
        long currentStart = 0;
        long skipBytes = 0;
        if (skipHeaderRowNumber > 0) {
            try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(path))) {
                skipBytes = skipHeader(bis, rowDelimiter, skipHeaderRowNumber);
            } catch (Exception e) {
                throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
            }
            currentStart = skipBytes;
        }
        while (currentStart < fileSize) {
            long tentativeEnd = currentStart + splitSize;
            if (tentativeEnd >= fileSize) {
                splits.add(
                        new FileSourceSplit(
                                tableId, filePath, currentStart, fileSize - currentStart));
                break;
            }
            long actualEnd = adjustToLineEnd(path, tentativeEnd, rowDelimiter);
            if (actualEnd <= currentStart) {
                actualEnd = tentativeEnd;
            }
            splits.add(
                    new FileSourceSplit(tableId, filePath, currentStart, actualEnd - currentStart));
            currentStart = actualEnd;
        }
        return splits;
    }

    private long skipHeader(BufferedInputStream bis, String delimiter, long skipLines)
            throws IOException {
        byte[] delimBytes = delimiter.getBytes(Charset.forName(encodingName));
        int matched = 0;
        long pos = 0;
        int ch;
        int lines = 0;
        while ((ch = bis.read()) != -1) {
            pos++;
            if (ch == delimBytes[matched]) {
                matched++;
                if (matched == delimBytes.length) {
                    matched = 0;
                    lines++;
                    if (lines >= skipLines) break;
                }
            } else {
                matched = 0;
            }
        }
        return pos;
    }

    private long adjustToLineEnd(Path path, long pos, String delimiter) {
        byte[] delimBytes = delimiter.getBytes(Charset.forName(encodingName));
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(path))) {
            bis.skip(pos);
            int matched = 0;
            int ch;
            long cur = pos;
            while ((ch = bis.read()) != -1) {
                cur++;
                if (ch == delimBytes[matched]) {
                    matched++;
                    if (matched == delimBytes.length) {
                        return cur;
                    }
                } else {
                    matched = 0;
                }
            }
            return cur;
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    public static Path toLocalNioPath(String filePath) {
        try {
            URI uri = URI.create(filePath);
            if (uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("file")) {
                return Paths.get(uri);
            }
        } catch (Exception ignored) {
            // nothing
        }
        return Paths.get(filePath);
    }
}
