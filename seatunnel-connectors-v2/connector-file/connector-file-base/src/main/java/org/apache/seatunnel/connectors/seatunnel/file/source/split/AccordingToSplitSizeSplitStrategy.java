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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class AccordingToSplitSizeSplitStrategy implements FileSplitStrategy {

    private final long skipHeaderRowNumber;
    private final long splitSize;
    private final byte[] delimiterBytes;
    private static final int BUFFER_SIZE = 64 * 1024;

    public AccordingToSplitSizeSplitStrategy(
            String rowDelimiter, long skipHeaderRowNumber, String encodingName, long splitSize) {
        this.skipHeaderRowNumber = skipHeaderRowNumber;
        this.splitSize = splitSize;
        this.delimiterBytes = rowDelimiter.getBytes(Charset.forName(encodingName));
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        List<FileSourceSplit> splits = new ArrayList<>();
        long fileSize = safeGetFileSize(filePath);
        if (fileSize == 0) {
            return splits;
        }
        long currentStart = 0;
        if (skipHeaderRowNumber > 0) {
            currentStart = skipHeaderWithBuffer(filePath, skipHeaderRowNumber);
        }
        while (currentStart < fileSize) {
            long tentativeEnd = currentStart + splitSize;
            if (tentativeEnd >= fileSize) {
                splits.add(
                        new FileSourceSplit(
                                tableId, filePath, currentStart, fileSize - currentStart));
                break;
            }
            long actualEnd = findNextDelimiterWithBuffer(filePath, tentativeEnd);
            if (actualEnd <= currentStart) {
                actualEnd = tentativeEnd;
            }
            splits.add(
                    new FileSourceSplit(tableId, filePath, currentStart, actualEnd - currentStart));
            currentStart = actualEnd;
        }
        return splits;
    }

    protected abstract InputStream getInputStream(String filePath) throws IOException;

    protected abstract long getFileSize(String filePath) throws IOException;

    private long safeGetFileSize(String filePath) {
        try {
            return getFileSize(filePath);
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    private long skipHeaderWithBuffer(String filePath, long skipLines) {
        try (InputStream input = getInputStream(filePath)) {
            return skipLinesUsingBuffer(input, skipLines);
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    private long skipLinesUsingBuffer(InputStream is, long skipLines) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long matched = 0;
        long lines = 0;
        long pos = 0;
        int n;
        while ((n = is.read(buffer)) != -1) {
            for (int i = 0; i < n; i++) {
                pos++;
                if (buffer[i] == delimiterBytes[(int) matched]) {
                    matched++;
                    if (matched == delimiterBytes.length) {
                        matched = 0;
                        lines++;
                        if (lines >= skipLines) {
                            return pos;
                        }
                    }
                } else {
                    matched = 0;
                }
            }
        }

        return pos;
    }

    private long findNextDelimiterWithBuffer(String filePath, long startPos) {
        try (InputStream is = getInputStream(filePath)) {
            long skipped = skipManually(is, startPos);
            if (skipped < startPos) {
                return startPos;
            }
            byte[] buffer = new byte[BUFFER_SIZE];
            long matched = 0;
            long pos = startPos;
            int n;
            while ((n = is.read(buffer)) != -1) {
                for (int i = 0; i < n; i++) {
                    pos++;
                    if (buffer[i] == delimiterBytes[(int) matched]) {
                        matched++;
                        if (matched == delimiterBytes.length) {
                            return pos;
                        }
                    } else {
                        matched = 0;
                    }
                }
            }
            return pos;

        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    private long skipManually(InputStream is, long bytesToSkip) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long total = 0;
        while (total < bytesToSkip) {
            long toRead = Math.min(buffer.length, bytesToSkip - total);
            int n = is.read(buffer, 0, (int) toRead);
            if (n == -1) break;
            total += n;
        }
        return total;
    }
}
