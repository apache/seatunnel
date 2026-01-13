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
package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.split;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HdfsFileAccordingToSplitSizeSplitStrategy implements FileSplitStrategy, Closeable {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final HadoopFileSystemProxy hadoopFileSystemProxy;
    private final long skipHeaderRowNumber;
    private final long splitSize;
    private final byte[] delimiterBytes;

    public HdfsFileAccordingToSplitSizeSplitStrategy(
            HadoopConf hadoopConf,
            String rowDelimiter,
            long skipHeaderRowNumber,
            String encodingName,
            long splitSize) {
        if (splitSize <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    "SplitSizeBytes must be greater than 0");
        }
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
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
        try (FSDataInputStream input = hadoopFileSystemProxy.getInputStream(filePath)) {
            long currentStart = 0;
            if (skipHeaderRowNumber > 0) {
                currentStart = skipLinesUsingBuffer(input, skipHeaderRowNumber);
            }
            while (currentStart < fileSize) {
                long tentativeEnd = currentStart + splitSize;
                if (tentativeEnd >= fileSize) {
                    splits.add(
                            new FileSourceSplit(
                                    tableId, filePath, currentStart, fileSize - currentStart));
                    break;
                }
                long actualEnd = findNextDelimiterWithSeek(input, tentativeEnd, fileSize);
                if (actualEnd <= currentStart) {
                    actualEnd = tentativeEnd;
                }
                splits.add(
                        new FileSourceSplit(
                                tableId, filePath, currentStart, actualEnd - currentStart));
                currentStart = actualEnd;
            }
            return splits;
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    private long safeGetFileSize(String filePath) {
        try {
            return hadoopFileSystemProxy.getFileStatus(filePath).getLen();
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    private long skipLinesUsingBuffer(FSDataInputStream input, long skipLines) throws IOException {
        input.seek(0);
        byte[] buffer = new byte[BUFFER_SIZE];
        int matched = 0;
        long lines = 0;
        long pos = 0;
        int n;
        while ((n = input.read(buffer)) != -1) {
            for (int i = 0; i < n; i++) {
                pos++;
                if (buffer[i] == delimiterBytes[matched]) {
                    matched++;
                    if (matched == delimiterBytes.length) {
                        matched = 0;
                        lines++;
                        if (lines >= skipLines) {
                            return pos;
                        }
                    }
                } else {
                    matched = buffer[i] == delimiterBytes[0] ? 1 : 0;
                }
            }
        }
        return pos;
    }

    private long findNextDelimiterWithSeek(FSDataInputStream input, long startPos, long fileSize)
            throws IOException {
        long scanStart = Math.max(0, startPos - (delimiterBytes.length - 1));
        input.seek(scanStart);
        byte[] buffer = new byte[BUFFER_SIZE];
        int matched = 0;
        long pos = scanStart;
        int n;
        while ((n = input.read(buffer)) != -1) {
            for (int i = 0; i < n; i++) {
                pos++;
                if (buffer[i] == delimiterBytes[matched]) {
                    matched++;
                    if (matched == delimiterBytes.length) {
                        long endPos = pos;
                        if (endPos >= startPos) {
                            return endPos;
                        }
                        matched = 0;
                    }
                } else {
                    matched = buffer[i] == delimiterBytes[0] ? 1 : 0;
                }
            }
        }
        return Math.min(fileSize, pos);
    }

    @Override
    public void close() throws IOException {
        hadoopFileSystemProxy.close();
    }
}
