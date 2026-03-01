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
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link AccordingToSplitSizeSplitStrategy} defines a split strategy for text-like files by using
 * {@code rowDelimiter} as the minimum indivisible unit and generating {@link FileSourceSplit}s by
 * merging one or more contiguous rows according to the configured split size.
 *
 * <p>This strategy will never break a row delimiter, ensuring each split starts at a row boundary.
 *
 * <p>To avoid scanning the whole file for large files, this strategy uses {@link FSDataInputStream}
 * seek to locate the next delimiter around each split boundary.
 */
public class AccordingToSplitSizeSplitStrategy implements FileSplitStrategy, Closeable {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final HadoopFileSystemProxy hadoopFileSystemProxy;
    private final long skipHeaderRowNumber;
    private final long splitSize;
    private final byte[] delimiterBytes;

    public AccordingToSplitSizeSplitStrategy(
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
        if (rowDelimiter == null || rowDelimiter.isEmpty()) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_FAIL, "rowDelimiter must not be empty");
        }
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
        this.skipHeaderRowNumber = skipHeaderRowNumber;
        this.splitSize = splitSize;
        this.delimiterBytes = rowDelimiter.getBytes(Charset.forName(encodingName));
        if (delimiterBytes.length == 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_FAIL,
                    "rowDelimiter must not be empty after encoding");
        }
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        String normalizedPath = normalizePath(filePath);
        List<FileSourceSplit> splits = new ArrayList<>();
        long fileSize = safeGetFileSize(normalizedPath);
        if (fileSize == 0) {
            return splits;
        }
        try (FSDataInputStream input = hadoopFileSystemProxy.getInputStream(normalizedPath)) {
            long currentStart = 0;
            if (skipHeaderRowNumber > 0) {
                currentStart = skipLinesUsingBuffer(input, skipHeaderRowNumber);
            }
            while (currentStart < fileSize) {
                long tentativeEnd = currentStart + splitSize;
                if (tentativeEnd >= fileSize) {
                    splits.add(
                            new FileSourceSplit(
                                    tableId,
                                    normalizedPath,
                                    currentStart,
                                    fileSize - currentStart));
                    break;
                }
                long actualEnd = findNextDelimiterWithSeek(input, tentativeEnd, fileSize);
                if (actualEnd <= currentStart) {
                    actualEnd = tentativeEnd;
                }
                splits.add(
                        new FileSourceSplit(
                                tableId, normalizedPath, currentStart, actualEnd - currentStart));
                currentStart = actualEnd;
            }
            return splits;
        } catch (IOException e) {
            throw mapToRuntimeException(normalizedPath, "Split file", e);
        }
    }

    private long safeGetFileSize(String filePath) {
        try {
            return hadoopFileSystemProxy.getFileStatus(filePath).getLen();
        } catch (IOException e) {
            throw mapToRuntimeException(filePath, "Get file status", e);
        }
    }

    private static SeaTunnelRuntimeException mapToRuntimeException(
            String filePath, String operation, IOException e) {
        IOException unwrapped = unwrapRemoteException(e);
        FileConnectorErrorCode errorCode = mapIOExceptionToErrorCode(unwrapped);
        String message =
                String.format(
                        "%s for [%s] failed, cause=%s: %s",
                        operation,
                        filePath,
                        unwrapped.getClass().getSimpleName(),
                        unwrapped.getMessage());
        return new SeaTunnelRuntimeException(errorCode, message, unwrapped);
    }

    private static FileConnectorErrorCode mapIOExceptionToErrorCode(IOException e) {
        if (hasCause(e, FileNotFoundException.class) || hasCause(e, NoSuchFileException.class)) {
            return FileConnectorErrorCode.FILE_NOT_FOUND;
        }
        if (hasCause(e, AccessDeniedException.class) || hasCause(e, AccessControlException.class)) {
            return FileConnectorErrorCode.FILE_ACCESS_DENIED;
        }
        if (hasCause(e, SocketTimeoutException.class)
                || hasCause(e, InterruptedIOException.class)) {
            return FileConnectorErrorCode.FILE_IO_TIMEOUT;
        }
        return FileConnectorErrorCode.FILE_READ_FAILED;
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static IOException unwrapRemoteException(IOException e) {
        if (e instanceof RemoteException) {
            return ((RemoteException) e)
                    .unwrapRemoteException(
                            FileNotFoundException.class,
                            NoSuchFileException.class,
                            AccessControlException.class,
                            AccessDeniedException.class,
                            SocketTimeoutException.class,
                            InterruptedIOException.class);
        }
        return e;
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

    private static String normalizePath(String filePath) {
        if (filePath == null) {
            return null;
        }
        if (filePath.contains("://")) {
            return filePath;
        }
        if (filePath.length() >= 3
                && Character.isLetter(filePath.charAt(0))
                && filePath.charAt(1) == ':'
                && (filePath.charAt(2) == '\\' || filePath.charAt(2) == '/')) {
            return Paths.get(filePath).toUri().toString();
        }
        return filePath;
    }
}
