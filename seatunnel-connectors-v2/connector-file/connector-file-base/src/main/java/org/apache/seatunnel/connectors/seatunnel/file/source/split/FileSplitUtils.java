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

import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for splitting files based on size while maintaining row integrity. Currently
 * supports CSV files only.
 */
@Slf4j
public class FileSplitUtils {

    private static final int READ_BUFFER_SIZE = 8192;
    private static final byte[] LINE_SEPARATORS = {'\n', '\r'};

    /**
     * Generate file splits for a given file if it's larger than the split size.
     *
     * @param filePath the file path
     * @param tableId the table identifier
     * @param fileFormat the file format
     * @param splitSizeMB the split size in MB
     * @param hadoopFileSystemProxy the hadoop file system proxy
     * @return list of file source splits
     * @throws IOException if file operations fail
     */
    public static List<FileSourceSplit> generateFileSplits(
            String filePath,
            String tableId,
            FileFormat fileFormat,
            int splitSizeMB,
            HadoopFileSystemProxy hadoopFileSystemProxy)
            throws IOException {

        List<FileSourceSplit> splits = new ArrayList<>();

        // Only support text-based formats that can be safely split
        if (!supportsSplitting(fileFormat) || splitSizeMB <= 0) {
            // Return a single split for the entire file
            splits.add(new FileSourceSplit(tableId, filePath));
            return splits;
        }

        FileStatus fileStatus = hadoopFileSystemProxy.getFileStatus(filePath);
        long fileSize = fileStatus.getLen();
        long splitSizeBytes = splitSizeMB * 1024L * 1024L;

        // If file is smaller than split size, return single split
        if (fileSize <= splitSizeBytes) {
            splits.add(new FileSourceSplit(tableId, filePath));
            return splits;
        }

        // Precheck: some filesystems/streams may not support seeking (e.g., ftp). If seek is not
        // supported, fall back to a single split to ensure correctness.
        if (!supportsSeek(hadoopFileSystemProxy, filePath)) {
            log.warn(
                    "The underlying input stream does not support seek operation. "
                            + "File splitting will be disabled for file: {}",
                    filePath);
            splits.add(new FileSourceSplit(tableId, filePath));
            return splits;
        }

        // Calculate split positions
        List<Long> splitPositions =
                calculateSplitPositions(filePath, fileSize, splitSizeBytes, hadoopFileSystemProxy);

        // Create splits based on positions
        for (int i = 0; i < splitPositions.size(); i++) {
            long startOffset = i == 0 ? 0 : splitPositions.get(i - 1);
            long length =
                    (i == splitPositions.size() - 1)
                            ? (fileSize - startOffset)
                            : (splitPositions.get(i) - startOffset);
            boolean isFirstSplit = (i == 0);

            splits.add(new FileSourceSplit(tableId, filePath, startOffset, length, isFirstSplit));
        }

        log.info(
                "Split file {} ({} bytes) into {} splits of approximately {} MB each",
                filePath,
                fileSize,
                splits.size(),
                splitSizeMB);

        return splits;
    }

    /** Calculate split positions that respect row boundaries. */
    private static List<Long> calculateSplitPositions(
            String filePath,
            long fileSize,
            long splitSizeBytes,
            HadoopFileSystemProxy hadoopFileSystemProxy)
            throws IOException {

        List<Long> positions = new ArrayList<>();

        try (FSDataInputStream inputStream = hadoopFileSystemProxy.getInputStream(filePath)) {
            long currentPosition = 0;

            while (currentPosition < fileSize) {
                long targetPosition = Math.min(currentPosition + splitSizeBytes, fileSize);

                if (targetPosition >= fileSize) {
                    // No more splits needed
                    break;
                }

                // Find the next line boundary after targetPosition
                long actualSplitPosition =
                        findNextLineBoundary(inputStream, targetPosition, fileSize);

                if (actualSplitPosition > currentPosition) {
                    positions.add(actualSplitPosition);
                    currentPosition = actualSplitPosition;
                } else {
                    // If we can't find a line boundary, break to avoid infinite loop
                    break;
                }
            }
        }

        return positions;
    }

    /** Find the next line boundary after the given position. */
    private static long findNextLineBoundary(
            FSDataInputStream inputStream, long startPosition, long fileSize) throws IOException {

        inputStream.seek(startPosition);

        byte[] buffer = new byte[READ_BUFFER_SIZE];
        long currentPosition = startPosition;

        while (currentPosition < fileSize) {
            int bytesToRead = (int) Math.min(buffer.length, fileSize - currentPosition);
            int bytesRead = inputStream.read(buffer, 0, bytesToRead);

            if (bytesRead <= 0) {
                break;
            }

            // Look for line separators
            for (int i = 0; i < bytesRead; i++) {
                byte b = buffer[i];
                for (byte separator : LINE_SEPARATORS) {
                    if (b == separator) {
                        long foundPosition = currentPosition + i + 1;

                        // Handle \r\n case
                        if (b == '\r' && foundPosition < fileSize) {
                            inputStream.seek(foundPosition);
                            int nextByte = inputStream.read();
                            if (nextByte == '\n') {
                                foundPosition++;
                            }
                        }

                        return foundPosition;
                    }
                }
            }

            currentPosition += bytesRead;
        }

        // If no line boundary found, return the file size
        return fileSize;
    }

    /**
     * Best-effort detection whether the current filesystem/stream supports random seek.
     *
     * <p>Implementation note: Hadoop's {@link FSDataInputStream} normally supports seek, but for
     * some schemes or custom implementations it may throw {@link UnsupportedOperationException}.
     * Here we defensively try a no-op seek (to the current position). If this fails, we assume
     * seeking is not supported and disable splitting.
     */
    private static boolean supportsSeek(
            HadoopFileSystemProxy hadoopFileSystemProxy, String filePath) {
        FSDataInputStream inputStream = null;
        try {
            inputStream = hadoopFileSystemProxy.getInputStream(filePath);
            long pos = inputStream.getPos();
            inputStream.seek(pos);
            return true;
        } catch (UnsupportedOperationException | IOException e) {
            return false;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /** Check if a file format supports splitting. */
    public static boolean supportsSplitting(FileFormat fileFormat) {
        // Text-based row-oriented formats support splitting
        // Binary formats like PARQUET, ORC, EXCEL cannot be split at arbitrary byte boundaries
        switch (fileFormat) {
            case CSV:
            case TEXT:
            case JSON:
            case XML:
                return true;
            case PARQUET:
            case ORC:
            case EXCEL:
            case BINARY:
            case CANAL_JSON:
            case DEBEZIUM_JSON:
            case MAXWELL_JSON:
            default:
                return false;
        }
    }

    /** Estimate the number of splits for a file. */
    public static int estimateSplitCount(long fileSize, int splitSizeMB) {
        if (splitSizeMB <= 0) {
            return 1;
        }
        long splitSizeBytes = splitSizeMB * 1024L * 1024L;
        return (int) Math.ceil((double) fileSize / splitSizeBytes);
    }
}
