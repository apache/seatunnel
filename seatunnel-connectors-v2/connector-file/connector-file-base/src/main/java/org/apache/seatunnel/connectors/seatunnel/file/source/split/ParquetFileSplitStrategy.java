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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ParquetFileSplitStrategy} defines a split strategy for Parquet files based on Parquet
 * physical storage units (RowGroups).
 *
 * <p>This strategy uses {@code RowGroup} as the minimum indivisible split unit and generates {@link
 * FileSourceSplit}s by merging one or more contiguous RowGroups according to the configured split
 * size. A split will never break a RowGroup, ensuring correctness and compatibility with Parquet
 * readers.
 *
 * <p>The generated split range ({@code start}, {@code length}) represents a byte range covering
 * complete RowGroups. The actual row-level reading and decoding are delegated to the Parquet reader
 * implementation.
 *
 * <p>This design enables efficient parallel reading of Parquet files while preserving Parquet
 * format semantics and avoiding invalid byte-level splits.
 */
public class ParquetFileSplitStrategy implements FileSplitStrategy, Closeable {

    private final long splitSizeBytes;
    private final HadoopFileSystemProxy hadoopFileSystemProxy;

    public ParquetFileSplitStrategy(long splitSizeBytes) {
        if (splitSizeBytes <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    String.format(
                            "file_split_size must be greater than 0 when enable_file_split=true, but got: %d",
                            splitSizeBytes));
        }
        this.splitSizeBytes = splitSizeBytes;
        this.hadoopFileSystemProxy = null;
    }

    public ParquetFileSplitStrategy(long splitSizeBytes, HadoopConf hadoopConf) {
        if (splitSizeBytes <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    String.format(
                            "file_split_size must be greater than 0 when enable_file_split=true, but got: %d",
                            splitSizeBytes));
        }
        this.splitSizeBytes = splitSizeBytes;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        try {
            return splitByRowGroups(tableId, filePath, readRowGroups(filePath));
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_FAIL,
                    String.format(
                            "Split parquet file for [%s] failed, cause=%s: %s",
                            filePath, e.getClass().getSimpleName(), e.getMessage()),
                    e);
        }
    }

    /**
     * Core split logic based on row group metadata. This method is IO-free and unit-test friendly.
     */
    List<FileSourceSplit> splitByRowGroups(
            String tableId, String filePath, List<BlockMetaData> rowGroups) {
        List<FileSourceSplit> splits = new ArrayList<>();
        if (rowGroups == null || rowGroups.isEmpty()) {
            return splits;
        }
        long currentStart = 0;
        long currentEnd = 0;
        boolean hasOpenSplit = false;
        for (BlockMetaData block : rowGroups) {
            long rgStart = block.getStartingPos();
            long rgSize = block.getCompressedSize();
            long rgEnd = rgStart + rgSize;
            // start a new split
            if (!hasOpenSplit) {
                currentStart = rgStart;
                currentEnd = rgEnd;
                hasOpenSplit = true;
                continue;
            }
            // exceeds threshold, close current split
            if (rgEnd - currentStart > splitSizeBytes) {
                splits.add(
                        new FileSourceSplit(
                                tableId, filePath, currentStart, currentEnd - currentStart));
                // start next split
                currentStart = rgStart;
                currentEnd = rgEnd;
            } else {
                currentEnd = rgEnd;
            }
        }
        // last split
        if (hasOpenSplit && currentEnd > currentStart) {
            splits.add(
                    new FileSourceSplit(
                            tableId, filePath, currentStart, currentEnd - currentStart));
        }
        return splits;
    }

    private List<BlockMetaData> readRowGroups(String filePath) throws IOException {
        Path path = new Path(filePath);
        if (hadoopFileSystemProxy == null) {
            Configuration conf = new Configuration();
            try (ParquetFileReader reader =
                    ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
                return reader.getFooter().getBlocks();
            }
        }
        try {
            return hadoopFileSystemProxy.doWithHadoopAuth(
                    (configuration, userGroupInformation) -> {
                        try (ParquetFileReader reader =
                                ParquetFileReader.open(
                                        HadoopInputFile.fromPath(path, configuration))) {
                            return reader.getFooter().getBlocks();
                        }
                    });
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (hadoopFileSystemProxy == null) {
            return;
        }
        hadoopFileSystemProxy.close();
    }
}
