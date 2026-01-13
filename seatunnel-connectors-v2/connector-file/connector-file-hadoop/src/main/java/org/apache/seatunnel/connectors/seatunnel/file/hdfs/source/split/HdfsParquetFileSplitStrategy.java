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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsParquetFileSplitStrategy implements FileSplitStrategy, Closeable {

    private final long splitSizeBytes;
    private final HadoopFileSystemProxy hadoopFileSystemProxy;

    public HdfsParquetFileSplitStrategy(long splitSizeBytes, HadoopConf hadoopConf) {
        if (splitSizeBytes <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    "SplitSizeBytes must be greater than 0");
        }
        this.splitSizeBytes = splitSizeBytes;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        try {
            return splitByRowGroups(tableId, filePath, readRowGroups(filePath));
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_SPLIT_FAIL, e);
        }
    }

    List<FileSourceSplit> splitByRowGroups(
            String tableId, String filePath, List<BlockMetaData> rowGroups) {
        List<FileSourceSplit> splits = new ArrayList<>();
        if (rowGroups == null || rowGroups.isEmpty()) {
            return splits;
        }
        long currentStart = 0;
        long currentLength = 0;
        boolean hasOpenSplit = false;
        for (BlockMetaData block : rowGroups) {
            long rgStart = block.getStartingPos();
            long rgSize = block.getCompressedSize();
            if (!hasOpenSplit) {
                currentStart = rgStart;
                currentLength = rgSize;
                hasOpenSplit = true;
                continue;
            }
            if (currentLength + rgSize > splitSizeBytes) {
                splits.add(new FileSourceSplit(tableId, filePath, currentStart, currentLength));
                currentStart = rgStart;
                currentLength = rgSize;
            } else {
                currentLength += rgSize;
            }
        }
        if (hasOpenSplit && currentLength > 0) {
            splits.add(new FileSourceSplit(tableId, filePath, currentStart, currentLength));
        }
        return splits;
    }

    private List<BlockMetaData> readRowGroups(String filePath) throws IOException {
        Path path = new Path(filePath);
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
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        hadoopFileSystemProxy.close();
    }
}
