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

import java.util.List;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

/**
 * {@link ParquetFileSplitStrategy} defines a split strategy for Parquet files based on
 * Parquet physical storage units (RowGroups).
 *
 * <p>This strategy uses {@code RowGroup} as the minimum indivisible split unit and
 * generates {@link FileSourceSplit}s by merging one or more contiguous RowGroups
 * according to the configured split size. A split will never break a RowGroup,
 * ensuring correctness and compatibility with Parquet readers.</p>
 *
 * <p>The generated split range ({@code start}, {@code length}) represents a byte range
 * covering complete RowGroups. The actual row-level reading and decoding are delegated
 * to the Parquet reader implementation.</p>
 *
 * <p>This design enables efficient parallel reading of Parquet files while preserving
 * Parquet format semantics and avoiding invalid byte-level splits.</p>
 */
public abstract class ParquetFileSplitStrategy implements FileSplitStrategy {

    private final long splitSize;

    public ParquetFileSplitStrategy(long splitSize) {
        this.splitSize = splitSize;
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        List<FileSourceSplit> splits = new ArrayList<>();
        try {
            Path path = new Path(filePath);
            Configuration conf = new Configuration();
            ParquetMetadata metadata;
            try (ParquetFileReader reader =
                         ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
                metadata = reader.getFooter();
            }
            List<BlockMetaData> rowGroups = metadata.getBlocks();
            // init index
            long currentStart = -1;
            long currentLength = 0;
            // start split
            for (BlockMetaData block : rowGroups) {
                long rgStart = block.getStartingPos();
                long rgSize = block.getCompressedSize();
                // first RowGroup
                if (currentStart < 0) {
                    currentStart = rgStart;
                    currentLength = rgSize;
                    continue;
                }
                // Exceeds splitSize, generates a split
                if (currentLength + rgSize > splitSize) {
                    splits.add(new FileSourceSplit(
                            tableId,
                            filePath,
                            currentStart,
                            currentLength
                    ));
                    // new split
                    currentStart = rgStart;
                    currentLength = rgSize;
                } else {
                    currentLength += rgSize;
                }
            }
            // The last split
            if (currentStart >= 0 && currentLength > 0) {
                splits.add(new FileSourceSplit(
                        tableId,
                        filePath,
                        currentStart,
                        currentLength
                ));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to split parquet file: " + filePath, e);
        }
        return splits;
    }
}

