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

import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.parquet.hadoop.metadata.BlockMetaData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

public class HdfsParquetFileSplitStrategyTest {

    private static final String TABLE_ID = "test.table";
    private static final String FILE_PATH = "/tmp/test.parquet";

    @Test
    void testSplitByRowGroupsEmpty() throws Exception {
        try (HdfsParquetFileSplitStrategy strategy =
                new HdfsParquetFileSplitStrategy(100, new HdfsFileHadoopConfig("file:///"))) {
            List<FileSourceSplit> splits =
                    strategy.splitByRowGroups(TABLE_ID, FILE_PATH, Collections.emptyList());
            Assertions.assertTrue(splits.isEmpty());
        }
    }

    @Test
    void testSplitByRowGroupsSingleRowGroup() throws Exception {
        try (HdfsParquetFileSplitStrategy strategy =
                new HdfsParquetFileSplitStrategy(1000, new HdfsFileHadoopConfig("file:///"))) {
            List<BlockMetaData> blocks = new ArrayList<>();
            blocks.add(mockBlock(0, 200));
            List<FileSourceSplit> splits = strategy.splitByRowGroups(TABLE_ID, FILE_PATH, blocks);
            Assertions.assertEquals(1, splits.size());
            FileSourceSplit split = splits.get(0);
            Assertions.assertEquals(0, split.getStart());
            Assertions.assertEquals(200, split.getLength());
        }
    }

    @Test
    void testSplitByRowGroupsMergeRowGroups() throws Exception {
        try (HdfsParquetFileSplitStrategy strategy =
                new HdfsParquetFileSplitStrategy(500, new HdfsFileHadoopConfig("file:///"))) {
            List<BlockMetaData> blocks = new ArrayList<>();
            blocks.add(mockBlock(0, 100));
            blocks.add(mockBlock(100, 150));
            blocks.add(mockBlock(250, 200));
            List<FileSourceSplit> splits = strategy.splitByRowGroups(TABLE_ID, FILE_PATH, blocks);
            Assertions.assertEquals(1, splits.size());
            FileSourceSplit split = splits.get(0);
            Assertions.assertEquals(0, split.getStart());
            Assertions.assertEquals(450, split.getLength());
        }
    }

    @Test
    void testSplitByRowGroupsSplitWhenExceedsThreshold() throws Exception {
        try (HdfsParquetFileSplitStrategy strategy =
                new HdfsParquetFileSplitStrategy(300, new HdfsFileHadoopConfig("file:///"))) {
            List<BlockMetaData> blocks = new ArrayList<>();
            blocks.add(mockBlock(0, 100));
            blocks.add(mockBlock(100, 150));
            blocks.add(mockBlock(250, 200));
            List<FileSourceSplit> splits = strategy.splitByRowGroups(TABLE_ID, FILE_PATH, blocks);
            Assertions.assertEquals(2, splits.size());
            FileSourceSplit first = splits.get(0);
            Assertions.assertEquals(0, first.getStart());
            Assertions.assertEquals(250, first.getLength());
            FileSourceSplit second = splits.get(1);
            Assertions.assertEquals(250, second.getStart());
            Assertions.assertEquals(200, second.getLength());
        }
    }

    private BlockMetaData mockBlock(long start, long compressedSize) {
        BlockMetaData block = Mockito.mock(BlockMetaData.class);
        when(block.getStartingPos()).thenReturn(start);
        when(block.getCompressedSize()).thenReturn(compressedSize);
        return block;
    }
}
