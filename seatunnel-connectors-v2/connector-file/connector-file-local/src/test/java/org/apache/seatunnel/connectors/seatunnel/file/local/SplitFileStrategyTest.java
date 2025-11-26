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
package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

public class SplitFileStrategyTest {

    @SneakyThrows
    @Test
    public void testSplitNoSkipHeader() {
        final LocalFileSplitStrategy localFileSplitStrategy =
                new LocalFileSplitStrategy("\n", 0L, StandardCharsets.UTF_8, 100L);
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        final List<FileSourceSplit> splits = localFileSplitStrategy.split("test.table", realPath);
        Assertions.assertEquals(2, splits.size());
        // check split-1
        Assertions.assertEquals(0, splits.get(0).getStart());
        Assertions.assertEquals(105, splits.get(0).getLength());
        // check split-2
        Assertions.assertEquals(105, splits.get(1).getStart());
        Assertions.assertEquals(85, splits.get(1).getLength());
    }

    @SneakyThrows
    @Test
    public void testSplitSkipHeader() {
        final LocalFileSplitStrategy localFileSplitStrategy =
                new LocalFileSplitStrategy("\n", 1L, StandardCharsets.UTF_8, 30L);
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        final List<FileSourceSplit> splits = localFileSplitStrategy.split("test.table", realPath);
        Assertions.assertEquals(4, splits.size());
        // check split-1
        Assertions.assertEquals(21, splits.get(0).getStart());
        Assertions.assertEquals(41, splits.get(0).getLength());
        // check split-2
        Assertions.assertEquals(62, splits.get(1).getStart());
        Assertions.assertEquals(43, splits.get(1).getLength());
        // check split-3
        Assertions.assertEquals(105, splits.get(2).getStart());
        Assertions.assertEquals(43, splits.get(2).getLength());
        // check split-4
        Assertions.assertEquals(148, splits.get(3).getStart());
        Assertions.assertEquals(42, splits.get(3).getLength());
    }

    @SneakyThrows
    @Test
    public void testSplitSkipHeaderLargeSize() {
        final LocalFileSplitStrategy localFileSplitStrategy =
                new LocalFileSplitStrategy("\n", 1L, StandardCharsets.UTF_8, 300L);
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        final List<FileSourceSplit> splits = localFileSplitStrategy.split("test.table", realPath);
        Assertions.assertEquals(1, splits.size());
        // check split-1
        Assertions.assertEquals(21, splits.get(0).getStart());
        Assertions.assertEquals(169, splits.get(0).getLength());
    }

    @SneakyThrows
    @Test
    public void testSplitSkipHeaderSmallSize() {
        final LocalFileSplitStrategy localFileSplitStrategy =
                new LocalFileSplitStrategy("\n", 1L, StandardCharsets.UTF_8, 3L);
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        final List<FileSourceSplit> splits = localFileSplitStrategy.split("test.table", realPath);
        Assertions.assertEquals(8, splits.size());
        // check split-1
        Assertions.assertEquals(21, splits.get(0).getStart());
        Assertions.assertEquals(42, splits.get(1).getStart());
        Assertions.assertEquals(62, splits.get(2).getStart());
        Assertions.assertEquals(82, splits.get(3).getStart());
        Assertions.assertEquals(105, splits.get(4).getStart());
        Assertions.assertEquals(126, splits.get(5).getStart());
        Assertions.assertEquals(148, splits.get(6).getStart());
        Assertions.assertEquals(169, splits.get(7).getStart());
    }

    @SneakyThrows
    @Test
    public void testSplitSkipHeaderSpecialRowDelimiter() {
        final LocalFileSplitStrategy localFileSplitStrategy =
                new LocalFileSplitStrategy("|^|", 1L, StandardCharsets.UTF_8, 80L);
        URL url =
                getClass()
                        .getClassLoader()
                        .getResource("test_split_special_row_delimiter_data.text");
        String realPath = Paths.get(url.toURI()).toString();
        final List<FileSourceSplit> splits = localFileSplitStrategy.split("test.table", realPath);
        Assertions.assertEquals(2, splits.size());
        // check split-1
        Assertions.assertEquals(23, splits.get(0).getStart());
        Assertions.assertEquals(92, splits.get(0).getLength());
        // check split-1
        Assertions.assertEquals(115, splits.get(1).getStart());
        Assertions.assertEquals(91, splits.get(1).getLength());
    }
}
