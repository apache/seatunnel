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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSourceReader;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ParallelSourceTest {

    @Test
    void testParallelSourceForPollingFileAllocation() throws Exception {
        int fileSize = 15;
        int parallelism = 4;

        // create file source
        BaseFileSource baseFileSource =
                new BaseFileSource() {
                    @Override
                    public void prepare(Config pluginConfig) throws PrepareFailException {
                        filePaths = new ArrayList<>();
                        for (int i = 0; i < fileSize; i++) {
                            filePaths.add("file" + i + ".txt");
                        }
                    }

                    @Override
                    public String getPluginName() {
                        return FileSystemType.HDFS.getFileSystemPluginName();
                    }
                };

        // prepare files
        baseFileSource.prepare(null);

        ParallelSource parallelSource =
                new ParallelSource(baseFileSource, null, parallelism, "parallel-source-test", 0);
        ParallelSource parallelSource2 =
                new ParallelSource(baseFileSource, null, parallelism, "parallel-source-test2", 1);
        ParallelSource parallelSource3 =
                new ParallelSource(baseFileSource, null, parallelism, "parallel-source-test3", 2);
        ParallelSource parallelSource4 =
                new ParallelSource(baseFileSource, null, parallelism, "parallel-source-test4", 3);

        parallelSource.open();
        parallelSource2.open();
        parallelSource3.open();
        parallelSource4.open();

        // execute file allocation process
        parallelSource.splitEnumerator.run();
        parallelSource2.splitEnumerator.run();
        parallelSource3.splitEnumerator.run();
        parallelSource4.splitEnumerator.run();

        // Gets the splits assigned for each reader
        List<FileSourceSplit> sourceSplits =
                ((BaseFileSourceReader) parallelSource.reader).snapshotState(0);
        List<FileSourceSplit> sourceSplits2 =
                ((BaseFileSourceReader) parallelSource2.reader).snapshotState(0);
        List<FileSourceSplit> sourceSplits3 =
                ((BaseFileSourceReader) parallelSource3.reader).snapshotState(0);
        List<FileSourceSplit> sourceSplits4 =
                ((BaseFileSourceReader) parallelSource4.reader).snapshotState(0);

        log.info(
                "parallel source1 splits => {}",
                sourceSplits.stream().map(FileSourceSplit::splitId).collect(Collectors.toList()));

        log.info(
                "parallel source2 splits => {}",
                sourceSplits2.stream().map(FileSourceSplit::splitId).collect(Collectors.toList()));

        log.info(
                "parallel source3 splits => {}",
                sourceSplits3.stream().map(FileSourceSplit::splitId).collect(Collectors.toList()));

        log.info(
                "parallel source4 splits => {}",
                sourceSplits4.stream().map(FileSourceSplit::splitId).collect(Collectors.toList()));

        // check that there are no duplicate file assignments
        Set<FileSourceSplit> splitSet = new HashSet<>();
        splitSet.addAll(sourceSplits);
        splitSet.addAll(sourceSplits2);
        splitSet.addAll(sourceSplits3);
        splitSet.addAll(sourceSplits4);

        Assertions.assertEquals(splitSet.size(), fileSize);
    }
}
