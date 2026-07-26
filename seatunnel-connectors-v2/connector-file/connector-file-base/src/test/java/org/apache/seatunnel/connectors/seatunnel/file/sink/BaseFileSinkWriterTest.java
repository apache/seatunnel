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

package org.apache.seatunnel.connectors.seatunnel.file.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;

class BaseFileSinkWriterTest {

    private static final String JOB_ID = "job-1";
    private static final String UUID_PREFIX = "uuid-1";
    private static final String TRANSACTION_ID = "T_job-1_uuid-1_0_7";
    private static final String TRANSACTION_DIR = "/tmp/seatunnel/" + TRANSACTION_ID;

    @Test
    void shouldNotReplayCommittedTransactionWhenRestoringWriterState() throws Exception {
        WriteStrategy<?> writeStrategy = Mockito.mock(WriteStrategy.class);
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        FileSinkConfig fileSinkConfig = Mockito.mock(FileSinkConfig.class);
        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
        FileSinkState state =
                new FileSinkState(
                        TRANSACTION_ID,
                        UUID_PREFIX,
                        7L,
                        new LinkedHashMap<>(),
                        new LinkedHashMap<>(),
                        TRANSACTION_DIR);

        Mockito.when(context.getIndexOfSubtask()).thenReturn(0);
        Mockito.when(writeStrategy.getHadoopFileSystemProxy()).thenReturn(fileSystemProxy);
        Mockito.when(writeStrategy.getFileSinkConfig()).thenReturn(fileSinkConfig);
        Mockito.when(fileSinkConfig.getTmpPath()).thenReturn("/tmp/seatunnel");
        Mockito.when(fileSystemProxy.getAllSubFiles(Mockito.anyString()))
                .thenReturn(Collections.singletonList(new Path(TRANSACTION_ID)));

        try (MockedConstruction<FileSinkAggregatedCommitter> ignored =
                Mockito.mockConstruction(FileSinkAggregatedCommitter.class)) {
            new BaseFileSinkWriter(
                    writeStrategy,
                    new HadoopConf("hdfs://dummy"),
                    context,
                    JOB_ID,
                    Collections.singletonList(state));

            Assertions.assertTrue(
                    ignored.constructed().isEmpty(),
                    "writer recovery must leave checkpoint commit replay to the aggregated committer");
        }

        Mockito.verify(writeStrategy).beginTransaction(8L);
        Mockito.verify(writeStrategy, Mockito.never()).abortPrepare(TRANSACTION_ID);
    }
}
