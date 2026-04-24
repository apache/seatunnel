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

package org.apache.seatunnel.connectors.seatunnel.file.sink.commit;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

class FileSinkAggregatedCommitterTest {

    private static class TestableCommitter extends FileSinkAggregatedCommitter {
        TestableCommitter() {
            super(new HadoopConf("hdfs://dummy"));
        }

        void setFileSystemProxy(HadoopFileSystemProxy proxy) {
            this.hadoopFileSystemProxy = proxy;
        }
    }

    @Test
    void shouldCleanEmptyTransactionParentDirectoriesAfterCommit() throws Exception {
        String tmpRoot = "/tmp/seatunnel/seatunnel";
        String jobDir = tmpRoot + "/job-1";
        String uuidDir = jobDir + "/uuid-1";
        String transactionDir = uuidDir + "/T_job-1_uuid-1_0_1";
        String tempFile = transactionDir + "/NON_PARTITION/out.txt";
        String targetFile = "/warehouse/table/out.txt";

        LinkedHashMap<String, String> fileMoves = new LinkedHashMap<>();
        fileMoves.put(tempFile, targetFile);

        LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap = new LinkedHashMap<>();
        transactionMap.put(transactionDir, fileMoves);

        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        TestableCommitter committer = new TestableCommitter();
        committer.setFileSystemProxy(fs);

        List<FileAggregatedCommitInfo> errors =
                committer.commit(
                        Collections.singletonList(
                                new FileAggregatedCommitInfo(
                                        transactionMap, new LinkedHashMap<>())));

        Assertions.assertTrue(errors.isEmpty());
        Mockito.verify(fs).renameFile(tempFile, targetFile, true);
        Mockito.verify(fs).deleteFile(transactionDir);
        Mockito.verify(fs).deleteEmptyDirectory(uuidDir);
        Mockito.verify(fs).deleteEmptyDirectory(jobDir);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(tmpRoot);
    }
}
