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

    private static final String TMP_ROOT = "/tmp/seatunnel/seatunnel";
    private static final String JOB_DIR = TMP_ROOT + "/job-1";
    private static final String UUID_DIR = JOB_DIR + "/uuid-1";
    private static final String TRANSACTION_DIR = UUID_DIR + "/T_job-1_uuid-1_0_1";
    private static final String TEMP_FILE = TRANSACTION_DIR + "/NON_PARTITION/out.txt";
    private static final String TARGET_FILE = "/warehouse/table/out.txt";

    private static class TestableCommitter extends FileSinkAggregatedCommitter {
        TestableCommitter() {
            super(new HadoopConf("hdfs://dummy"));
        }

        void setFileSystemProxy(HadoopFileSystemProxy proxy) {
            this.hadoopFileSystemProxy = proxy;
        }
    }

    @Test
    void shouldDeferEmptyTransactionParentDirectoryCleanupUntilClose() throws Exception {
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        TestableCommitter committer = newCommitter(fs);

        List<FileAggregatedCommitInfo> errors =
                committer.commit(Collections.singletonList(newCommitInfo(true)));

        Assertions.assertTrue(errors.isEmpty());
        Mockito.verify(fs).renameFile(TEMP_FILE, TARGET_FILE, true);
        Mockito.verify(fs).deleteFile(TRANSACTION_DIR);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(Mockito.anyString());

        committer.close();

        Mockito.verify(fs).deleteEmptyDirectory(UUID_DIR);
        Mockito.verify(fs).deleteEmptyDirectory(JOB_DIR);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(TMP_ROOT);
        Mockito.verify(fs).close();
    }

    @Test
    void shouldCleanEmptyTransactionParentDirectoriesAfterAbort() throws Exception {
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        TestableCommitter committer = newCommitter(fs);

        committer.abort(Collections.singletonList(newCommitInfo(false)));

        Mockito.verify(fs).deleteFile(TRANSACTION_DIR);
        Mockito.verify(fs).deleteEmptyDirectory(UUID_DIR);
        Mockito.verify(fs).deleteEmptyDirectory(JOB_DIR);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(TMP_ROOT);
    }

    @Test
    void shouldIgnoreTransactionDirectoryCleanupFailureAfterCommit() throws Exception {
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.doThrow(new java.io.IOException("cleanup failed"))
                .when(fs)
                .deleteFile(TRANSACTION_DIR);
        TestableCommitter committer = newCommitter(fs);

        List<FileAggregatedCommitInfo> errors =
                committer.commit(Collections.singletonList(newCommitInfo(true)));

        Assertions.assertTrue(errors.isEmpty());
        Mockito.verify(fs).renameFile(TEMP_FILE, TARGET_FILE, true);
        Mockito.verify(fs).deleteFile(TRANSACTION_DIR);
    }

    private static TestableCommitter newCommitter(HadoopFileSystemProxy fs) {
        TestableCommitter committer = new TestableCommitter();
        committer.setFileSystemProxy(fs);
        return committer;
    }

    private static FileAggregatedCommitInfo newCommitInfo(boolean withFileMove) {
        LinkedHashMap<String, String> fileMoves = new LinkedHashMap<>();
        if (withFileMove) {
            fileMoves.put(TEMP_FILE, TARGET_FILE);
        }
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap = new LinkedHashMap<>();
        transactionMap.put(TRANSACTION_DIR, fileMoves);
        return new FileAggregatedCommitInfo(transactionMap, new LinkedHashMap<>());
    }
}
