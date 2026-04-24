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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

class AbstractWriteStrategyTest {

    private static final String TMP_PATH = "/tmp/seatunnel";
    private static final String JOB_ID = "job-1";
    private static final String UUID_PREFIX = "uuid-1";
    private static final String TRANSACTION_ID = "T_job-1_uuid-1_0_1";

    @Test
    void shouldCleanEmptyTransactionParentDirectoriesAfterAbortPrepare() throws Exception {
        TransactionPath transactionPath = newTransactionPath();
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        TestWriteStrategy writeStrategy = newTestWriteStrategy(fs);

        writeStrategy.abortPrepare(TRANSACTION_ID);

        Mockito.verify(fs).deleteFile(transactionPath.transactionDir);
        Mockito.verify(fs).deleteEmptyDirectory(transactionPath.uuidDir);
        Mockito.verify(fs).deleteEmptyDirectory(transactionPath.jobDir);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(TMP_PATH);
    }

    @Test
    void shouldHandleAbortPrepareCleanupFailures() throws Exception {
        TransactionPath transactionPath = newTransactionPath();
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.doThrow(new IOException("cleanup failed"))
                .when(fs)
                .deleteEmptyDirectory(transactionPath.uuidDir);
        TestWriteStrategy cleanupFailureStrategy = newTestWriteStrategy(fs);

        Assertions.assertDoesNotThrow(() -> cleanupFailureStrategy.abortPrepare(TRANSACTION_ID));

        Mockito.verify(fs).deleteFile(transactionPath.transactionDir);
        Mockito.verify(fs).deleteEmptyDirectory(transactionPath.uuidDir);
        Mockito.verify(fs).deleteEmptyDirectory(transactionPath.jobDir);

        fs = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.doThrow(new IOException("delete failed"))
                .when(fs)
                .deleteFile(transactionPath.transactionDir);
        TestWriteStrategy deleteFailureStrategy = newTestWriteStrategy(fs);

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> deleteFailureStrategy.abortPrepare(TRANSACTION_ID));

        Mockito.verify(fs).deleteFile(transactionPath.transactionDir);
        Mockito.verify(fs, Mockito.never()).deleteEmptyDirectory(Mockito.anyString());
    }

    private static TestWriteStrategy newTestWriteStrategy(HadoopFileSystemProxy fs) {
        TestWriteStrategy writeStrategy = new TestWriteStrategy(newFileSinkConfig());
        writeStrategy.setFileSystemProxy(fs);
        writeStrategy.setTransactionContext(JOB_ID, UUID_PREFIX);
        return writeStrategy;
    }

    private static FileSinkConfig newFileSinkConfig() {
        FileSinkConfig fileSinkConfig = Mockito.mock(FileSinkConfig.class);
        Mockito.when(fileSinkConfig.getSinkColumnsIndexInRow()).thenReturn(Collections.emptyList());
        Mockito.when(fileSinkConfig.getBatchSize()).thenReturn(1000);
        Mockito.when(fileSinkConfig.getCompressFormat()).thenReturn(CompressFormat.NONE);
        Mockito.when(fileSinkConfig.isSingleFileMode()).thenReturn(false);
        Mockito.when(fileSinkConfig.getTmpPath()).thenReturn(TMP_PATH);
        return fileSinkConfig;
    }

    private static TransactionPath newTransactionPath() {
        String transactionDir =
                String.join(
                        File.separator,
                        TMP_PATH,
                        FileBaseSinkOptions.SEATUNNEL,
                        JOB_ID,
                        UUID_PREFIX,
                        TRANSACTION_ID);
        String uuidDir = new Path(transactionDir).getParent().toString();
        String jobDir = new Path(uuidDir).getParent().toString();
        return new TransactionPath(transactionDir, uuidDir, jobDir);
    }

    private static class TransactionPath {
        private final String transactionDir;
        private final String uuidDir;
        private final String jobDir;

        private TransactionPath(String transactionDir, String uuidDir, String jobDir) {
            this.transactionDir = transactionDir;
            this.uuidDir = uuidDir;
            this.jobDir = jobDir;
        }
    }

    private static class TestWriteStrategy extends AbstractWriteStrategy<Object> {

        private TestWriteStrategy(FileSinkConfig fileSinkConfig) {
            super(fileSinkConfig);
        }

        private void setFileSystemProxy(HadoopFileSystemProxy proxy) {
            this.hadoopFileSystemProxy = proxy;
        }

        private void setTransactionContext(String jobId, String uuidPrefix) {
            this.jobId = jobId;
            this.uuidPrefix = uuidPrefix;
        }

        @Override
        public Object getOrCreateOutputStream(String path) {
            return new Object();
        }

        @Override
        public void finishAndCloseFile() {}
    }
}
