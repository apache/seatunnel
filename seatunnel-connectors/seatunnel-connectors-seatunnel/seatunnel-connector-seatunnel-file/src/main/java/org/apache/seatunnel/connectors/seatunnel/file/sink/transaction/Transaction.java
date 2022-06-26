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

package org.apache.seatunnel.connectors.seatunnel.file.sink.transaction;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.AbstractFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.TransactionStateFileSinkWriter;

import lombok.NonNull;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Transaction extends Serializable {
    /**
     * A new transaction needs to be started after each checkpoint is completed.
     *
     * @param checkpointId A checkpoint indicates that all tasks have a status snapshot operation
     * @return transactionId
     */
    String beginTransaction(@NonNull Long checkpointId);

    /**
     * Abort current Transaction, called when {@link TransactionStateFileSinkWriter#prepareCommit()} or {@link TransactionStateFileSinkWriter#snapshotState(long)} failed
     */
    void abortTransaction();

    /**
     * Get all transactionIds after the @param transactionId
     * This method called when {@link AbstractFileSink#restoreWriter(SinkWriter.Context, List)}
     * We get the transactionId of the last successful commit from {@link FileSinkState} and
     * then all transactionIds after this transactionId is dirty transactions that need to be rollback.
     *
     * @param transactionId The transactionId of the last successful commit get from {@link FileSinkState}
     * @return transactionId list
     */
    List<String> getTransactionAfter(@NonNull String transactionId);

    /**
     * Called by {@link TransactionStateFileSinkWriter#prepareCommit()}
     * We should end the transaction in this method. After this method is called, the transaction will no longer accept data writing
     *
     * @return Return the commit information that can be commit in {@link FileSinkAggregatedCommitter#commit(List)}
     */
    Optional<FileCommitInfo> prepareCommit();

    /**
     * rollback the transaction which is not be commit
     *
     * @param transactionIds transactionIds
     */
    void abortTransactions(List<String> transactionIds);
}
