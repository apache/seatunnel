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

import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Transaction extends Serializable {
    /**
     * prepare commit operation
     * @return the file commit information
     */
    Optional<FileCommitInfo> prepareCommit();

    /**
     * abort prepare commit operation
     */
    void abortPrepare();

    /**
     * abort prepare commit operation using transaction id
     * @param transactionId transaction id
     */
    void abortPrepare(String transactionId);

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     * @param checkpointId checkpointId
     * @return the list of states
     */
    List<FileSinkState> snapshotState(long checkpointId);

    /**
     * get dirty transaction ids from file sink states
     * @param fileSinkState file sink states
     * @return transaction ids
     */
    List<String> getTransactionIdFromStates(List<FileSinkState> fileSinkState);

    /**
     * when a checkpoint triggered, file sink should begin a new transaction
     * @param checkpointId checkpoint id
     */
    void beginTransaction(Long checkpointId);
}
