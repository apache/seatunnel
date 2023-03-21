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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSemantics;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.IOException;
import java.util.List;

public class PulsarSinkCommitter implements SinkCommitter<PulsarCommitInfo> {

    private PulsarClientConfig clientConfig;
    private PulsarClient pulsarClient;
    private TransactionCoordinatorClient coordinatorClient;

    public PulsarSinkCommitter(PulsarClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public List<PulsarCommitInfo> commit(List<PulsarCommitInfo> commitInfos) throws IOException {
        if (commitInfos.isEmpty()) {
            return commitInfos;
        }

        TransactionCoordinatorClient client = transactionCoordinatorClient();

        for (PulsarCommitInfo pulsarCommitInfo : commitInfos) {
            TxnID txnID = pulsarCommitInfo.getTxnID();
            client.commit(txnID);
        }
        return commitInfos;
    }

    @Override
    public void abort(List<PulsarCommitInfo> commitInfos) throws IOException {
        if (commitInfos.isEmpty()) {
            return;
        }
        TransactionCoordinatorClient client = transactionCoordinatorClient();
        for (PulsarCommitInfo commitInfo : commitInfos) {
            TxnID txnID = commitInfo.getTxnID();
            client.abort(txnID);
        }
        if (this.pulsarClient != null) {
            pulsarClient.close();
        }
    }

    private TransactionCoordinatorClient transactionCoordinatorClient()
            throws PulsarClientException {
        if (coordinatorClient == null) {
            this.pulsarClient =
                    PulsarConfigUtil.createClient(clientConfig, PulsarSemantics.EXACTLY_ONCE);
            this.coordinatorClient = PulsarConfigUtil.getTcClient(pulsarClient);
        }
        return coordinatorClient;
    }
}
