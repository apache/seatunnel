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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.serialize.KuduRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kudu.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** A Kudu outputFormat */
@Slf4j
public class KuduOutputFormat implements Serializable {

    private final String kuduTableName;
    private final KuduSinkConfig.SaveMode saveMode;
    private final KuduSinkConfig kuduSinkConfig;
    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduTable kuduTable;

    private SeaTunnelRowSerializer seaTunnelRowSerializer;

    private SeaTunnelRowType seaTunnelRowType;

    private transient AtomicInteger numPendingRequests;

    public KuduOutputFormat(
            @NonNull KuduSinkConfig kuduSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        this.kuduTableName = kuduSinkConfig.getTable();
        this.saveMode = kuduSinkConfig.getSaveMode();
        this.kuduSinkConfig = kuduSinkConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.numPendingRequests = new AtomicInteger(0);
        openOutputFormat();
    }

    private void openOutputFormat() {
        this.kuduClient = KuduUtil.getKuduClient(kuduSinkConfig);
        this.kuduSession = getSession();
        try {
            kuduTable = kuduClient.openTable(kuduTableName);
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
        }
        log.info(
                "The Kudu client for Master: {} is initialized successfully.",
                kuduSinkConfig.getMasters());

        seaTunnelRowSerializer = new KuduRowSerializer(kuduTable, saveMode, seaTunnelRowType);
    }

    private KuduSession getSession() {
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(kuduSinkConfig.getOperationTimeout());
        session.setFlushMode(kuduSinkConfig.getFlushMode());
        session.setFlushInterval(kuduSinkConfig.getFlushInterval());
        session.setMutationBufferSpace(kuduSinkConfig.getMaxBufferSize());
        session.setIgnoreAllNotFoundRows(kuduSinkConfig.isIgnoreNotFound());
        session.setIgnoreAllDuplicateRows(kuduSinkConfig.isIgnoreDuplicate());
        return session;
    }

    public void closeOutputFormat() throws IOException {
        try {
            flush();
        } finally {
            try {
                if (kuduSession != null) {
                    kuduSession.close();
                }
            } catch (Exception e) {
                log.error("Error while closing session.", e);
            }
            try {
                if (kuduClient != null) {
                    kuduClient.close();
                }
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }
    }

    public void flush() throws KuduException {
        kuduSession.flush();
        checkAsyncErrors();
    }

    private void checkAsyncErrors() {
        if (kuduSession.countPendingErrors() == 0) {
            return;
        }
        String errorMessage =
                Arrays.stream(kuduSession.getPendingErrors().getRowErrors())
                        .map(error -> error.toString() + System.lineSeparator())
                        .collect(Collectors.joining());
        throw new KuduConnectorException(KuduConnectorErrorCode.WRITE_DATA_FAILED, errorMessage);
    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            throw new KuduConnectorException(
                    KuduConnectorErrorCode.WRITE_DATA_FAILED, response.getRowError().toString());
        }
    }

    public void write(SeaTunnelRow row) throws IOException {
        checkAsyncErrors();
        if (row.getRowKind() == RowKind.UPDATE_BEFORE) return;
        Operation operation = seaTunnelRowSerializer.serializeRow(row);
        checkErrors(kuduSession.apply(operation));
        if (kuduSinkConfig.getMaxBufferSize() > 0
                && numPendingRequests.incrementAndGet() >= kuduSinkConfig.getMaxBufferSize()) {
            flush();
        }
    }
}
