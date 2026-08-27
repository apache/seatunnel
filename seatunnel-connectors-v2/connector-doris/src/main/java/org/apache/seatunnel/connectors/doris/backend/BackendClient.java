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

package org.apache.seatunnel.connectors.doris.backend;

import org.apache.seatunnel.shade.org.apache.thrift.TConfiguration;
import org.apache.seatunnel.shade.org.apache.thrift.TException;
import org.apache.seatunnel.shade.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.seatunnel.shade.org.apache.thrift.protocol.TProtocol;
import org.apache.seatunnel.shade.org.apache.thrift.transport.TSocket;
import org.apache.seatunnel.shade.org.apache.thrift.transport.TTransport;
import org.apache.seatunnel.shade.org.apache.thrift.transport.TTransportException;

import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.source.serialization.Routing;
import org.apache.seatunnel.connectors.doris.util.ErrorMessages;

import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatusCode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendClient {

    private Routing routing;

    private TDorisExternalService.Client client;
    private TTransport transport;

    private boolean isConnected = false;
    private final int retries;
    private final int socketTimeout;
    private final int connectTimeout;

    public BackendClient(Routing routing, DorisSourceConfig readOptions) {
        this.routing = routing;
        this.connectTimeout = readOptions.getRequestConnectTimeoutMs();
        this.socketTimeout = readOptions.getRequestReadTimeoutMs();
        this.retries = readOptions.getRequestRetries();
        log.trace(
                "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                this.connectTimeout,
                this.socketTimeout,
                this.retries);
        open();
    }

    private void open() {
        log.debug("Open client to Doris BE '{}'.", routing);
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            log.debug("Attempt {} to connect {}.", attempt, routing);
            try {
                TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
                transport =
                        new TSocket(
                                new TConfiguration(),
                                routing.getHost(),
                                routing.getPort(),
                                socketTimeout,
                                connectTimeout);
                TProtocol protocol = factory.getProtocol(transport);
                client = new TDorisExternalService.Client(protocol);
                log.trace(
                        "Connect status before open transport to {} is '{}'.",
                        routing,
                        isConnected);
                if (!transport.isOpen()) {
                    transport.open();
                    isConnected = true;
                    log.info("Success connect to {}.", routing);
                    break;
                }
            } catch (TTransportException e) {
                log.warn(ErrorMessages.CONNECT_FAILED_MESSAGE, routing, e);
                ex = e;
            }
        }
        if (!isConnected) {
            log.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
            //            throw new ConnectedFailedException(routing.toString(), ex);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.BACKEND_CLIENT_FAILED, routing.toString(), ex);
        }
    }

    private void close() {
        log.trace("Connect status before close with '{}' is '{}'.", routing, isConnected);
        isConnected = false;
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            log.info("Closed a connection to {}.", routing);
        }
        if (null != client) {
            client = null;
        }
    }

    /**
     * Open a scanner for reading Doris data.
     *
     * @param openParams thrift struct to required by request
     * @return scan open result
     * @throws DorisConnectorException throw if cannot connect to Doris BE
     */
    public TScanOpenResult openScanner(TScanOpenParams openParams) {
        log.debug("OpenScanner to '{}', parameter is '{}'.", routing, openParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            log.debug("Attempt {} to openScanner {}.", attempt, routing);
            try {
                TScanOpenResult result = client.openScanner(openParams);
                if (result == null) {
                    log.warn("Open scanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    log.warn(
                            "The status of open scanner result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                log.warn("Open scanner from {} failed.", routing, e);
                ex = e;
            }
        }
        log.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
        //        throw new ConnectedFailedException(routing.toString(), ex);
        throw new DorisConnectorException(
                DorisConnectorErrorCode.SCAN_BATCH_FAILED, routing.toString(), ex);
    }

    /**
     * get next row batch from Doris BE
     *
     * @param nextBatchParams thrift struct to required by request
     * @return scan batch result
     * @throws DorisConnectorException throw if cannot connect to Doris BE
     */
    public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) {
        log.debug("GetNext to '{}', parameter is '{}'.", routing, nextBatchParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        TScanBatchResult result = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            log.debug("Attempt {} to getNext {}.", attempt, routing);
            try {
                result = client.getNext(nextBatchParams);
                if (result == null) {
                    log.warn("GetNext result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    log.warn(
                            "The status of get next result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                log.warn("Get next from {} failed.", routing, e);
                ex = e;
            }
        }
        if (result != null && (TStatusCode.OK != (result.getStatus().getStatusCode()))) {
            log.error(
                    ErrorMessages.DORIS_INTERNAL_FAIL_MESSAGE,
                    routing,
                    result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
            //            throw new DorisInternalException(routing.toString(),
            // result.getStatus().getStatusCode(),
            //                    result.getStatus().getErrorMsgs());
            String errMsg =
                    "Doris server "
                            + routing.toString()
                            + " internal failed, status code ["
                            + result.getStatus().getStatusCode()
                            + "] error message is "
                            + result.getStatus().getErrorMsgs();
            throw new DorisConnectorException(DorisConnectorErrorCode.SCAN_BATCH_FAILED, errMsg);
        }
        log.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
        //        throw new ConnectedFailedException(routing.toString(), ex);
        throw new DorisConnectorException(
                DorisConnectorErrorCode.SCAN_BATCH_FAILED, routing.toString(), ex);
    }

    /**
     * close a scanner.
     *
     * @param closeParams thrift struct to required by request
     */
    public void closeScanner(TScanCloseParams closeParams) {
        log.debug("CloseScanner to '{}', parameter is '{}'.", routing, closeParams);
        for (int attempt = 0; attempt < retries; ++attempt) {
            log.debug("Attempt {} to closeScanner {}.", attempt, routing);
            try {
                TScanCloseResult result = client.closeScanner(closeParams);
                if (result == null) {
                    log.warn("CloseScanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    log.warn(
                            "The status of get next result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                break;
            } catch (TException e) {
                log.warn("Close scanner from {} failed.", routing, e);
            }
        }
        log.info("CloseScanner to Doris BE '{}' success.", routing);
        close();
    }
}
