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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPartition;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.shade.org.apache.thrift.transport.TTransportException;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.CLOSE_BE_READER_FAILED;

@Slf4j
public class StarRocksBeReadClient implements Serializable {
    private static final String DEFAULT_CLUSTER_NAME = "default_cluster";

    private TStarrocksExternalService.Client client;
    private final String ip;
    private final int port;
    private String contextId;
    private int readerOffset = 0;
    private final SourceConfig sourceConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private StarRocksRowBatchReader rowBatch;
    protected AtomicBoolean eos = new AtomicBoolean(false);

    public StarRocksBeReadClient(String beNodeInfo, SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        log.debug("Parse StarRocks BE address: '{}'.", beNodeInfo);
        String[] hostPort = beNodeInfo.split(":");
        if (hostPort.length != 2) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.CREATE_BE_READER_FAILED,
                    String.format("Format of StarRocks BE address[%s] is illegal", beNodeInfo));
        }
        this.ip = hostPort[0].trim();
        this.port = Integer.parseInt(hostPort[1].trim());
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket =
                new TSocket(
                        ip,
                        port,
                        sourceConfig.getConnectTimeoutMs(),
                        sourceConfig.getConnectTimeoutMs());
        try {
            socket.open();
        } catch (TTransportException e) {
            socket.close();
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.CREATE_BE_READER_FAILED,
                    "Failed to open socket",
                    e);
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);
    }

    public void openScanner(QueryPartition partition, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        Set<Long> tabletIds = partition.getTabletIds();
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(new ArrayList<>(tabletIds));
        params.setOpaqued_query_plan(partition.getQueryPlan());
        params.setCluster(DEFAULT_CLUSTER_NAME);
        params.setDatabase(sourceConfig.getDatabase());
        params.setTable(sourceConfig.getTable());
        params.setUser(sourceConfig.getUsername());
        params.setPasswd(sourceConfig.getPassword());
        params.setBatch_size(sourceConfig.getBatchRows());
        if (sourceConfig.getSourceOptionProps() != null) {
            params.setProperties(sourceConfig.getSourceOptionProps());
        }
        short keepAliveMin = (short) Math.min(Short.MAX_VALUE, sourceConfig.getKeepAliveMin());
        params.setKeep_alive_min(keepAliveMin);
        params.setQuery_timeout(sourceConfig.getQueryTimeoutSec());
        params.setMem_limit(sourceConfig.getMemLimit());
        log.info("open Scan params.mem_limit {} B", params.getMem_limit());
        log.info("open Scan params.keep-alive-min {} min", params.getKeep_alive_min());
        log.info("open Scan params.batch_size {}", params.getBatch_size());
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED,
                        "Failed to open scanner."
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs());
            }
        } catch (TException e) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED, e.getMessage());
        }
        this.contextId = result.getContext_id();
        log.info(
                "Open scanner for {}:{} with context id {}, and there are {} tablets {}",
                ip,
                port,
                contextId,
                tabletIds.size(),
                tabletIds);
    }

    public boolean hasNext() {
        boolean hasNext = false;
        // Arrow data was acquired synchronously during the iterative process
        if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
            if (rowBatch != null) {
                readerOffset += rowBatch.getReadRowCount();
                rowBatch.close();
            }
            TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
            nextBatchParams.setContext_id(contextId);
            nextBatchParams.setOffset(readerOffset);
            TScanBatchResult result;
            try {
                result = client.get_next(nextBatchParams);
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED,
                            "Failed to get next from be -> ip:["
                                    + ip
                                    + "] "
                                    + result.getStatus().getStatus_code()
                                    + " msg:"
                                    + result.getStatus().getError_msgs());
                }
                eos.set(result.isEos());
                if (!eos.get()) {
                    rowBatch = new StarRocksRowBatchReader(result, seaTunnelRowType).readArrow();
                }
            } catch (TException e) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED, e.getMessage());
            }
        }
        hasNext = !eos.get();
        return hasNext;
    }

    public SeaTunnelRow getNext() {
        return rowBatch.next();
    }

    public void close() {
        log.info("Close reader for {}:{} with context id {}", ip, port, contextId);
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        } catch (TException e) {
            log.error("Failed to close reader {}:{} with context id {}", ip, port, contextId, e);
            throw new StarRocksConnectorException(CLOSE_BE_READER_FAILED, e);
        }
    }
}
