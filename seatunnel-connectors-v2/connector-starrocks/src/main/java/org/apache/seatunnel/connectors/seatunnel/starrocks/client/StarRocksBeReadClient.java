/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.CLOSE_READER_FAILED;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.source.model.QueryPartition;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class StarRocksBeReadClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksBeReadClient.class);
    private static final String DEFAULT_CLUSTER_NAME = "default_cluster";

    private TStarrocksExternalService.Client client;
    private final String ip;
    private final int port;
    private String contextId;
    private int readerOffset = 0;
    private final SourceConfig sourceConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private SeaTunnelRowBatch rowBatch;

    private final List<Long> tabletIds;

    private final String queryPlan;
    protected AtomicBoolean eos = new AtomicBoolean(false);

    public StarRocksBeReadClient(QueryPartition queryPartition,
                                 SourceConfig sourceConfig,
                                 SeaTunnelRowType seaTunnelRowType) {
        this.sourceConfig = sourceConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        String beNodeInfo = queryPartition.getBeAddress();
        log.debug("Parse StarRocks BE address: '{}'.", beNodeInfo);
        String[] hostPort = beNodeInfo.split(":");
        if (hostPort.length != 2) {
            log.error("Format of StarRocks BE address '{}' is illegal.", queryPartition.getBeAddress());
            throw new RuntimeException("Format of StarRocks BE address fail");
        }
        this.ip = hostPort[0].trim();
        this.port = Integer.parseInt(hostPort[1].trim());
        this.queryPlan = queryPartition.getQueryPlan();
        this.tabletIds = new ArrayList<>(queryPartition.getTabletIds());
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket = new TSocket(ip, port, sourceConfig.getConnectTimeoutMs(), sourceConfig.getConnectTimeoutMs());
        try {
            socket.open();
        } catch (TTransportException e) {
            socket.close();
            throw new RuntimeException("Failed to create brpc source:" + e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);

    }

    public void openScanner() {
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(tabletIds);
        params.setOpaqued_query_plan(queryPlan);
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
        LOG.info("open Scan params.mem_limit {} B", params.getMem_limit());
        LOG.info("open Scan params.keep-alive-min {} min", params.getKeep_alive_min());
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new RuntimeException(
                        "Failed to open scanner."
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs()
                );
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to open scanner." + e.getMessage());
        }
        this.contextId = result.getContext_id();
        LOG.info("Open scanner for {}:{} with context id {}, and there are {} tablets {}",
                ip, port, contextId, tabletIds.size(), tabletIds);
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
                    throw new RuntimeException(
                            "Failed to get next from be -> ip:[" + ip + "] "
                                    + result.getStatus().getStatus_code() + " msg:" + result.getStatus().getError_msgs()
                    );
                }
                eos.set(result.isEos());
                if (!eos.get()) {
                    log.info("get result rows {}", result.getRows().length);
                    rowBatch = new SeaTunnelRowBatch(result, seaTunnelRowType).readArrow();
                }
            } catch (TException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        hasNext = !eos.get();
        return hasNext;
    }

    public SeaTunnelRow getNext() {
        return rowBatch.next();
    }

    public void close() {
        LOG.info("Close reader for {}:{} with context id {}", ip, port, contextId);
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        } catch (TException e) {
            LOG.error("Failed to close reader {}:{} with context id {}", ip, port, contextId, e);
            throw new StarRocksConnectorException(CLOSE_READER_FAILED, e);
        }
    }
}
