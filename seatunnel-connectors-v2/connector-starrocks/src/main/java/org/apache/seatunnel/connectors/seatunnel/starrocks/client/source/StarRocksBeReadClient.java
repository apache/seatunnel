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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader.ArrowToSeatunnelRowReader;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPartition;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.BeHostPortMapping;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
    private ArrowToSeatunnelRowReader rowBatch;
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
        String normalizedBeNodeInfo =
                hostPort[0].trim() + ":" + Integer.parseInt(hostPort[1].trim());

        // If the user has configured beHostPortMapping, we need to parse it
        Map<String, Pair<String, Integer>> beHostPortMapping =
                formatBeHostPortMapping(sourceConfig);

        if (beHostPortMapping.containsKey(normalizedBeNodeInfo)) {
            Pair<String, Integer> accessIpPort = beHostPortMapping.get(normalizedBeNodeInfo);
            this.ip = accessIpPort.getKey();
            this.port = accessIpPort.getValue();
            log.debug(
                    "The be host and be_port is configured by user with config 'be_host_port_mapping', the be host is '{}', the be port is '{}'.",
                    accessIpPort.getKey(),
                    accessIpPort.getValue());
        } else {
            this.ip = hostPort[0].trim();
            this.port = Integer.parseInt(hostPort[1].trim());
        }

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

    /**
     * If the user has configured beHostPortMapping, we need to parse it
     *
     * @param sourceConfig sourceConfig
     * @return <host, Pair<ip, port>>
     */
    private static Map<String, Pair<String, Integer>> formatBeHostPortMapping(
            SourceConfig sourceConfig) {
        return sourceConfig.getBeHostPortMapping().stream()
                .collect(
                        Collectors.toMap(
                                StarRocksBeReadClient::extractHostPort,
                                StarRocksBeReadClient::parseAccessiblePort,
                                (existing, duplicate) -> {
                                    log.warn(
                                            "Duplicate host mapping found: '{}'. Using first mapping: '{}', ignoring: '{}'",
                                            existing.getKey(),
                                            existing,
                                            duplicate);
                                    return existing;
                                }));
    }

    /**
     * Validate and normalize the host:port mapping key.
     *
     * @throws StarRocksConnectorException if format is invalid
     */
    private static String extractHostPort(BeHostPortMapping mapping) {
        // host:be_port
        String hostPort = mapping.getHostPort();
        if (StringUtils.isBlank(hostPort)) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL, "host_port cannot be blank");
        }

        String[] parts = hostPort.split(":", -1);
        if (parts.length != 2) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                    String.format(
                            "Invalid host_port format: '%s'. Expected 'host:port'", hostPort));
        }

        String host = parts[0].trim();
        if (StringUtils.isBlank(host)) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                    String.format("Host cannot be empty in host_port: '%s'", hostPort));
        }

        return host + ":" + parseMappingPort(parts[1], "host_port", hostPort);
    }

    /**
     * Validate and parse accessible ip:port to Pair.
     *
     * @throws StarRocksConnectorException if format is invalid
     */
    private static Pair<String, Integer> parseAccessiblePort(BeHostPortMapping mapping) {
        String actualValue = mapping.getIpPort();
        // accessible ip and be_port
        String[] accessIpInfo;
        if (StringUtils.isBlank(actualValue)
                || (accessIpInfo = actualValue.split(":", -1)).length != 2) {
            log.error(
                    "Invalid ip_port configuration: '{}'. Expected format 'ip:port'", actualValue);
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                    String.format(
                            "Invalid ip_port configuration: '%s'. Expected format 'ip:port'",
                            actualValue));
        }
        String host = accessIpInfo[0].trim();
        if (StringUtils.isBlank(host)) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                    String.format("Host cannot be empty in ip_port: '%s'", actualValue));
        }
        return Pair.of(host, parseMappingPort(accessIpInfo[1], "ip_port", actualValue));
    }

    private static int parseMappingPort(String portValue, String optionName, String actualValue) {
        try {
            int port = Integer.parseInt(portValue.trim());
            if (port <= 0 || port > 65535) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                        String.format(
                                "Invalid port number: %s in %s '%s'. Port must be between 1 and 65535",
                                port, optionName, actualValue));
            }
            return port;
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_MAPPING_ILLEGAL,
                    String.format(
                            "The port '%s' in %s '%s' is not a valid number",
                            portValue, optionName, actualValue),
                    e);
        }
    }

    public void openScanner(QueryPartition partition, SeaTunnelRowType seaTunnelRowType) {
        Set<Long> tabletIds = partition.getTabletIds();
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(new ArrayList<>(tabletIds));
        params.setOpaqued_query_plan(partition.getQueryPlan());
        params.setCluster(DEFAULT_CLUSTER_NAME);
        params.setDatabase(sourceConfig.getDatabase());
        params.setTable(partition.getTable());
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
        this.eos.set(false);
        this.rowBatch = null;
        this.readerOffset = 0;
        this.seaTunnelRowType = seaTunnelRowType;
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

                    rowBatch =
                            new ArrowToSeatunnelRowReader(result.getRows(), seaTunnelRowType)
                                    .readArrow();
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
        if (contextId == null) {
            // not opened yet
            return;
        }
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        for (int i = 0; i < 3; i++) {
            try {
                this.client.close_scanner(tScanCloseParams);
                break;
            } catch (Exception e) {
                log.error(
                        "Failed to close reader {}:{} with context id {}", ip, port, contextId, e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error(
                        "Waiting for closing is interrupted, reader {}:{} with context id {}",
                        ip,
                        port,
                        contextId);
            }
        }
    }
}
