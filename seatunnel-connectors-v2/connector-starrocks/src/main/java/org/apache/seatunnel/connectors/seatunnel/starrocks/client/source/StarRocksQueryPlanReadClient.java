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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpHelper;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPartition;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.model.QueryPlan;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class StarRocksQueryPlanReadClient {
    private final RetryUtils.RetryMaterial retryMaterial;
    private final SourceConfig sourceConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final HttpHelper httpHelper = new HttpHelper();

    private static final long DEFAULT_SLEEP_TIME_MS = 1000L;

    public StarRocksQueryPlanReadClient(
            SourceConfig sourceConfig, SeaTunnelRowType seaTunnelRowType) {
        this.sourceConfig = sourceConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.retryMaterial =
                new RetryUtils.RetryMaterial(
                        sourceConfig.getMaxRetries(),
                        true,
                        exception -> true,
                        DEFAULT_SLEEP_TIME_MS);
    }

    public List<QueryPartition> findPartitions() {
        List<String> nodeUrls = sourceConfig.getNodeUrls();
        QueryPlan queryPlan = getQueryPlan(genQuerySql(), nodeUrls);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan);
        return tabletsMapToPartition(
                be2Tablets,
                queryPlan.getQueryPlan(),
                sourceConfig.getDatabase(),
                sourceConfig.getTable());
    }

    private List<QueryPartition> tabletsMapToPartition(
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table)
            throws IllegalArgumentException {
        int tabletsSize = sourceConfig.getRequestTabletSize();
        List<QueryPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            log.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets =
                        new HashSet<>(
                                beInfo.getValue()
                                        .subList(
                                                first,
                                                Math.min(
                                                        beInfo.getValue().size(),
                                                        first + tabletsSize)));
                first = first + tabletsSize;
                QueryPartition partitionDefinition =
                        new QueryPartition(
                                database,
                                table,
                                beInfo.getKey(),
                                partitionTablets,
                                opaquedQueryPlan);
                log.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }

    private Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan) {
        Map<String, List<Long>> beXTablets = new HashMap<>();
        queryPlan
                .getPartitions()
                .forEach(
                        (tabletId, routingList) -> {
                            int tabletCount = Integer.MAX_VALUE;
                            String candidateBe = "";
                            for (String beNode : routingList.getRoutings()) {
                                if (!beXTablets.containsKey(beNode)) {
                                    beXTablets.put(beNode, new ArrayList<>());
                                    candidateBe = beNode;
                                    break;
                                }
                                if (beXTablets.get(beNode).size() < tabletCount) {
                                    candidateBe = beNode;
                                    tabletCount = beXTablets.get(beNode).size();
                                }
                            }
                            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
                        });
        return beXTablets;
    }

    private QueryPlan getQueryPlan(String querySQL, List<String> nodeUrls) {

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = JsonUtils.toJsonString(bodyMap);
        String respString;
        for (String feNode : nodeUrls) {
            String url =
                    "http://"
                            + feNode
                            + "/api/"
                            + sourceConfig.getDatabase()
                            + "/"
                            + sourceConfig.getTable()
                            + "/_query_plan";
            try {
                respString =
                        RetryUtils.retryWithException(
                                () -> httpHelper.doHttpPost(url, getQueryPlanHttpHeader(), body),
                                retryMaterial);
                if (StringUtils.isNoneEmpty(respString)) {
                    return JsonUtils.parseObject(respString, QueryPlan.class);
                }
            } catch (Exception e) {
                log.error("Request query Plan From {} failed: {}", feNode, e.getMessage());
            }
        }

        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.QUEST_QUERY_PLAN_FAILED,
                "query failed with empty response");
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    private Map<String, String> getQueryPlanHttpHeader() {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Content-Type", "application/json;charset=UTF-8");
        headerMap.put(
                "Authorization",
                getBasicAuthHeader(sourceConfig.getUsername(), sourceConfig.getPassword()));
        return headerMap;
    }

    private String genQuerySql() {
        String columns =
                seaTunnelRowType.getFieldNames().length != 0
                        ? String.join(",", seaTunnelRowType.getFieldNames())
                        : "*";
        String filter =
                sourceConfig.getScanFilter().isEmpty()
                        ? ""
                        : " where " + sourceConfig.getScanFilter();

        String sql =
                "select "
                        + columns
                        + " from "
                        + "`"
                        + sourceConfig.getDatabase()
                        + "`"
                        + "."
                        + "`"
                        + sourceConfig.getTable()
                        + "`"
                        + filter;
        log.debug("Generate query sql '{}'.", sql);
        return sql;
    }
}
