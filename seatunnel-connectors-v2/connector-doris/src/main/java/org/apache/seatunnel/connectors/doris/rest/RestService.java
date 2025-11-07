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

package org.apache.seatunnel.connectors.doris.rest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParseException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.config.DorisSourceOptions;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.models.QueryPlan;
import org.apache.seatunnel.connectors.doris.rest.models.Tablet;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.util.ErrorMessages;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RestService implements Serializable {
    public static final int REST_RESPONSE_STATUS_OK = 200;
    private static final String API_PREFIX = "/api";
    private static final String QUERY_PLAN = "_query_plan";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static String send(
            DorisSourceConfig dorisSourceConfig, HttpRequestBase request, Logger logger)
            throws DorisConnectorException {
        int connectTimeout = dorisSourceConfig.getRequestConnectTimeoutMs();
        int socketTimeout = dorisSourceConfig.getRequestReadTimeoutMs();
        int retries = dorisSourceConfig.getRequestRetries();
        logger.trace(
                "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout,
                socketTimeout,
                retries);

        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout)
                        .build();

        request.setConfig(requestConfig);
        logger.info(
                "Send request to Doris FE '{}' with user '{}'.",
                request.getURI(),
                dorisSourceConfig.getUsername());
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            logger.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                String response;
                if (request instanceof HttpGet) {
                    response =
                            getConnectionGet(
                                    request.getURI().toString(),
                                    dorisSourceConfig.getUsername(),
                                    dorisSourceConfig.getPassword(),
                                    logger);
                } else {
                    response =
                            getConnectionPost(
                                    request,
                                    dorisSourceConfig.getUsername(),
                                    dorisSourceConfig.getPassword(),
                                    logger);
                }
                if (StringUtils.isEmpty(response)) {
                    logger.warn(
                            "Failed to get response from Doris FE {}, http code is {}",
                            request.getURI(),
                            statusCode);
                    continue;
                }
                logger.trace(
                        "Success get response from Doris FE: {}, response is: {}.",
                        request.getURI(),
                        response);
                // Handle the problem of inconsistent data format returned by http v1 and v2
                Map map = OBJECT_MAPPER.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return OBJECT_MAPPER.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                logger.warn(ErrorMessages.CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }
        String errMsg =
                "Connect to "
                        + request.getURI().toString()
                        + "failed, status code is "
                        + statusCode
                        + ".";
        throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, ex);
    }

    private static String getConnectionPost(
            HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", user, passwd)
                                        .getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content, StandardCharsets.UTF_8);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn, logger);
    }

    private static String getConnectionGet(
            String request, String user, String passwd, Logger logger) throws IOException {
        URL realUrl = new URL(request);
        // open connection
        HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
        String authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", user, passwd)
                                        .getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + authEncoding);

        connection.connect();
        return parseResponse(connection, logger);
    }

    private static String parseResponse(HttpURLConnection connection, Logger logger)
            throws IOException {
        int responseCode = connection.getResponseCode();
        if (responseCode != HttpStatus.SC_OK) {
            logger.warn(
                    "Failed to get response from Doris {}, http code is {}",
                    connection.getURL(),
                    responseCode);
            throw new IOException("Failed to get response from Doris");
        }

        StringBuilder result = new StringBuilder();
        try (BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(
                                connection.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
        }

        return result.toString();
    }

    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier, Logger logger)
            throws DorisConnectorException {
        logger.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            String errMsg =
                    String.format(
                            ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE,
                            "table.identifier",
                            tableIdentifier);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            String errMsg =
                    String.format(
                            ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE,
                            "table.identifier",
                            tableIdentifier);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        return identifier;
    }

    @VisibleForTesting
    static String getUriStr(String node, DorisSourceTable dorisSourceTable, Logger logger)
            throws DorisConnectorException {
        String tableIdentifier =
                dorisSourceTable.getTablePath().getDatabaseName()
                        + "."
                        + dorisSourceTable.getTablePath().getTableName();
        String[] identifier = parseIdentifier(tableIdentifier, logger);
        return "http://"
                + node.trim()
                + API_PREFIX
                + "/"
                + identifier[0]
                + "/"
                + identifier[1]
                + "/";
    }

    public static List<PartitionDefinition> findPartitions(
            DorisSourceConfig dorisSourceConfig, DorisSourceTable dorisSourceTable, Logger logger)
            throws DorisConnectorException {
        String tableIdentifier =
                dorisSourceTable.getTablePath().getDatabaseName()
                        + "."
                        + dorisSourceTable.getTablePath().getTableName();
        SeaTunnelRowType rowType = dorisSourceTable.getCatalogTable().getSeaTunnelRowType();
        String[] tableIdentifiers = parseIdentifier(tableIdentifier, logger);
        String readFields = "*";
        if (rowType.getFieldNames().length != 0) {
            readFields = String.join(",", rowType.getFieldNames());
        }
        String sql =
                "select "
                        + readFields
                        + " from `"
                        + tableIdentifiers[0]
                        + "`.`"
                        + tableIdentifiers[1]
                        + "`";
        if (!StringUtils.isEmpty(dorisSourceTable.getFilterQuery())) {
            sql += " where " + dorisSourceTable.getFilterQuery();
        }
        logger.debug("Query SQL Sending to Doris FE is: '{}'.", sql);

        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to Doris FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");

        List<String> feNodes = Arrays.asList(dorisSourceConfig.getFrontends().split(","));
        Collections.shuffle(feNodes);
        int feNodesNum = feNodes.size();
        String resStr = null;

        for (int i = 0; i < feNodesNum; i++) {
            try {
                HttpPost httpPost =
                        new HttpPost(
                                getUriStr(feNodes.get(i), dorisSourceTable, logger) + QUERY_PLAN);
                httpPost.setEntity(stringEntity);
                resStr = send(dorisSourceConfig, httpPost, logger);
                break;
            } catch (Exception e) {
                if (i == feNodesNum - 1) {
                    throw new DorisConnectorException(
                            DorisConnectorErrorCode.REST_SERVICE_FAILED, e);
                }
                log.error(
                        "Find partition error for feNode: {} with exception: {}",
                        feNodes.get(i),
                        e.getMessage());
            }
        }

        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                dorisSourceTable,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                tableIdentifiers[0],
                tableIdentifiers[1],
                logger);
    }

    @VisibleForTesting
    static QueryPlan getQueryPlan(String response, Logger logger) throws DorisConnectorException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        }

        if (queryPlan == null) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED,
                    ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE);
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger)
            throws DorisConnectorException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate Doris BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug(
                            "Choice a new Doris BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug(
                                "Current candidate Doris BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId,
                                target,
                                tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
            }

            logger.debug("Choice Doris BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    @VisibleForTesting
    static int tabletCountLimitForOnePartition(DorisSourceTable dorisSourceTable, Logger logger) {
        int tabletsSize = DorisSourceOptions.DORIS_TABLET_SIZE_DEFAULT;
        if (dorisSourceTable.getTabletSize() != null) {
            tabletsSize = dorisSourceTable.getTabletSize();
        }
        if (tabletsSize < DorisSourceOptions.DORIS_TABLET_SIZE_MIN) {
            logger.warn(
                    "{} is less than {}, set to default value {}.",
                    DorisSourceOptions.DORIS_TABLET_SIZE,
                    DorisSourceOptions.DORIS_TABLET_SIZE_MIN,
                    DorisSourceOptions.DORIS_TABLET_SIZE_MIN);
            tabletsSize = DorisSourceOptions.DORIS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(
            DorisSourceTable dorisSourceTable,
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table,
            Logger logger)
            throws DorisConnectorException {
        int tabletsSize = tabletCountLimitForOnePartition(dorisSourceTable, logger);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
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
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(
                                database,
                                table,
                                beInfo.getKey(),
                                partitionTablets,
                                opaquedQueryPlan);
                logger.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }
}
