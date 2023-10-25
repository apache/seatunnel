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

import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.models.Backend;
import org.apache.seatunnel.connectors.doris.rest.models.BackendRow;
import org.apache.seatunnel.connectors.doris.rest.models.BackendV2;
import org.apache.seatunnel.connectors.doris.rest.models.QueryPlan;
import org.apache.seatunnel.connectors.doris.rest.models.Schema;
import org.apache.seatunnel.connectors.doris.rest.models.Tablet;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.stream.Collectors;

@Slf4j
public class RestService implements Serializable {
    public static final int REST_RESPONSE_STATUS_OK = 200;
    public static final int REST_RESPONSE_CODE_OK = 0;
    private static final String REST_RESPONSE_BE_ROWS_KEY = "rows";
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";
    private static final String UNIQUE_KEYS_TYPE = "UNIQUE_KEYS";
    @Deprecated private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
    private static final String FE_LOGIN = "/rest/v1/login";
    private static final String BASE_URL = "http://%s%s";

    private static String send(DorisConfig dorisConfig, HttpRequestBase request, Logger logger)
            throws DorisConnectorException {
        int connectTimeout =
                dorisConfig.getRequestConnectTimeoutMs() == null
                        ? DorisConfig.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : dorisConfig.getRequestConnectTimeoutMs();
        int socketTimeout =
                dorisConfig.getRequestReadTimeoutMs() == null
                        ? DorisConfig.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT
                        : dorisConfig.getRequestReadTimeoutMs();
        int retries =
                dorisConfig.getRequestRetries() == null
                        ? DorisConfig.DORIS_REQUEST_RETRIES_DEFAULT
                        : dorisConfig.getRequestRetries();
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
                dorisConfig.getUsername());
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
                                    dorisConfig.getUsername(),
                                    dorisConfig.getPassword(),
                                    logger);
                } else {
                    response =
                            getConnectionPost(
                                    request,
                                    dorisConfig.getUsername(),
                                    dorisConfig.getPassword(),
                                    logger);
                }
                if (response == null) {
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
                ObjectMapper mapper = new ObjectMapper();
                Map map = mapper.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return mapper.writeValueAsString(data);
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
        String res = IOUtils.toString(content);
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
    static String randomEndpoint(String feNodes, Logger logger) throws DorisConnectorException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            String errMsg =
                    String.format(ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    @VisibleForTesting
    static List<String> allEndpoints(String feNodes, Logger logger) throws DorisConnectorException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            String errMsg =
                    String.format(ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        List<String> nodes =
                Arrays.stream(feNodes.split(",")).map(String::trim).collect(Collectors.toList());
        Collections.shuffle(nodes);
        return nodes;
    }

    @VisibleForTesting
    public static String randomBackend(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException, IOException {
        List<BackendV2.BackendRowV2> backends = getBackendsV2(dorisConfig, logger);
        logger.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            logger.error(ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            String errMsg =
                    String.format(ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        Collections.shuffle(backends);
        BackendV2.BackendRowV2 backend = backends.get(0);
        return backend.getIp() + ":" + backend.getHttpPort();
    }

    public static String getBackend(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException {
        try {
            return randomBackend(dorisConfig, logger);
        } catch (Exception e) {
            String errMsg = "Failed to get backend via " + dorisConfig.getFrontends();
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        }
    }

    @Deprecated
    @VisibleForTesting
    static List<BackendRow> getBackends(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException, IOException {
        String feNodes = dorisConfig.getFrontends();
        String feNode = randomEndpoint(feNodes, logger);
        String beUrl = String.format(BASE_URL, feNode, BACKENDS);
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(dorisConfig, httpGet, logger);
        logger.info("Backend Info:{}", response);
        List<BackendRow> backends = parseBackend(response, logger);
        return backends;
    }

    @Deprecated
    static List<BackendRow> parseBackend(String response, Logger logger)
            throws DorisConnectorException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Backend backend;
        try {
            backend = mapper.readValue(response, Backend.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        }

        if (backend == null) {
            logger.error(ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED,
                    ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE);
        }
        List<BackendRow> backendRows =
                backend.getRows().stream().filter(v -> v.getAlive()).collect(Collectors.toList());
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    @VisibleForTesting
    public static List<BackendV2.BackendRowV2> getBackendsV2(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException, IOException {
        String feNodes = dorisConfig.getFrontends();
        List<String> feNodeList = allEndpoints(feNodes, logger);
        for (String feNode : feNodeList) {
            try {
                String beUrl = "http://" + feNode + BACKENDS_V2;
                HttpGet httpGet = new HttpGet(beUrl);
                String response = send(dorisConfig, httpGet, logger);
                logger.info("Backend Info:{}", response);
                List<BackendV2.BackendRowV2> backends = parseBackendV2(response, logger);
                return backends;
            } catch (DorisConnectorException e) {
                logger.info(
                        "Doris FE node {} is unavailable: {}, Request the next Doris FE node",
                        feNode,
                        e.getMessage());
            }
        }
        String errMsg = "No Doris FE is available, please check configuration";
        throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
    }

    static List<BackendV2.BackendRowV2> parseBackendV2(String response, Logger logger)
            throws DorisConnectorException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg, e);
        }

        if (backend == null) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED,
                    ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE);
        }
        List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    @VisibleForTesting
    static String getUriStr(DorisConfig dorisConfig, Logger logger) throws DorisConnectorException {
        String[] identifier = parseIdentifier(dorisConfig.getTableIdentifier(), logger);
        return "http://"
                + randomEndpoint(dorisConfig.getFrontends(), logger)
                + API_PREFIX
                + "/"
                + identifier[0]
                + "/"
                + identifier[1]
                + "/";
    }

    public static Schema getSchema(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException {
        logger.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getUriStr(dorisConfig, logger) + SCHEMA);
        String response = send(dorisConfig, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    public static boolean isUniqueKeyType(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException {
        try {
            return UNIQUE_KEYS_TYPE.equals(getSchema(dorisConfig, logger).getKeysType());
        } catch (Exception e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, e);
        }
    }

    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger)
            throws DorisConnectorException {
        logger.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
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

        if (schema == null) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.REST_SERVICE_FAILED,
                    ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE);
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new DorisConnectorException(DorisConnectorErrorCode.REST_SERVICE_FAILED, errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    public static List<PartitionDefinition> findPartitions(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException {
        String[] tableIdentifiers = parseIdentifier(dorisConfig.getTableIdentifier(), logger);
        String readFields =
                StringUtils.isBlank(dorisConfig.getReadField()) ? "*" : dorisConfig.getReadField();
        String sql =
                "select "
                        + readFields
                        + " from `"
                        + tableIdentifiers[0]
                        + "`.`"
                        + tableIdentifiers[1]
                        + "`";
        if (!StringUtils.isEmpty(dorisConfig.getFilterQuery())) {
            sql += " where " + dorisConfig.getFilterQuery();
        }
        logger.debug("Query SQL Sending to Doris FE is: '{}'.", sql);

        HttpPost httpPost = new HttpPost(getUriStr(dorisConfig, logger) + QUERY_PLAN);
        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to Doris FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(dorisConfig, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                dorisConfig,
                be2Tablets,
                queryPlan.getOpaquedQueryPlan(),
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
    static int tabletCountLimitForOnePartition(DorisConfig dorisConfig, Logger logger) {
        int tabletsSize = DorisConfig.DORIS_TABLET_SIZE_DEFAULT;
        if (dorisConfig.getTabletSize() != null) {
            tabletsSize = dorisConfig.getTabletSize();
        }
        if (tabletsSize < DorisConfig.DORIS_TABLET_SIZE_MIN) {
            logger.warn(
                    "{} is less than {}, set to default value {}.",
                    DorisConfig.DORIS_TABLET_SIZE,
                    DorisConfig.DORIS_TABLET_SIZE_MIN,
                    DorisConfig.DORIS_TABLET_SIZE_MIN);
            tabletsSize = DorisConfig.DORIS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(
            DorisConfig dorisConfig,
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table,
            Logger logger)
            throws DorisConnectorException {
        int tabletsSize = tabletCountLimitForOnePartition(dorisConfig, logger);
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
