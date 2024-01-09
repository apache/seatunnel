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
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.models.BackendV2;
import org.apache.seatunnel.connectors.doris.util.ErrorMessages;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;

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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class RestService implements Serializable {
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";

    private static String send(DorisConfig dorisConfig, HttpRequestBase request, Logger logger)
            throws DorisConnectorException {
        int connectTimeout =
                dorisConfig.getRequestConnectTimeoutMs() == null
                        ? DorisOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : dorisConfig.getRequestConnectTimeoutMs();
        int socketTimeout =
                dorisConfig.getRequestReadTimeoutMs() == null
                        ? DorisOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT
                        : dorisConfig.getRequestReadTimeoutMs();
        int retries =
                dorisConfig.getRequestRetries() == null
                        ? DorisOptions.DORIS_REQUEST_RETRIES_DEFAULT
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
            throws DorisConnectorException {
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

    @VisibleForTesting
    public static List<BackendV2.BackendRowV2> getBackendsV2(DorisConfig dorisConfig, Logger logger)
            throws DorisConnectorException {
        String feNodes = dorisConfig.getFrontends();
        List<String> feNodeList = allEndpoints(feNodes, logger);
        for (String feNode : feNodeList) {
            try {
                String beUrl = "http://" + feNode + BACKENDS_V2;
                HttpGet httpGet = new HttpGet(beUrl);
                String response = send(dorisConfig, httpGet, logger);
                logger.info("Backend Info:{}", response);
                return parseBackendV2(response, logger);
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
            throws DorisConnectorException {
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
}
