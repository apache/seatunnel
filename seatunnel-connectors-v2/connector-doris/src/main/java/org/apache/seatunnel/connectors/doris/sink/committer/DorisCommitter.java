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

package org.apache.seatunnel.connectors.doris.sink.committer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.sink.HttpPutBuilder;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.util.DorisRedirectExceptionBuilder;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;
import org.apache.seatunnel.connectors.doris.util.ResponseUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The committer to commit transaction. */
@Slf4j
public class DorisCommitter implements SinkCommitter<DorisCommitInfo> {
    private static final String COMMIT_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private static final int HTTP_OK = 200;
    private static final int HTTP_TEMPORARY_REDIRECT = 307;
    private final CloseableHttpClient httpClient;
    private final DorisSinkConfig dorisSinkConfig;
    int maxRetry;

    public DorisCommitter(DorisSinkConfig dorisSinkConfig) {
        this(dorisSinkConfig, new HttpUtil().getHttpClient());
    }

    public DorisCommitter(DorisSinkConfig dorisSinkConfig, CloseableHttpClient client) {
        this.dorisSinkConfig = dorisSinkConfig;
        this.httpClient = client;
        this.maxRetry = dorisSinkConfig.getMaxRetries();
    }

    @Override
    public List<DorisCommitInfo> commit(List<DorisCommitInfo> commitInfos) throws IOException {
        for (DorisCommitInfo commitInfo : commitInfos) {
            commitTransaction(commitInfo);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<DorisCommitInfo> commitInfos) throws IOException {
        for (DorisCommitInfo commitInfo : commitInfos) {
            abortTransaction(commitInfo);
        }
    }

    private void commitTransaction(DorisCommitInfo committable)
            throws IOException, DorisConnectorException {
        String reasonPhrase = null;
        IOException lastIOException = null;
        DorisConnectorException lastRedirectException = null;
        List<String> retryHosts = resolveFrontendRetryHosts(committable.getHostPort());
        for (int attempt = 0; attempt <= dorisSinkConfig.getMaxRetries(); attempt++) {
            String hostPort = retryHosts.get(attempt % retryHosts.size());
            String requestUrl = String.format(COMMIT_PATTERN, hostPort, committable.getDb());
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(requestUrl)
                    .baseAuth(dorisSinkConfig.getUsername(), dorisSinkConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(committable.getTxbID())
                    .setEmptyEntity()
                    .commit();
            CloseableHttpResponse response;
            try {
                response =
                        HttpUtil.executeWithRedirectTracking(
                                httpClient,
                                putBuilder.build(),
                                requestUrl,
                                dorisSinkConfig.isDirectToBe(),
                                dorisSinkConfig.getEnable2PC(),
                                "commit");
            } catch (DorisConnectorException e) {
                lastRedirectException = e;
                log.error("commit transaction redirect follow-up failed on {}: ", hostPort, e);
                continue;
            } catch (IOException e) {
                lastIOException = e;
                log.error("commit transaction failed on {}: ", hostPort, e);
                continue;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == HTTP_TEMPORARY_REDIRECT) {
                Header location = response.getFirstHeader("Location");
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        DorisRedirectExceptionBuilder.build(
                                requestUrl,
                                location == null ? null : location.getValue(),
                                dorisSinkConfig.isDirectToBe(),
                                dorisSinkConfig.getEnable2PC(),
                                "commit"));
            }
            if (statusCode != HTTP_OK) {
                log.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
                continue;
            }
            handleCommitSuccess(committable, hostPort, response);
            return;
        }

        if (lastRedirectException != null) {
            throw lastRedirectException;
        }
        if (lastIOException != null && reasonPhrase == null) {
            throw lastIOException;
        }
        throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, reasonPhrase);
    }

    private void abortTransaction(DorisCommitInfo committable)
            throws IOException, DorisConnectorException {
        List<String> retryHosts = resolveFrontendRetryHosts(committable.getHostPort());
        String responseStatus = null;
        IOException lastIOException = null;
        DorisConnectorException lastRedirectException = null;
        for (int attempt = 0; attempt <= maxRetry; attempt++) {
            String hostPort = retryHosts.get(attempt % retryHosts.size());
            String requestUrl = String.format(COMMIT_PATTERN, hostPort, committable.getDb());
            HttpPutBuilder builder = new HttpPutBuilder();
            builder.setUrl(requestUrl)
                    .baseAuth(dorisSinkConfig.getUsername(), dorisSinkConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(committable.getTxbID())
                    .setEmptyEntity()
                    .abort();
            CloseableHttpResponse response;
            try {
                response =
                        HttpUtil.executeWithRedirectTracking(
                                httpClient,
                                builder.build(),
                                requestUrl,
                                dorisSinkConfig.isDirectToBe(),
                                dorisSinkConfig.getEnable2PC(),
                                "abort");
            } catch (DorisConnectorException e) {
                lastRedirectException = e;
                log.error("abort transaction redirect follow-up failed on {}: ", hostPort, e);
                continue;
            } catch (IOException e) {
                lastIOException = e;
                log.error("abort transaction failed on {}: ", hostPort, e);
                continue;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            responseStatus = response.getStatusLine().toString();
            if (statusCode == HTTP_TEMPORARY_REDIRECT) {
                Header location = response.getFirstHeader("Location");
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        DorisRedirectExceptionBuilder.build(
                                requestUrl,
                                location == null ? null : location.getValue(),
                                dorisSinkConfig.isDirectToBe(),
                                dorisSinkConfig.getEnable2PC(),
                                "abort"));
            }
            if (statusCode != HTTP_OK || response.getEntity() == null) {
                log.warn("abort transaction response: {}", response.getStatusLine().toString());
                continue;
            }
            handleAbortSuccess(committable, response);
            return;
        }
        if (lastRedirectException != null) {
            throw lastRedirectException;
        }
        if (lastIOException != null && responseStatus == null) {
            throw lastIOException;
        }
        throw new DorisConnectorException(
                DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                responseStatus == null
                        ? "Fail to abort transaction "
                                + committable.getTxbID()
                                + " with frontends "
                                + dorisSinkConfig.getFrontends()
                        : responseStatus);
    }

    private void handleCommitSuccess(
            DorisCommitInfo committable, String hostPort, CloseableHttpResponse response)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res =
                    mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
            if (!LoadStatus.SUCCESS.equals(res.get("status"))
                    && !ResponseUtil.isCommitted(res.get("msg"))) {
                log.error(
                        "commit transaction error url:{},TxnId:{},result:{}",
                        String.format(COMMIT_PATTERN, hostPort, committable.getDb()),
                        committable.getTxbID(),
                        loadResult);
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.COMMIT_FAILED, loadResult);
            } else {
                log.info("load result {}", loadResult);
            }
        }
    }

    private void handleAbortSuccess(DorisCommitInfo committable, CloseableHttpResponse response)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String loadResult = EntityUtils.toString(response.getEntity());
        Map<String, String> res =
                mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
        if (!LoadStatus.SUCCESS.equals(res.get("status"))) {
            if (ResponseUtil.isCommitted(res.get("msg"))) {
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        "try abort committed transaction, " + "do you recover from old savepoint?");
            }
            log.warn(
                    "Fail to abort transaction. txnId: {}, error: {}",
                    committable.getTxbID(),
                    res.get("msg"));
        }
    }

    private List<String> resolveFrontendRetryHosts(String preferredHostPort) {
        List<String> hosts = new ArrayList<>();
        hosts.add(preferredHostPort);
        Arrays.stream(dorisSinkConfig.getFrontends().split(","))
                .map(String::trim)
                .filter(host -> !host.isEmpty())
                .filter(host -> !host.equals(preferredHostPort))
                .forEach(hosts::add);
        return hosts;
    }
}
