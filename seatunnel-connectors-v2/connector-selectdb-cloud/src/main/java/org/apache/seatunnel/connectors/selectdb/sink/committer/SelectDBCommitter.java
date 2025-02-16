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

package org.apache.seatunnel.connectors.selectdb.sink.committer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.rest.CopySQLUtil;
import org.apache.seatunnel.connectors.selectdb.sink.HttpPutBuilder;
import org.apache.seatunnel.connectors.selectdb.util.HttpUtil;
import org.apache.seatunnel.connectors.selectdb.util.ResponseUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SelectDBCommitter implements SinkCommitter<SelectDBCommitInfo> {

    private static final String COMMIT_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private static final int HTTP_TEMPORARY_REDIRECT = 200;
    private final CloseableHttpClient httpClient;
    int maxRetry;

    private final SelectDBConfig selectdbConfig;

    public SelectDBCommitter(Config pluginConfig) {
        this(SelectDBConfig.loadConfig(pluginConfig), HttpUtil.getHttpRedirectClient());
    }

    public SelectDBCommitter(SelectDBConfig selectdbConfig, CloseableHttpClient client) {
        this.selectdbConfig = selectdbConfig;
        this.httpClient = client;
    }

    @Override
    public List<SelectDBCommitInfo> commit(List<SelectDBCommitInfo> commitInfos)
            throws IOException {
        for (SelectDBCommitInfo committable : commitInfos) {
            commitTransaction(committable);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<SelectDBCommitInfo> commitInfos) throws IOException {
        if (selectdbConfig.isEnableStreamLoad()) {
            for (SelectDBCommitInfo commitInfo : commitInfos) {
                streamLoadAbortTransaction(commitInfo);
            }
        }
    }

    private void commitTransaction(SelectDBCommitInfo commitInfo) throws IOException {
        String hostPort = commitInfo.getHostPort();
        String clusterName = commitInfo.getClusterName();
        String copySQL = commitInfo.getCopySQL();
        log.info("commit to cluster {} with copy sql: {}", clusterName, copySQL);
        if (selectdbConfig.isEnableStreamLoad()) {
            streamLoadCommitTransaction(commitInfo);
        } else {
            CopySQLUtil.copyFileToDatabase(selectdbConfig, clusterName, copySQL, hostPort);
        }
    }

    private void streamLoadCommitTransaction(SelectDBCommitInfo commitInfo) throws IOException {
        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        String hostPort = commitInfo.getHostPort();
        CloseableHttpResponse response = null;
        while (retry++ <= selectdbConfig.getMaxRetries()) {
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(String.format(COMMIT_PATTERN, hostPort, commitInfo.getClusterName()))
                    .baseAuth(selectdbConfig.getUsername(), selectdbConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(Long.valueOf(commitInfo.getCopySQL()))
                    .setEmptyEntity()
                    .commit();
            try {
                response = httpClient.execute(putBuilder.build());
            } catch (IOException e) {
                log.error("commit transaction failed: ", e);
                hostPort = selectdbConfig.getLoadUrl();
                continue;
            }
            statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != HTTP_TEMPORARY_REDIRECT) {
                log.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
                hostPort = selectdbConfig.getLoadUrl();
            } else {
                break;
            }
        }

        if (statusCode != HTTP_TEMPORARY_REDIRECT) {
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.STREAM_LOAD_FAILED, reasonPhrase);
        }

        ObjectMapper mapper = new ObjectMapper();
        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res =
                    mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
            if (!"Success".equals(res.get("status"))) {
                log.error(
                        "commit transaction error url:{},TxnId:{},result:{}",
                        String.format(COMMIT_PATTERN, hostPort, commitInfo.getClusterName()),
                        commitInfo.getCopySQL(),
                        loadResult);
                throw new SelectDBConnectorException(
                        SelectDBConnectorErrorCode.COMMIT_FAILED, loadResult);
            } else {
                log.info("load result {}", loadResult);
            }
        }
    }

    private void streamLoadAbortTransaction(SelectDBCommitInfo committable) throws IOException {
        int statusCode;
        int retry = 0;
        String hostPort = committable.getHostPort();
        CloseableHttpResponse response = null;
        while (retry++ <= maxRetry) {
            HttpPutBuilder builder = new HttpPutBuilder();
            builder.setUrl(String.format(COMMIT_PATTERN, hostPort, committable.getClusterName()))
                    .baseAuth(selectdbConfig.getUsername(), selectdbConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(Long.valueOf(committable.getCopySQL()))
                    .setEmptyEntity()
                    .abort();
            response = httpClient.execute(builder.build());
            statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HTTP_TEMPORARY_REDIRECT || response.getEntity() == null) {
                log.warn("abort transaction response: " + response.getStatusLine().toString());
                throw new SelectDBConnectorException(
                        SelectDBConnectorErrorCode.STREAM_LOAD_FAILED,
                        "Fail to abort transaction "
                                + committable.getCopySQL()
                                + " with url "
                                + String.format(
                                        COMMIT_PATTERN, hostPort, committable.getClusterName()));
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        String loadResult = EntityUtils.toString(response.getEntity());
        Map<String, String> res =
                mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
        if (!"Success".equals(res.get("status"))) {
            if (ResponseUtil.isStreamLoadCommitted(res.get("msg"))) {
                throw new SelectDBConnectorException(
                        SelectDBConnectorErrorCode.STREAM_LOAD_FAILED,
                        "try abort committed transaction, " + "do you recover from old savepoint?");
            }
            log.warn(
                    "Fail to abort transaction. txnId: {}, error: {}",
                    committable.getCopySQL(),
                    res.get("msg"));
        }
    }
}
