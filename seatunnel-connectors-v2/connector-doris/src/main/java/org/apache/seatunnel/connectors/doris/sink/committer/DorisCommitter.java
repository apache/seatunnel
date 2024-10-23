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

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.sink.HttpPutBuilder;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;
import org.apache.seatunnel.connectors.doris.util.ResponseUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The committer to commit transaction. */
@Slf4j
public class DorisCommitter implements SinkCommitter<DorisCommitInfo> {
    private static final String COMMIT_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private static final int HTTP_TEMPORARY_REDIRECT = 200;
    private final CloseableHttpClient httpClient;
    private final DorisSinkConfig dorisSinkConfig;
    int maxRetry;

    public DorisCommitter(DorisSinkConfig dorisSinkConfig) {
        this(dorisSinkConfig, new HttpUtil().getHttpClient());
    }

    public DorisCommitter(DorisSinkConfig dorisSinkConfig, CloseableHttpClient client) {
        this.dorisSinkConfig = dorisSinkConfig;
        this.httpClient = client;
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
        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        String hostPort = committable.getHostPort();
        CloseableHttpResponse response = null;
        while (retry++ <= dorisSinkConfig.getMaxRetries()) {
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(String.format(COMMIT_PATTERN, hostPort, committable.getDb()))
                    .baseAuth(dorisSinkConfig.getUsername(), dorisSinkConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(committable.getTxbID())
                    .setEmptyEntity()
                    .commit();
            try {
                response = httpClient.execute(putBuilder.build());
            } catch (IOException e) {
                log.error("commit transaction failed: ", e);
                hostPort = dorisSinkConfig.getFrontends();
                continue;
            }
            statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != HTTP_TEMPORARY_REDIRECT) {
                log.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
                hostPort = dorisSinkConfig.getFrontends();
            } else {
                break;
            }
        }

        if (statusCode != HTTP_TEMPORARY_REDIRECT) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.STREAM_LOAD_FAILED, reasonPhrase);
        }

        ObjectMapper mapper = new ObjectMapper();
        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res =
                    mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
            if (!LoadStatus.SUCCESS.equals(res.get("status"))) {
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

    private void abortTransaction(DorisCommitInfo committable)
            throws IOException, DorisConnectorException {
        int statusCode;
        int retry = 0;
        String hostPort = committable.getHostPort();
        CloseableHttpResponse response = null;
        while (retry++ <= maxRetry) {
            HttpPutBuilder builder = new HttpPutBuilder();
            builder.setUrl(String.format(COMMIT_PATTERN, hostPort, committable.getDb()))
                    .baseAuth(dorisSinkConfig.getUsername(), dorisSinkConfig.getPassword())
                    .addCommonHeader()
                    .addTxnId(committable.getTxbID())
                    .setEmptyEntity()
                    .abort();
            response = httpClient.execute(builder.build());
            statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HTTP_TEMPORARY_REDIRECT || response.getEntity() == null) {
                log.warn("abort transaction response: " + response.getStatusLine().toString());
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        "Fail to abort transaction "
                                + committable.getTxbID()
                                + " with url "
                                + String.format(COMMIT_PATTERN, hostPort, committable.getDb()));
            }
        }

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
}
