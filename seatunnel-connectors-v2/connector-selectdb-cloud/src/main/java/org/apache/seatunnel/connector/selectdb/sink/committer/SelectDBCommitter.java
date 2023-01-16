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

package org.apache.seatunnel.connector.selectdb.sink.committer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connector.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connector.selectdb.rest.BaseResponse;
import org.apache.seatunnel.connector.selectdb.rest.CopyIntoResp;
import org.apache.seatunnel.connector.selectdb.util.HttpUtil;
import org.apache.seatunnel.connector.selectdb.util.HttpPostBuilder;
import org.apache.seatunnel.connector.selectdb.util.ResponseUtil;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadStatus.SUCCESS;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadStatus.FAIL;

public class SelectDBCommitter implements SinkCommitter<SelectDBCommitInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(SelectDBCommitter.class);
    private static final String commitPattern = "http://%s/copy/query";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CloseableHttpClient httpClient;
    private final SelectDBConfig selectdbConfig;
    int maxRetry;

    public SelectDBCommitter(Config pluginConfig) {
        this(SelectDBConfig.loadConfig(pluginConfig), SelectDBConfig.loadConfig(pluginConfig).getMaxRetries(), new HttpUtil().getHttpClient());
    }

    public SelectDBCommitter(SelectDBConfig selectdbConfig, int maxRetry, CloseableHttpClient client) {
        this.selectdbConfig = selectdbConfig;
        this.maxRetry = maxRetry;
        this.httpClient = client;
    }

    @Override
    public List<SelectDBCommitInfo> commit(List<SelectDBCommitInfo> commitInfos) throws IOException {
        for (SelectDBCommitInfo committable : commitInfos) {
            commitTransaction(committable);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<SelectDBCommitInfo> commitInfos) throws IOException {
    }

    private void commitTransaction(SelectDBCommitInfo committable) throws IOException {
        long start = System.currentTimeMillis();
        String hostPort = committable.getHostPort();
        String clusterName = committable.getClusterName();
        String copySQL = committable.getCopySQL();
        LOG.info("commit to cluster {} with copy sql: {}", clusterName, copySQL);

        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        Map<String, String> params = new HashMap<>();
        params.put("cluster", clusterName);
        params.put("sql", copySQL);
        boolean success = false;
        CloseableHttpResponse response;
        String loadResult = "";
        while (retry++ <= maxRetry) {
            HttpPostBuilder postBuilder = new HttpPostBuilder();
            postBuilder.setUrl(String.format(commitPattern, hostPort))
                    .baseAuth(selectdbConfig.getUsername(), selectdbConfig.getPassword())
                    .setEntity(new StringEntity(objectMapper.writeValueAsString(params)));
            try {
                response = httpClient.execute(postBuilder.build());
            } catch (IOException e) {
                LOG.error("commit error : ", e);
                continue;
            }
            statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != 200) {
                LOG.warn("commit failed with status {} {}, reason {}", statusCode, hostPort, reasonPhrase);
            } else if (response.getEntity() != null) {
                loadResult = EntityUtils.toString(response.getEntity());
                success = handleCommitResponse(loadResult);
                if (success) {
                    LOG.info("commit success cost {}ms, response is {}", System.currentTimeMillis() - start, loadResult);
                    break;
                } else {
                    LOG.warn("commit failed, retry again");
                }
            }
        }

        if (!success) {
            LOG.error("commit error with status {}, reason {}, response {}", statusCode, reasonPhrase, loadResult);
            throw new SelectDBConnectorException(SelectDBConnectorErrorCode.COMMIT_FAILED, committable.getCopySQL());
        }
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse<CopyIntoResp> baseResponse = objectMapper.readValue(loadResult, new TypeReference<BaseResponse<CopyIntoResp>>() {
        });
        if (baseResponse.getCode() == SUCCESS) {
            CopyIntoResp dataResp = baseResponse.getData();
            if (FAIL.equals(dataResp.getDataCode())) {
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (!result.get("state").equals("FINISHED") && !ResponseUtil.isCommitted(result.get("msg"))) {
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            LOG.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }
}
