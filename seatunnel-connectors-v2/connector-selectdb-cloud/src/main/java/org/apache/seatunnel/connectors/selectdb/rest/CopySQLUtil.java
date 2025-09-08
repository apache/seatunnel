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

package org.apache.seatunnel.connectors.selectdb.rest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.sink.writer.LoadStatus;
import org.apache.seatunnel.connectors.selectdb.util.HttpPostBuilder;
import org.apache.seatunnel.connectors.selectdb.util.HttpUtil;
import org.apache.seatunnel.connectors.selectdb.util.ResponseUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CopySQLUtil {

    private static final String COMMIT_PATTERN = "http://%s/copy/query";
    private static final int HTTP_TEMPORARY_REDIRECT = 200;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void copyFileToDatabase(
            SelectDBConfig selectdbConfig, String clusterName, String copySQL, String hostPort)
            throws IOException {
        long start = System.currentTimeMillis();
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();
        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        Map<String, String> params = new HashMap<>();
        params.put("cluster", clusterName);
        params.put("sql", copySQL);
        boolean success = false;
        CloseableHttpResponse response;
        String loadResult = "";
        while (retry++ <= selectdbConfig.getMaxRetries()) {
            HttpPostBuilder postBuilder = new HttpPostBuilder();
            postBuilder
                    .setUrl(String.format(COMMIT_PATTERN, hostPort))
                    .baseAuth(selectdbConfig.getUsername(), selectdbConfig.getPassword())
                    .setEntity(new StringEntity(OBJECT_MAPPER.writeValueAsString(params)));
            try {
                response = httpClient.execute(postBuilder.build());
            } catch (IOException e) {
                log.error("commit error : ", e);
                continue;
            }
            statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != HTTP_TEMPORARY_REDIRECT) {
                log.warn(
                        "commit failed with status {} {}, reason {}",
                        statusCode,
                        hostPort,
                        reasonPhrase);
            } else if (response.getEntity() != null) {
                loadResult = EntityUtils.toString(response.getEntity());
                success = handleCommitResponse(loadResult);
                if (success) {
                    log.info(
                            "commit success cost {}ms, response is {}",
                            System.currentTimeMillis() - start,
                            loadResult);
                    break;
                } else {
                    log.warn("commit failed, retry again");
                }
            }
        }

        if (!success) {
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.COMMIT_FAILED,
                    "commit failed with SQL: "
                            + copySQL
                            + " Commit error with status: "
                            + statusCode
                            + ", Reason: "
                            + reasonPhrase
                            + ", Response: "
                            + loadResult);
        }
    }

    private static boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse<CopyIntoResp> baseResponse =
                OBJECT_MAPPER.readValue(
                        loadResult, new TypeReference<BaseResponse<CopyIntoResp>>() {});
        if (baseResponse.getCode() == LoadStatus.SUCCESS) {
            CopyIntoResp dataResp = baseResponse.getData();
            if (LoadStatus.FAIL.equals(dataResp.getDataCode())) {
                log.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (!result.get("state").equals("FINISHED")
                        && !ResponseUtil.isCommitted(result.get("msg"))) {
                    log.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            log.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }
}
