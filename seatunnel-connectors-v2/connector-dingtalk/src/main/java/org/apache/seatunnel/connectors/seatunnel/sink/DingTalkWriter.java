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

package org.apache.seatunnel.connectors.seatunnel.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.exception.DingTalkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.exception.DingTalkConnectorException;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.taobao.api.ApiException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * DingTalk write class
 */
public class DingTalkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private RobotClient robotClient;

    public DingTalkWriter(String url, String secret) {
        this.robotClient = new RobotClient(url, secret);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        robotClient.send(element.toString());
    }

    @Override
    public void close() throws IOException {

    }

    private static class RobotClient implements Serializable {

        private String url;

        private String secret;

        private DefaultDingTalkClient client;

        public RobotClient(String url, String secret) {
            this.url = url;
            this.secret = secret;
        }

        public OapiRobotSendResponse send(String message) throws IOException {
            if (null == client) {
                client = new DefaultDingTalkClient(getUrl());
            }
            OapiRobotSendRequest request = new OapiRobotSendRequest();
            request.setMsgtype("text");
            OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
            text.setContent(message);
            request.setText(text);
            try {
                return this.client.execute(request);
            } catch (ApiException e) {
                throw new DingTalkConnectorException(DingTalkConnectorErrorCode.SEND_RESPONSE_FAILED,
                        "Send response message to DinkTalk server failed", e);
            }
        }

        public String getUrl() throws IOException {
            Long timestamp = System.currentTimeMillis();
            String sign = getSign(timestamp);
            return url + "&timestamp=" + timestamp + "&sign=" + sign;
        }

        public String getSign(Long timestamp) throws IOException {
            try {
                String stringToSign = timestamp + "\n" + secret;
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                byte[] signData = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
                return URLEncoder.encode(Base64.getEncoder().encodeToString(signData), "UTF-8");
            } catch (Exception e) {
                throw new DingTalkConnectorException(DingTalkConnectorErrorCode.GET_SIGN_FAILED,
                        "Get signature from DinkTalk server failed", e);
            }
        }
    }

}

