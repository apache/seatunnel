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

package org.apache.seatunnel.connectors.seatunnel.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.DingTalkConstant;
import org.apache.seatunnel.connectors.seatunnel.common.DingTalkParameter;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiV2DepartmentListsubRequest;
import com.dingtalk.api.response.OapiV2DepartmentListsubResponse;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DingTalkSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DingTalkSourceReader.class);
    protected final SingleSplitReaderContext context;
    protected final DingTalkParameter dtParameter;
    protected DingTalkClient dtClient;
    protected OapiV2DepartmentListsubRequest dtRequest;
    protected final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public DingTalkSourceReader(DingTalkParameter dtParameter, SingleSplitReaderContext context, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.context = context;
        this.dtParameter = dtParameter;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open() {
        dtClient = new DefaultDingTalkClient(dtParameter.getApiClient());
        dtRequest = new OapiV2DepartmentListsubRequest();
        LOGGER.info("Ding Talk Access Token is :" + dtParameter.getAccessToken());
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        try {
            OapiV2DepartmentListsubResponse response = dtClient.execute(dtRequest, dtParameter.getAccessToken());
            if (DingTalkConstant.STATUS_OK.equals(response.getErrmsg())) {
                String tmpContent = response.getBody();
                JsonNode bodyJson = JsonUtils.stringToJsonNode(tmpContent);
                JsonNode resJson = bodyJson.get(DingTalkConstant.BODY_RESULT);
                if (resJson.isArray()) {
                    for (JsonNode tmpJson : resJson) {
                        output.collect(new SeaTunnelRow(new Object[]{tmpJson.toString()}));
                    }
                }
            }
            LOGGER.error("Ding Talk client execute exception, response status code:[{}], content:[{}]", response.getErrorCode(), response.getBody());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                LOGGER.info("Closed the bounded http source");
                context.signalNoMoreElement();
            }
        }
    }
}
