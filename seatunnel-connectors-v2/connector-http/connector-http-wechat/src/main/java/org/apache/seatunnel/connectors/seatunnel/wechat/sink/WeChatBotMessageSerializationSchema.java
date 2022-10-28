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

package org.apache.seatunnel.connectors.seatunnel.wechat.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.wechat.sink.config.WeChatSinkConfig;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.Map;

public class WeChatBotMessageSerializationSchema implements SerializationSchema {
    private final WeChatSinkConfig weChatSinkConfig;
    private final SeaTunnelRowType rowType;
    private final JsonSerializationSchema jsonSerializationSchema;

    public WeChatBotMessageSerializationSchema(WeChatSinkConfig weChatSinkConfig,
                                               SeaTunnelRowType rowType) {
        this.weChatSinkConfig = weChatSinkConfig;
        this.rowType = rowType;
        this.jsonSerializationSchema = new JsonSerializationSchema(rowType);
    }

    @SneakyThrows
    @Override
    public byte[] serialize(SeaTunnelRow row) {
        StringBuffer stringBuffer = new StringBuffer();
        int totalFields = rowType.getTotalFields();
        for (int i = 0; i < totalFields; i++) {
            stringBuffer.append(rowType.getFieldName(i) + ": " + row.getField(i) + "\\n");
        }
        if (totalFields > 0) {
            //remove last empty line
            stringBuffer.delete(stringBuffer.length() - 2, stringBuffer.length());
        }

        HashMap<Object, Object> content = new HashMap<>();
        content.put(WeChatSinkConfig.WECHAT_SEND_MSG_CONTENT_KEY, stringBuffer.toString());
        if (!CollectionUtils.isEmpty(weChatSinkConfig.getMentionedList())) {
            content.put(WeChatSinkConfig.MENTIONED_LIST, weChatSinkConfig.getMentionedList());
        }
        if (!CollectionUtils.isEmpty(weChatSinkConfig.getMentionedMobileList())) {
            content.put(WeChatSinkConfig.MENTIONED_MOBILE_LIST, weChatSinkConfig.getMentionedMobileList());
        }

        Map<String, Object> wechatMessage = new HashMap<>();
        wechatMessage.put(WeChatSinkConfig.WECHAT_SEND_MSG_TYPE_KEY, WeChatSinkConfig.WECHAT_SEND_MSG_SUPPORT_TYPE);
        wechatMessage.put(WeChatSinkConfig.WECHAT_SEND_MSG_SUPPORT_TYPE, content);
        return jsonSerializationSchema.getMapper().writeValueAsBytes(wechatMessage);
    }
}
