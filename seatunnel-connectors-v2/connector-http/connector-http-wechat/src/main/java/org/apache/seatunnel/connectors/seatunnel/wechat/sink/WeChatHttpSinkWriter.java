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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.wechat.sink.config.WeChatSinkConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.HashMap;

public class WeChatHttpSinkWriter extends HttpSinkWriter {

    private final WeChatSinkConfig weChatSinkConfig;
    private final SeaTunnelRowType seaTunnelRowType;

    public WeChatHttpSinkWriter(HttpParameter httpParameter,  Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        //new SeaTunnelRowType can match SeaTunnelRowWrapper fields sequence
        super(new SeaTunnelRowType(new String[]{WeChatSinkConfig.WECHAT_SEND_MSG_TYPE_KEY, WeChatSinkConfig.WECHAT_SEND_MSG_SUPPORT_TYPE}, new SeaTunnelDataType[]{BasicType.VOID_TYPE, BasicType.VOID_TYPE}), httpParameter);
        this.weChatSinkConfig = new WeChatSinkConfig(pluginConfig);
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();
        int totalFields = seaTunnelRowType.getTotalFields();
        for (int i = 0; i < totalFields; i++) {
            stringBuffer.append(seaTunnelRowType.getFieldName(i) + ": " + element.getField(i) + "\\n");
        }
        if (totalFields > 0) {
            //remove last empty line
            stringBuffer.delete(stringBuffer.length() - 2, stringBuffer.length());
        }
        HashMap<Object, Object> objectMap = new HashMap<>();
        objectMap.put(WeChatSinkConfig.WECHAT_SEND_MSG_CONTENT_KEY, stringBuffer.toString());
        if (!CollectionUtils.isEmpty(weChatSinkConfig.getMentionedList())) {
            objectMap.put(WeChatSinkConfig.MENTIONED_LIST, weChatSinkConfig.getMentionedList());
        }
        if (!CollectionUtils.isEmpty(weChatSinkConfig.getMentionedMobileList())) {
            objectMap.put(WeChatSinkConfig.MENTIONED_MOBILE_LIST, weChatSinkConfig.getMentionedMobileList());
        }
        //SeaTunnelRowWrapper can used to post wechat web hook
        SeaTunnelRow wechatRowWrapper = new SeaTunnelRow(new Object[]{WeChatSinkConfig.WECHAT_SEND_MSG_SUPPORT_TYPE, objectMap});
        super.write(wechatRowWrapper);
    }
}
