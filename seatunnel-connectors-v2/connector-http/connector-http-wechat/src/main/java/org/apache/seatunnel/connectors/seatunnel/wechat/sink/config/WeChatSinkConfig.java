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

package org.apache.seatunnel.connectors.seatunnel.wechat.sink.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@Getter
public class WeChatSinkConfig extends HttpConfig {
    public static final String WECHAT_SEND_MSG_SUPPORT_TYPE = "text";
    public static final String WECHAT_SEND_MSG_TYPE_KEY = "msgtype";
    public static final String WECHAT_SEND_MSG_CONTENT_KEY = "content";
    public static final Option<List<String>> MENTIONED_LIST = Options.key("mentioned_list")
            .listType()
            .noDefaultValue()
            .withDescription("A list of userids to remind the specified members in the group (@ a member), @ all means to remind everyone");
    public static final Option<List<String>> MENTIONED_MOBILE_LIST = Options.key("mentioned_mobile_list")
            .listType()
            .noDefaultValue()
            .withDescription("Mobile phone number list, remind the group member corresponding to the mobile phone number (@ a member), @ all means remind everyone");
    private List<String> mentionedList;
    private List<String> mentionedMobileList;

    public WeChatSinkConfig(@NonNull Config pluginConfig){
        if (pluginConfig.hasPath(MENTIONED_LIST.key())) {
            this.mentionedList = pluginConfig.getStringList(MENTIONED_LIST.key());
        }
        if (pluginConfig.hasPath(MENTIONED_MOBILE_LIST.key())) {
            this.mentionedMobileList = pluginConfig.getStringList(MENTIONED_MOBILE_LIST.key());
        }
    }
}
