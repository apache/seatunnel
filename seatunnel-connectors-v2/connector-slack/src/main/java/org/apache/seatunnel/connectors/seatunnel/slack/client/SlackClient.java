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

package org.apache.seatunnel.connectors.seatunnel.slack.client;

import static org.apache.seatunnel.connectors.seatunnel.slack.config.SlackConfig.OAUTH_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.slack.config.SlackConfig.SLACK_CHANNEL;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.slack.exception.SlackConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.slack.exception.SlackConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.model.Conversation;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class SlackClient {
    private final Config pluginConfig;
    private final MethodsClient methodsClient;

    public SlackClient(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        this.methodsClient = Slack.getInstance().methods();
    }

    /**
     * Find conversation ID using the conversations.list method
     */
    public String findConversation() {
        String conversionId = "";
        List<Conversation> channels;
        try {
            // Get Conversion List
            ConversationsListResponse conversationsListResponse = methodsClient.conversationsList(r -> r
                // The Token used to initialize app
                .token(pluginConfig.getString(OAUTH_TOKEN.key()))
            );
            channels = conversationsListResponse.getChannels();
            for (Conversation channel : channels) {
                if (channel.getName().equals(pluginConfig.getString(SLACK_CHANNEL.key()))) {
                    conversionId = channel.getId();
                    // Break from for loop
                    break;
                }
            }
        } catch (IOException | SlackApiException e) {
            log.warn("Find Slack Conversion Fail.", e);
            throw new SlackConnectorException(SlackConnectorErrorCode.FIND_SLACK_CONVERSATION_FAILED, e);
        }
        return conversionId;
    }

    /**
     * Post a message to a channel using Channel ID and message text
     */
    public boolean publishMessage(String channelId, String text) {
        boolean publishMessageSuccess = false;
        try {
            ChatPostMessageResponse chatPostMessageResponse = methodsClient.chatPostMessage(r -> r
                // The Token used to initialize app
                .token(pluginConfig.getString(SLACK_CHANNEL.key()))
                .channel(channelId)
                .text(text)
            );
            publishMessageSuccess = chatPostMessageResponse.isOk();
        } catch (IOException | SlackApiException e) {
            log.error("error: {}", ExceptionUtils.getMessage(e));
        }
        return publishMessageSuccess;
    }

    /**
     * Close Conversion
     */
    public void closeMethodClient() {
    }
}
