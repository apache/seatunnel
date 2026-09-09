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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SlackClientTest {

    @Test
    void shouldUseOauthTokenWhenPublishingMessage() throws Exception {
        MethodsClient methodsClient = mock(MethodsClient.class);
        ChatPostMessageResponse response = mock(ChatPostMessageResponse.class);
        when(response.isOk()).thenReturn(true);
        when(methodsClient.chatPostMessage(any(ChatPostMessageRequest.class))).thenReturn(response);

        SlackClient client = new SlackClient(ReadonlyConfig.fromMap(slackConfig()), methodsClient);

        Assertions.assertTrue(client.publishMessage("C123", "test message"));

        ArgumentCaptor<ChatPostMessageRequest> requestCaptor =
                ArgumentCaptor.forClass(ChatPostMessageRequest.class);
        verify(methodsClient).chatPostMessage(requestCaptor.capture());

        ChatPostMessageRequest request = requestCaptor.getValue();
        Assertions.assertEquals("xoxb-token", request.getToken());
        Assertions.assertEquals("C123", request.getChannel());
        Assertions.assertEquals("test message", request.getText());
    }

    @Test
    void testCreateMessageRequestUsesOAuthToken() {
        ChatPostMessageRequest request =
                SlackClient.createMessageRequest(
                        "xoxb-test-token", "resolved-channel-id", "test-message");

        Assertions.assertEquals("xoxb-test-token", request.getToken());
        Assertions.assertEquals("resolved-channel-id", request.getChannel());
        Assertions.assertEquals("test-message", request.getText());
    }

    private Map<String, Object> slackConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("oauth_token", "xoxb-token");
        config.put("slack_channel", "alerts");
        return config;
    }
}
