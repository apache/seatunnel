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

package org.apache.seatunnel.connectors.seatunnel.slack.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.slack.client.SlackClient;
import org.apache.seatunnel.connectors.seatunnel.slack.exception.SlackConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.slack.exception.SlackConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.StringJoiner;

@Slf4j
public class SlackWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final String conversationId;
    private final SlackClient slackClient;
    private final SeaTunnelRowType seaTunnelRowType;
    private static final long POST_MSG_WAITING_TIME = 1500L;

    public SlackWriter(SeaTunnelRowType seaTunnelRowType, Config pluginConfig) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.slackClient = new SlackClient(pluginConfig);
        this.conversationId = slackClient.findConversation();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Object[] fields = element.getFields();
        StringJoiner stringJoiner = new StringJoiner(",", "", "\n");
        for (Object field : fields) {
            stringJoiner.add(String.valueOf(field));
        }
        String message = stringJoiner.toString();
        try {
            slackClient.publishMessage(conversationId, message);
            // Slack has a limit on the frequency of sending messages
            // One message can be sent as soon as one second
            Thread.sleep(POST_MSG_WAITING_TIME);
        } catch (Exception e) {
            log.error("Write to Slack Fail.", ExceptionUtils.getMessage(e));
            throw new SlackConnectorException(SlackConnectorErrorCode.WRITE_TO_SLACK_CHANNEL_FAILED, e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}
