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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.client.SlackClient;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.config.SlackConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SlackWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private SlackConfig slackConfig;
    private StringBuffer stringBuffer;
    private final SlackClient slackClient;
    private final SeaTunnelRowType seaTunnelRowType;

    public SlackWriter(SeaTunnelRowType seaTunnelRowType, Config pluginConfig) {
        this.slackConfig = new SlackConfig(pluginConfig);
        this.seaTunnelRowType = seaTunnelRowType;
        this.stringBuffer = new StringBuffer();
        this.slackClient = new SlackClient(slackConfig);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Object[] fields = element.getFields();

        for (Object field : fields) {
            stringBuffer.append(field.toString() + ",");
        }
        stringBuffer.deleteCharAt(fields.length - 1);
        stringBuffer.append("\n");
        try {
            String conversationId = slackClient.findConversation();
            if (conversationId == null || conversationId.equals("")) {
                throw new RuntimeException("There are no channels to write!");
            }
            slackClient.publishMessage(conversationId, stringBuffer.toString());
        } catch (Exception e) {
            log.warn("Write to Slack Fail.", e);
            throw new RuntimeException("Write to Slack Fail.", e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}
