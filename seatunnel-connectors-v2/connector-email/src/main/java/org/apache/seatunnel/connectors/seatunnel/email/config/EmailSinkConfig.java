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

package org.apache.seatunnel.connectors.seatunnel.email.config;

import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_AUTHORIZATION_CODE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_FROM_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_HOST;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_CONTENT;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_HEADLINE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_SMTP_AUTH;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TO_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TRANSPORT_PROTOCOL;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import lombok.NonNull;

@Data
public class EmailSinkConfig {
    private String emailFromAddress;
    private String emailToAddress;
    private String emailAuthorizationCode;
    private String emailMessageHeadline;
    private String emailMessageContent;
    private String emailHost;
    private String emailTransportProtocol;
    private String emailSmtpAuth;

    public EmailSinkConfig(@NonNull Config pluginConfig) {
        if (pluginConfig.hasPath(EMAIL_FROM_ADDRESS.key())) {
            this.emailFromAddress = pluginConfig.getString(EMAIL_FROM_ADDRESS.key());
        }
        if (pluginConfig.hasPath(EMAIL_TO_ADDRESS.key())) {
            this.emailToAddress = pluginConfig.getString(EMAIL_TO_ADDRESS.key());
        }
        if (pluginConfig.hasPath(EMAIL_AUTHORIZATION_CODE.key())) {
            this.emailAuthorizationCode = pluginConfig.getString(EMAIL_AUTHORIZATION_CODE.key());
        }
        if (pluginConfig.hasPath(EMAIL_MESSAGE_HEADLINE.key())) {
            this.emailMessageHeadline = pluginConfig.getString(EMAIL_MESSAGE_HEADLINE.key());
        }
        if (pluginConfig.hasPath(EMAIL_MESSAGE_CONTENT.key())) {
            this.emailMessageContent = pluginConfig.getString(EMAIL_MESSAGE_CONTENT.key());
        }
        if (pluginConfig.hasPath(EMAIL_HOST.key())) {
            this.emailHost = pluginConfig.getString(EMAIL_HOST.key());
        }
        if (pluginConfig.hasPath(EMAIL_TRANSPORT_PROTOCOL.key())) {
            this.emailTransportProtocol = pluginConfig.getString(EMAIL_TRANSPORT_PROTOCOL.key());
        }
        if (pluginConfig.hasPath(EMAIL_SMTP_AUTH.key())) {
            this.emailSmtpAuth = pluginConfig.getString(EMAIL_SMTP_AUTH.key());
        }
    }
}
