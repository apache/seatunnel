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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_AUTHORIZATION_CODE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_FROM_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_HOST;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_CONTENT;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_HEADLINE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_SMTP_AUTH;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_SMTP_PORT;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TO_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TRANSPORT_PROTOCOL;

@Data
public class EmailSinkConfig implements Serializable {
    private String emailFromAddress;
    private String emailToAddress;
    private String emailAuthorizationCode;
    private String emailMessageHeadline;
    private String emailMessageContent;
    private String emailHost;
    private String emailTransportProtocol;
    private Boolean emailSmtpAuth;
    private Integer emailSmtpPort;

    public EmailSinkConfig(@NonNull ReadonlyConfig pluginConfig) {
        super();
        this.emailFromAddress = pluginConfig.get(EMAIL_FROM_ADDRESS);
        this.emailToAddress = pluginConfig.get(EMAIL_TO_ADDRESS);
        this.emailAuthorizationCode = pluginConfig.get(EMAIL_AUTHORIZATION_CODE);
        this.emailMessageHeadline = pluginConfig.get(EMAIL_MESSAGE_HEADLINE);
        this.emailMessageContent = pluginConfig.get(EMAIL_MESSAGE_CONTENT);
        this.emailHost = pluginConfig.get(EMAIL_HOST);
        this.emailTransportProtocol = pluginConfig.get(EMAIL_TRANSPORT_PROTOCOL);
        this.emailSmtpAuth = pluginConfig.get(EMAIL_SMTP_AUTH);
        this.emailSmtpPort = pluginConfig.get(EMAIL_SMTP_PORT);
    }
}
