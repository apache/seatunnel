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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import lombok.NonNull;

@Data
public class EmailSinkConfig {

    private static final String EMAIL_FROM_ADDRESS = "email_from_address";
    private static final String EMAIL_TO_ADDRESS = "email_to_address";
    private static final String EMAIL_AUTHORIZATION_CODE = "email_authorization_code";
    private static final String EMAIL_MESSAGE_HEADLINE = "email_message_headline";
    private static final String EMAIL_MESSAGE_CONTENT = "email_message_content";
    private static final String EMAIL_HOST = "email_host";
    private static final String EMAIL_TRANSPORT_PROTOCOL = "email_transport_protocol";
    private static final String EMAIL_SMTP_AUTH = "email_smtp_auth";
    private String emailFromAddress;
    private String emailToAddress;
    private String emailAuthorizationCode;
    private String emailMessageHeadline;
    private String emailMessageContent;
    private String emailHost;
    private String emailTransportProtocol;
    private String emailSmtpAuth;

    public EmailSinkConfig(@NonNull Config pluginConfig) {
        if (pluginConfig.hasPath(EMAIL_FROM_ADDRESS)) {
            this.emailFromAddress = pluginConfig.getString(EMAIL_FROM_ADDRESS);
        }
        if (pluginConfig.hasPath(EMAIL_TO_ADDRESS)) {
            this.emailToAddress = pluginConfig.getString(EMAIL_TO_ADDRESS);
        }
        if (pluginConfig.hasPath(EMAIL_AUTHORIZATION_CODE)) {
            this.emailAuthorizationCode = pluginConfig.getString(EMAIL_AUTHORIZATION_CODE);
        }
        if (pluginConfig.hasPath(EMAIL_MESSAGE_HEADLINE)) {
            this.emailMessageHeadline = pluginConfig.getString(EMAIL_MESSAGE_HEADLINE);
        }
        if (pluginConfig.hasPath(EMAIL_MESSAGE_CONTENT)) {
            this.emailMessageContent = pluginConfig.getString(EMAIL_MESSAGE_CONTENT);
        }
        if (pluginConfig.hasPath(EMAIL_HOST)) {
            this.emailHost = pluginConfig.getString(EMAIL_HOST);
        }
        if (pluginConfig.hasPath(EMAIL_TRANSPORT_PROTOCOL)) {
            this.emailTransportProtocol = pluginConfig.getString(EMAIL_TRANSPORT_PROTOCOL);
        }
        if (pluginConfig.hasPath(EMAIL_SMTP_AUTH)) {
            this.emailSmtpAuth = pluginConfig.getString(EMAIL_SMTP_AUTH);
        }
    }
}
