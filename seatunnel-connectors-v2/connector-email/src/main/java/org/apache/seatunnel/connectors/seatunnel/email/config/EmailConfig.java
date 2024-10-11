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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class EmailConfig {

    public static final String CONNECTOR_IDENTITY = "EmailSink";

    public static final Option<String> EMAIL_FROM_ADDRESS =
            Options.key("email_from_address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sender Email Address");

    public static final Option<String> EMAIL_TO_ADDRESS =
            Options.key("email_to_address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Address to receive mail");

    public static final Option<String> EMAIL_AUTHORIZATION_CODE =
            Options.key("email_authorization_code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Authorization code,You can obtain the authorization code from the mailbox Settings");
    public static final Option<String> EMAIL_MESSAGE_HEADLINE =
            Options.key("email_message_headline")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The subject line of the entire message");

    public static final Option<String> EMAIL_MESSAGE_CONTENT =
            Options.key("email_message_content")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The body of the entire message");
    public static final Option<String> EMAIL_HOST =
            Options.key("email_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SMTP server to connect to");
    public static final Option<String> EMAIL_TRANSPORT_PROTOCOL =
            Options.key("email_transport_protocol")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The protocol used to send the message");
    public static final Option<Boolean> EMAIL_SMTP_AUTH =
            Options.key("email_smtp_auth")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether to use SMTP authentication");

    public static final Option<Integer> EMAIL_SMTP_PORT =
            Options.key("email_smtp_port")
                    .intType()
                    .defaultValue(465)
                    .withDescription("Select port for authentication.");
}
